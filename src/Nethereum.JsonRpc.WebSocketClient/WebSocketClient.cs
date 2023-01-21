
using Nethereum.JsonRpc.Client;
using Nethereum.JsonRpc.Client.RpcMessages;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Concurrent;
#if NETSTANDARD2_0_OR_GREATER || NETCOREAPP3_1_OR_GREATER || NET461_OR_GREATER || NET5_0_OR_GREATER
using Microsoft.Extensions.Logging;
#endif

namespace Nethereum.JsonRpc.WebSocketClient
{
    public class WebSocketClient : ClientBase, IDisposable, IClientRequestHeaderSupport
    {
        private static readonly int DefaultMessageBufferSize = 8192;
        private static readonly Encoding WebSocketMessageEncoding = new UTF8Encoding(false);

        private readonly SemaphoreSlim _semaphoreSlimClientWebSocket = new SemaphoreSlim(1, 1);
        private readonly SemaphoreSlim _semaphoreSlimRead = new SemaphoreSlim(1, 1);
        private readonly SemaphoreSlim _semaphoreSlimWrite = new SemaphoreSlim(1, 1);
        private readonly ConcurrentDictionary<Object, TaskCompletionSource<RpcResponseMessage>> _pendingRequests =
            new ConcurrentDictionary<Object, TaskCompletionSource<RpcResponseMessage>>(new RequestMessageIdValueEqualityComparer());
        private readonly ConcurrentQueue<TaskCompletionSource<RpcResponseMessage[]>> _pendingBatchRequests =
            new ConcurrentQueue<TaskCompletionSource<RpcResponseMessage[]>>();
        private readonly byte[] _readBuffer = new byte[DefaultMessageBufferSize];        
        private JsonSerializerSettings _jsonSerializerSettings;
        private JsonSerializer _jsonSerializer;
        private readonly ILogger _log;
        private ClientWebSocket _clientWebSocket;

        private WebSocketClient(string path, JsonSerializerSettings jsonSerializerSettings = null)
        {
            this.SetBasicAuthenticationHeaderFromUri(new Uri(path));
            
            Path = path;
            JsonSerializerSettings = jsonSerializerSettings;
        }

        public Dictionary<string, string> RequestHeaders { get; set; } = new Dictionary<string, string>();
        protected string Path { get; set; }
        public static int ForceCompleteReadTotalMilliseconds { get; set; } = 30000;

        public JsonSerializerSettings JsonSerializerSettings
        { 
            get 
            {
                return _jsonSerializerSettings;
            } 
            
            set 
            {
                if(value == null)
                {
                    value = DefaultJsonSerializerSettingsFactory.BuildDefaultJsonSerializerSettings();
                }

                _jsonSerializerSettings = value;                
                _jsonSerializer = JsonSerializer.Create(_jsonSerializerSettings);
            }
        }

       


        public WebSocketClient(string path, JsonSerializerSettings jsonSerializerSettings = null, ILogger log = null) : this(path, jsonSerializerSettings)
        {
            _log = log;
        }

        public  Task StopAsync()
        {
             return StopAsync(WebSocketCloseStatus.NormalClosure, "", new CancellationTokenSource(ConnectionTimeout).Token);
        }

        public async Task StopAsync(WebSocketCloseStatus webSocketCloseStatus, string status, CancellationToken timeOutToken)
        {
            if (_clientWebSocket != null && (_clientWebSocket.State == WebSocketState.Open || _clientWebSocket.State == WebSocketState.Connecting))
            {
                await _semaphoreSlimClientWebSocket.WaitAsync().ConfigureAwait(false);

                try
                {
                    // Check to make sure someone else didn't beat us into the semaphore and stop things
                    if (_clientWebSocket != null && (_clientWebSocket.State == WebSocketState.Open || _clientWebSocket.State == WebSocketState.Connecting))
                    {
                        await Task.WhenAll(_semaphoreSlimRead.WaitAsync(), _semaphoreSlimRead.WaitAsync()).ConfigureAwait(false);
                        await _clientWebSocket.CloseOutputAsync(webSocketCloseStatus, status, timeOutToken).ConfigureAwait(false);
                        while (_clientWebSocket.State != WebSocketState.Closed && !timeOutToken.IsCancellationRequested) ;
                    }
                }
                finally
                {
                    _semaphoreSlimRead.Release();
                    _semaphoreSlimWrite.Release();
                    _semaphoreSlimClientWebSocket.Release();
                }
            }                

        }

        private async Task<ClientWebSocket> GetClientWebSocketAsync()
        {
            try
            {
                if (_clientWebSocket == null || _clientWebSocket.State != WebSocketState.Open)
                {
                    await _semaphoreSlimClientWebSocket.WaitAsync().ConfigureAwait(false);

                    try
                    {
                        // Check to make sure someone else didn't beat us into the semaphore and initialize a new one
                        if (_clientWebSocket == null || _clientWebSocket.State != WebSocketState.Open)
                        {
                            _clientWebSocket = new ClientWebSocket();

                            if (RequestHeaders != null)
                            {
                                foreach (var requestHeader in RequestHeaders)
                                {
                                    _clientWebSocket.Options.SetRequestHeader(requestHeader.Key, requestHeader.Value);
                                }
                            }

                            await _clientWebSocket.ConnectAsync(new Uri(Path), new CancellationTokenSource(ConnectionTimeout).Token).ConfigureAwait(false);

                            // Start the async read loop now that we are connected
                            ReadNextResponseMessage(_clientWebSocket);
                        }
                    }
                    finally
                    {
                        _semaphoreSlimClientWebSocket.Release();
                    }
                }
            }
            catch (TaskCanceledException ex)
            {
                throw new RpcClientTimeoutException($"Rpc timeout after {ConnectionTimeout.TotalMilliseconds} milliseconds", ex);
            }
            catch
            {
                //Connection error we want to allow to retry.
                _clientWebSocket.Dispose();
                _clientWebSocket = null;
                throw;
            }

            return _clientWebSocket;
        }

        [Obsolete("This method is intended to support internal functionality and should not be used directly. Callers should prefer to only use SendAsync.")]
        public async Task<WebSocketReceiveResult> ReceiveBufferedResponseAsync(ClientWebSocket client, byte[] buffer)
        {
            try
            {
                var segmentBuffer = new ArraySegment<byte>(buffer);

                using(var cancellationTokenSource = new CancellationTokenSource(ForceCompleteReadTotalMilliseconds))
                {
                    return await client
                        .ReceiveAsync(segmentBuffer, cancellationTokenSource.Token)
                        .ConfigureAwait(false);
                }
            }
            catch (TaskCanceledException ex)
            {
                throw new RpcClientTimeoutException($"Rpc timeout after {ForceCompleteReadTotalMilliseconds} milliseconds", ex);
            }
        }

        [Obsolete("This method is intended to support internal functionality and should not be used directly. Callers should prefer to only use SendAsync.")]
        public async Task<MemoryStream> ReceiveFullResponseAsync(ClientWebSocket client)
        {
            var responseMessageStream = new MemoryStream(DefaultMessageBufferSize);

            var completedMessage = false;

            while (!completedMessage)
            {
                var receivedResult = await ReceiveBufferedResponseAsync(client, _readBuffer).ConfigureAwait(false);
                var bytesRead = receivedResult.Count;
                
                if (bytesRead > 0)
                {
                    responseMessageStream.Write(_readBuffer, 0, bytesRead);

                    var lastByte = _readBuffer[bytesRead - 1];

                    if (lastByte == 10 || receivedResult.EndOfMessage)  //return signaled with a line feed / or just less than the full message
                    {
                        completedMessage = true;
                    }
                }
                else
                {
                    // We have had a response already and EndOfMessage
                    if(receivedResult.EndOfMessage)
                    {
                        completedMessage = true;
                    }
                }
            }

            if (responseMessageStream.Length == 0) return await ReceiveFullResponseAsync(client).ConfigureAwait(false); //empty response
            return responseMessageStream;
        }

        private async void ReadNextResponseMessage(ClientWebSocket client)
        {
            // If the web socket is not open, then we don't want to continue reading and can just break out of the async
            // loop here
            if(client.State != WebSocketState.Open) {
                return;
            }

            try 
            {
                MemoryStream memoryStream;
                
                try 
                {
                    await _semaphoreSlimRead.WaitAsync().ConfigureAwait(false);

                    memoryStream = await ReceiveFullResponseAsync(client).ConfigureAwait(false);
                }
                finally
                {
                    _semaphoreSlimRead.Release();
                }

                memoryStream.Position = 0;

                using (var streamReader = new StreamReader(memoryStream, WebSocketMessageEncoding))
                using (var reader = new JsonTextReader(streamReader))
                {
                    reader.Read();

                    // Check if it's a single message or a batch (a batch would be a StartArray token)
                    if(reader.TokenType == JsonToken.StartObject)
                    {
                        var responseMessage = _jsonSerializer.Deserialize<RpcResponseMessage>(reader);
                        
                        // If the pending request task completion source still exists, try to resolve it with the response
                        if(_pendingRequests.TryGetValue(responseMessage.Id, out var responseTaskCompletionSource))
                        {
                            // Check if we got an non-empty response and, if so, complete the TaskCompletionSource with it
                            responseTaskCompletionSource.SetResult(responseMessage);
                        }
                        else
                        {
                            _log.Log(LogLevel.Warning, $"No pending request found for response with id: {responseMessage.Id}; dropping.");
                        }
                    }
                    else
                    {
                        // If the pending request task completion source still exists, try to resolve it with the response
                        if(_pendingBatchRequests.TryPeek(out var responseTaskCompletionSource))
                        {
                            // Check if we got an non-empty response and, if so, complete the TaskCompletionSource with it
                            responseTaskCompletionSource.SetResult(_jsonSerializer.Deserialize<RpcResponseMessage[]>(reader));
                        }
                        else
                        {
                            _log.Log(LogLevel.Warning, $"No pending batch requests found; dropping batch response.");
                        }
                    }
                }
            } catch (Exception exception) {
                _log.LogError(exception, "Error reading RPC response.");
            }

            // Initiate the read of the next response message (effectively an async read loop)
            ReadNextResponseMessage(client);
        }

        protected override async Task<RpcResponseMessage> SendAsync(RpcRequestMessage request, string route = null)
        {
            var responseTaskCompletionSource = new TaskCompletionSource<RpcResponseMessage>();
            
            if(!this._pendingRequests.TryAdd(request.Id, responseTaskCompletionSource)) {
                throw new InvalidOperationException($"A request with the specified id is already pending: {request.Id}");
            }

            var logger = new RpcLogger(_log);

            try
            {
                using(MemoryStream requestMessageStream = new MemoryStream(DefaultMessageBufferSize))
                {
                    using(StreamWriter streamWriter = new StreamWriter(requestMessageStream, WebSocketMessageEncoding, DefaultMessageBufferSize, true))
                    {
                        _jsonSerializer.Serialize(streamWriter, request);
                    }
                    
                    // Avoid allocation of string if not logging
                    if(logger.IsLogTraceEnabled())
                    {
                        logger.LogRequest(WebSocketMessageEncoding.GetString(requestMessageStream.GetBuffer(), 0, (int)requestMessageStream.Length));
                    }

                    var webSocket = await GetClientWebSocketAsync().ConfigureAwait(false);

                    try
                    {
                        await _semaphoreSlimWrite.WaitAsync().ConfigureAwait(false);
                        
                        using(var cancellationTokenSource = new CancellationTokenSource(ConnectionTimeout))
                        {
                            await webSocket.SendAsync(new ArraySegment<byte>(requestMessageStream.GetBuffer(), 0, (int)requestMessageStream.Length), WebSocketMessageType.Text, true, cancellationTokenSource.Token)
                                .ConfigureAwait(false);
                        }
                    }
                    finally
                    {
                        _semaphoreSlimWrite.Release();
                    }
                }

                var responseMessage = await responseTaskCompletionSource.Task.ConfigureAwait(false);

                logger.LogResponse(responseMessage);

                return responseMessage;
            }
            catch (Exception ex)
            {
                var exception = new RpcClientUnknownException("Error occurred when trying to make a web socket request." + request.Method, ex);
                logger.LogException(exception);

                throw exception;
            }
            finally
            {
                // Always clear the pending request no matter what
                this._pendingRequests.TryRemove(request.Id, out _);
            }
        }

        protected async override Task<RpcResponseMessage[]> SendAsync(RpcRequestMessage[] requests)
        {
            var responseTaskCompletionSource = new TaskCompletionSource<RpcResponseMessage[]>();
            
            _pendingBatchRequests.Enqueue(responseTaskCompletionSource);
            
            var logger = new RpcLogger(_log);

            try
            {
                using(MemoryStream requestMessageStream = new MemoryStream(DefaultMessageBufferSize))
                {
                    using(StreamWriter streamWriter = new StreamWriter(requestMessageStream, WebSocketMessageEncoding, DefaultMessageBufferSize, true))
                    {
                        _jsonSerializer.Serialize(streamWriter, requests);
                    }

                    // Avoid allocation of string if not logging
                    if(logger.IsLogTraceEnabled())
                    {
                        logger.LogRequest(WebSocketMessageEncoding.GetString(requestMessageStream.GetBuffer(), 0, (int)requestMessageStream.Length));
                    }
                
                    var webSocket = await GetClientWebSocketAsync().ConfigureAwait(false);

                    try
                    {
                        await _semaphoreSlimWrite.WaitAsync().ConfigureAwait(false);
                        
                        using(var cancellationTokenSource = new CancellationTokenSource(ConnectionTimeout))
                        {                
                            await webSocket.SendAsync(new ArraySegment<byte>(requestMessageStream.GetBuffer(), 0, (int)requestMessageStream.Length), WebSocketMessageType.Text, true, cancellationTokenSource.Token)
                                .ConfigureAwait(false);
                        }
                    }
                    finally
                    {
                        _semaphoreSlimWrite.Release();
                    }
                }

                return await responseTaskCompletionSource.Task.ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                var exception = new RpcClientUnknownException("Error occurred when trying to make a batch web socket request.", ex);
                logger.LogException(exception);
                throw exception;
            }
            finally
            {
                // Always clear the pending batch request
                this._pendingBatchRequests.TryDequeue(out _);
            }
        }

        public void Dispose()
        {
            try
            {
                StopAsync().Wait();
            }
            catch 
            {
                
            }
            _clientWebSocket?.Dispose();
            _clientWebSocket = null;
        }

        private sealed class RequestMessageIdValueEqualityComparer : IEqualityComparer<Object>
        {
            public new bool Equals(object x, object y)
            {
                return x?.Equals(y) ?? y == null;
            }

            public int GetHashCode(object obj)
            {
                return obj?.GetHashCode() ?? 0;
            }
        }
    }
}

