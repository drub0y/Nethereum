
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
        private static readonly Object BatchRequestId = new Object();

        private readonly SemaphoreSlim _semaphoreSlimRead = new SemaphoreSlim(1, 1);
        private readonly SemaphoreSlim _semaphoreSlimWrite = new SemaphoreSlim(1, 1);

        private readonly ConcurrentDictionary<Object, TaskCompletionSource<MemoryStream>> _pendingRequests =
            new ConcurrentDictionary<Object, TaskCompletionSource<MemoryStream>>();

        public Dictionary<string, string> RequestHeaders { get; set; } = new Dictionary<string, string>();
        protected string Path { get; set; }
        public static int ForceCompleteReadTotalMilliseconds { get; set; } = 2000;

        private WebSocketClient(string path, JsonSerializerSettings jsonSerializerSettings = null)
        {
            this.SetBasicAuthenticationHeaderFromUri(new Uri(path));
            if (jsonSerializerSettings == null)
            {
                jsonSerializerSettings = DefaultJsonSerializerSettingsFactory.BuildDefaultJsonSerializerSettings();
            }

            Path = path;
            JsonSerializerSettings = jsonSerializerSettings;
        }

        public JsonSerializerSettings JsonSerializerSettings { get; set; }
        private readonly object _lockingObject = new object();
        private readonly ILogger _log;

        private ClientWebSocket _clientWebSocket;


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
            try
            {
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
            }

        }

        private async Task<ClientWebSocket> GetClientWebSocketAsync()
        {
            try
            {
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


        public async Task<WebSocketReceiveResult> ReceiveBufferedResponseAsync(ClientWebSocket client, byte[] buffer)
        {
            try
            {
                var segmentBuffer = new ArraySegment<byte>(buffer);
                return await client
                    .ReceiveAsync(segmentBuffer, new CancellationTokenSource(ForceCompleteReadTotalMilliseconds).Token)
                    .ConfigureAwait(false);
            }
            catch (TaskCanceledException ex)
            {
                throw new RpcClientTimeoutException($"Rpc timeout after {ForceCompleteReadTotalMilliseconds} milliseconds", ex);
            }
        }

        public async Task ReceiveFullResponseAsync(ClientWebSocket client, Object requestId, CancellationToken cancellationToken)
        {
            var readBufferSize = 512;
            var memoryStream = new MemoryStream(readBufferSize);

            var buffer = new byte[readBufferSize];
            var completedMessage = false;

            try 
            {
                await _semaphoreSlimRead.WaitAsync().ConfigureAwait(false);

                while (!completedMessage)
                {
                    var receivedResult = await ReceiveBufferedResponseAsync(client, buffer).ConfigureAwait(false);
                    var bytesRead = receivedResult.Count;
                    if (bytesRead > 0)
                    {
                        memoryStream.Write(buffer, 0, bytesRead);
                        var lastByte = buffer[bytesRead - 1];

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
            }
            finally
            {
                _semaphoreSlimRead.Release();
            }

            TaskCompletionSource<MemoryStream> responseTaskCompletionSource = null;

            // If this is a batch request, then we want to resolve the first pending batch request otherwise we 
            // resolve the request with the given id
            if(Object.ReferenceEquals(WebSocketClient.BatchRequestId, requestId)) {
                _pendingBatchRequests.TryPeek(out responseTaskCompletionSource);
            }
            else 
            {
                _pendingRequests.TryGetValue(requestId, out responseTaskCompletionSource);
            }

            // Get the pending request and, only if it still exists, try to resolve it with the response
            if(responseTaskCompletionSource != null) 
            {
                // Check if we got an non-empty response and, if so, complete the TaskCompletionSource with it
                if (memoryStream.Length > 0) {
                    responseTaskCompletionSource.SetResult(memoryStream);

                    return;
                }

                // We received an empty response, asynchronously recurse to receive next response
                memoryStream.Dispose();
                
                await ReceiveFullResponseAsync(client, requestId, cancellationToken).ConfigureAwait(false);
            }
        }

        protected override async Task<RpcResponseMessage> SendAsync(RpcRequestMessage request, string route = null)
        {
            var responseTaskCompletionSource = new TaskCompletionSource<MemoryStream>();
            
            if(!this._pendingRequests.TryAdd(request.Id, responseTaskCompletionSource)) {
                throw new InvalidOperationException($"A request with the specified id is already pending: {request.Id}");
            }

            var logger = new RpcLogger(_log);

            try
            {
                var rpcRequestJson = JsonConvert.SerializeObject(request, JsonSerializerSettings);
                var requestBytes = new ArraySegment<byte>(Encoding.UTF8.GetBytes(rpcRequestJson));
                logger.LogRequest(rpcRequestJson);

                var webSocket = await GetClientWebSocketAsync().ConfigureAwait(false);
                var cancellationTokenSource = new CancellationTokenSource();

                try
                {
                    await _semaphoreSlimWrite.WaitAsync().ConfigureAwait(false);
                    cancellationTokenSource.CancelAfter(ConnectionTimeout);

                    await webSocket.SendAsync(requestBytes, WebSocketMessageType.Text, true, cancellationTokenSource.Token)
                        .ConfigureAwait(false);
                }
                finally
                {
                    _semaphoreSlimWrite.Release();
                }

                ReceiveFullResponseAsync(webSocket, request.Id, cancellationTokenSource.Token);
                
                using(var memoryData = await responseTaskCompletionSource.Task.ConfigureAwait(false))
                {
                    memoryData.Position = 0;
                    using (var streamReader = new StreamReader(memoryData))
                    using (var reader = new JsonTextReader(streamReader))
                    {
                        var serializer = JsonSerializer.Create(JsonSerializerSettings);
                        var message = serializer.Deserialize<RpcResponseMessage>(reader);
                        logger.LogResponse(message);
                        return message;
                    }
                }
            }
            catch (Exception ex)
            {
                var exception = new RpcClientUnknownException("Error occurred when trying to web socket requests(s): " + request.Method, ex);
                logger.LogException(exception);

                throw exception;
            }
            finally
            {
                // Always clear the pending request
                this._pendingRequests.TryRemove(request.Id, out _);
            }
        }

        protected async override Task<RpcResponseMessage[]> SendAsync(RpcRequestMessage[] requests)
        {
            var responseTaskCompletionSource = new TaskCompletionSource<MemoryStream>();
            
            _pendingBatchRequests.Enqueue(responseTaskCompletionSource);
            
            var logger = new RpcLogger(_log);

            try
            {
                var rpcRequestJson = JsonConvert.SerializeObject(requests, JsonSerializerSettings);
                var requestBytes = new ArraySegment<byte>(Encoding.UTF8.GetBytes(rpcRequestJson));
                logger.LogRequest(rpcRequestJson);
                
                var webSocket = await GetClientWebSocketAsync().ConfigureAwait(false);
                var cancellationTokenSource = new CancellationTokenSource();

                try
                {
                    await _semaphoreSlimWrite.WaitAsync().ConfigureAwait(false);
                    cancellationTokenSource.CancelAfter(ConnectionTimeout);
                
                    await webSocket.SendAsync(requestBytes, WebSocketMessageType.Text, true, cancellationTokenSource.Token)
                        .ConfigureAwait(false);
                }
                finally
                {
                    _semaphoreSlimWrite.Release();
                }

                ReceiveFullResponseAsync(webSocket, WebSocketClient.BatchRequestId, cancellationTokenSource.Token);

                using (var memoryData = await responseTaskCompletionSource.Task.ConfigureAwait(false))
                {
                    memoryData.Position = 0;
                    using (var streamReader = new StreamReader(memoryData))
                    using (var reader = new JsonTextReader(streamReader))
                    {
                        var serializer = JsonSerializer.Create(JsonSerializerSettings);
                        var messages = serializer.Deserialize<RpcResponseMessage[]>(reader);
                        return messages;
                    }
                }
            }
            catch (Exception ex)
            {
                var exception = new RpcClientUnknownException("Error occurred when trying to web socket requests(s)", ex);
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
    }
}

