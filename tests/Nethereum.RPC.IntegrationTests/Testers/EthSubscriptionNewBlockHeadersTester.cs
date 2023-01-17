using System;
using System.Threading;
using System.Threading.Tasks;
using Nethereum.JsonRpc.Client;
using Nethereum.JsonRpc.Client.RpcMessages;
using Nethereum.JsonRpc.Client.Streaming;
using Nethereum.RPC.Eth.DTOs;
using Nethereum.RPC.Eth.Subscriptions;
using Xunit;

namespace Nethereum.RPC.Tests.Testers
{

    //TODO:Subscriptions
    public class EthSubscriptionNewBlockHeadersTester : StreamingRPCRequestTester
    {
        [Fact]
        public async Task ShouldRetrieveBlock()
        {

            Block receivedMessage = null;
            await StreamingClient.StartAsync();
            
            var receivedTcs = new TaskCompletionSource<Block>();
            var subscription = new EthNewBlockHeadersSubscription(StreamingClient);
            subscription.SubscriptionDataResponse += delegate(object sender, StreamingEventArgs<Block> args)
                {
                    receivedMessage = args.Response;

                    receivedTcs.SetResult(receivedMessage);
                };

            await subscription.SubscribeAsync(Guid.NewGuid().ToString()).ConfigureAwait(false);

            await StreamingClient.SendRequestAsync(
                new RpcRequest(Guid.NewGuid().ToString(), "evm_mine", new object[] { new {blocks=1} }),
                TestRpcStreamingResponseHandler.Create()
            );

            CancellationTokenSource cts = new CancellationTokenSource();
            cts.CancelAfter(10000);
            
            try
            {
                receivedTcs.Task.Wait(cts.Token);
            }
            catch (TaskCanceledException taskCanceledException)
            {
                throw new Exception("Timed out waiting for block header!", taskCanceledException);
            }
            finally
            {
                cts.Dispose();
            }

            Assert.NotNull(receivedMessage);
        }

        public override async Task ExecuteAsync(IStreamingClient client)
        {
            var subscription = new EthNewBlockHeadersSubscription(client);
            await subscription.SubscribeAsync(Guid.NewGuid().ToString()).ConfigureAwait(false);
        }

        public override Type GetRequestType()
        {
            return typeof (EthNewPendingTransactionSubscription);
        }
    }

    public class TestRpcStreamingResponseHandler : IRpcStreamingResponseHandler
    {
        private readonly Action onResponse;
        private readonly Action onClientError;
        private readonly Action onClientDisconnection;

        public TestRpcStreamingResponseHandler(Action onResponse, Action onClientError, Action onClientDisconnection)
        {
            this.onResponse = onResponse;
            this.onClientError = onClientError;
            this.onClientDisconnection = onClientDisconnection;
        }

        public void HandleClientDisconnection()
        {
            this.onClientDisconnection?.Invoke();
        }

        public void HandleClientError(Exception ex)
        {
            this.onClientError?.Invoke();
        }

        public void HandleResponse(RpcStreamingResponseMessage rpcStreamingResponse)
        {
            this.onResponse?.Invoke();
        }

        public static TestRpcStreamingResponseHandler Create(
            Action onResponse = null,
            Action onClientError = null,
            Action onClientDisconnection = null)
        {
            return new TestRpcStreamingResponseHandler(onResponse, onClientError, onClientDisconnection);
        }
    }
}