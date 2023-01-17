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
    public class EthPendingTransactionSubscriptionTester : StreamingRPCRequestTester
    {
        [Fact]
        public async Task ShouldRetrievePendingTransaction()
        {
            await StreamingClient.StartAsync();

            string receivedMessage = null;
            
            var receivedTcs = new TaskCompletionSource<string>();
            var subscription = new EthNewPendingTransactionSubscription(StreamingClient);
            subscription.SubscriptionDataResponse += delegate (object sender, StreamingEventArgs<string> args)
            {
                receivedMessage = args.Response;

                receivedTcs.SetResult(receivedMessage);
            };

            await subscription.SubscribeAsync(Guid.NewGuid().ToString()).ConfigureAwait(false);

            var transactionData = new {
                from=Settings.GetDefaultAccount(), 
                to=Settings.GetDefaultAccount(),
                value=0,
            };
            
            await StreamingClient.SendRequestAsync(
                new RpcRequest(Guid.NewGuid().ToString(), "eth_sendTransaction", new object[] { transactionData }),
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
                throw new Exception("Timed out waiting for pending transaction!", taskCanceledException);
            }
            finally
            {
                cts.Dispose();
            }

            Assert.NotNull(receivedMessage);
        }

        public override async Task ExecuteAsync(IStreamingClient client)
        {
            var subscription = new EthNewPendingTransactionSubscription(client);
            await subscription.SubscribeAsync(Guid.NewGuid().ToString()).ConfigureAwait(false);
        }

        public override Type GetRequestType()
        {
            return typeof(EthNewPendingTransactionSubscription);
        }
    }
}