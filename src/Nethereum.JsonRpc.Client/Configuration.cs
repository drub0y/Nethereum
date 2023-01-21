using System;
using System.Threading;

namespace Nethereum.JsonRpc.Client
{
    public class Configuration
    {
        private static long _lastUniqueRequestId = 1;
        
        [Obsolete("Use NextUniqueRequestId instead to guarantee unique request IDs per message.")]
        public static object DefaultRequestId { get; set; } = 1;
        
        public static long NextUniqueRequestId() 
        {
            return Interlocked.Increment(ref _lastUniqueRequestId);
        }
    }
}