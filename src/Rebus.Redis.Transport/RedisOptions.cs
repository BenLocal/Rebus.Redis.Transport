using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Rebus.Redis.Transport
{
    public class RedisOptions
    {
        /// <summary>
        /// Redis connection string
        /// </summary>
        public string ConnectionString { get; set; } = default!;

        /// <summary>
        /// The size of the message count for read redis
        /// </summary>
        public int StreamEntriesCount { get; set; } = 10;

        /// <summary>
        /// The topic name
        /// </summary>
        public string QueueName { get; set; } = string.Empty;

        /// <summary>
        /// The consumer name
        /// </summary>
        public string ConsumerName { get; set; } = string.Empty;

        /// <summary>
        /// The size of the message queue for processing
        /// </summary>
        public uint QueueDepth { get; set; }

        /// <summary>
        /// The amount time a message must be pending before attempting to redeliver it (0 disables redelivery)
        /// </summary>
        public TimeSpan ProcessingTimeout { get; set; } = TimeSpan.FromSeconds(60);

        /// <summary>
        /// The interval between checking for pending messages to redelivery (0 disables redelivery)
        /// </summary>
        public TimeSpan RedeliverInterval { get; set; } = TimeSpan.FromSeconds(15);
    }
}
