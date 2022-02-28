using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Rebus.Redis.Transport
{
    public class RedisOptions
    {
        public string ConnectionString { get; set; } = default!;

        public int StreamEntriesCount { get; set; } = 10;

        public string QueueName { get; set; } = string.Empty;

        /// <summary>
        /// 
        /// </summary>
        public long ProcessingTimeout { get; set; } = 30 * 1000;

        /// <summary>
        /// Pending loop interval milliseconds
        /// </summary>
        public int RedeliverInterval { get; set; } = 30 * 000;
    }
}
