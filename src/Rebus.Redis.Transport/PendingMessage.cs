using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Rebus.Redis.Transport
{
    public class PendingMessage
    {
        /// <summary>
        /// message was delivered to this consumer.
        /// </summary>
        public string? Id { get; set; }

        /// <summary>
        /// message was delivered to this consumer. message was delivered to this consumer.
        /// </summary>
        public string? Consumer { get; set; }

        /// <summary>
        /// The number of milliseconds that elapsed since the last time this message was delivered to this consumer.
        /// </summary>
        public long DeliveredTimes { get; set; }

        /// <summary>
        /// message was delivered to this consumer (in milliseconds).
        /// </summary>
        public long Idle { get; set; }
    }
}
