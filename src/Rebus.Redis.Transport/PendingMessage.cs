using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Rebus.Redis.Transport
{
    public class PendingMessage
    {
        public string? Id { get; set; }

        public string? Consumer { get; set; }

        public long TransferTimes { get; set; }

        public long Idle { get; set; }
    }
}
