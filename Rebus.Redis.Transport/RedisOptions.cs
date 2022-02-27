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
    }
}
