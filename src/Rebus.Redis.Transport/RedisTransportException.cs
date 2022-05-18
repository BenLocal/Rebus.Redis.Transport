using System;
using System.Collections.Generic;
using System.Text;

namespace Rebus.Redis.Transport
{
    public class RedisTransportException : Exception
    {
        public RedisTransportException(string message) : base(message)
        { 
        }

        public RedisTransportException(string message, Exception inner) 
            : base(message, inner)
        {
        }
    }
}
