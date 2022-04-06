using Rebus.Messages;
using System;
using System.Collections.Generic;
using System.Text;

namespace Rebus.Redis.Transport
{
    public interface IRedisManagerFactory
    {
        public IRedisManager CreateRedisManager();
    }
}
