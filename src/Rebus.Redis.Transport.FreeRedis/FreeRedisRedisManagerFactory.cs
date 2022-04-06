using Rebus.Config;
using Rebus.Messages;
using System;
using System.Collections.Generic;
using System.Text;

namespace Rebus.Redis.Transport.FreeRedis
{
    internal class FreeRedisRedisManagerFactory : IRedisManagerFactory
    {
        private readonly RedisOptions _options;

        public FreeRedisRedisManagerFactory(RedisOptions options)
        {
            _options = options;
        }

        public IRedisManager CreateRedisManager()
        {
            switch (_options.QueueType)
            {
                case QueueType.STREAM:
                    return new StreamRedisManager(_options);
                case QueueType.LIST:
                    return new QueueRedisManager(_options);
                default:
                    throw new ArgumentException("Options's QueueType is required");
                    
            }
        }

        public string GetIdByTransportMessage(TransportMessage message)
        {
            switch (_options.QueueType)
            {
                case QueueType.STREAM:
                    return message.Headers.GetValueOrDefault(RedisHeaderConsts.RedisStreamMessageId);
                case QueueType.LIST:
                default:
                    return message.Headers.GetValueOrDefault(Headers.MessageId);
            }
        }
    }
}
