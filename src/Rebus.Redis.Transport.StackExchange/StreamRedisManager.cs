using Rebus.Messages;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Rebus.Redis.Transport.StackExchange
{
    public class StreamRedisManager : IRedisManager
    {
        private readonly RedisClient _redisClient;
        private readonly RedisOptions _options;

        public StreamRedisManager(RedisOptions options)
        {
            _options = options;
            _redisClient = await ConnectionMultiplexer.ConnectAsync(redisOptions.Configuration, redisLogger)
        }

        public void Ack(string key, string consumerGroup, string messageId)
        {
            throw new NotImplementedException();
        }

        public void CreateConsumerGroup(string key, string consumerGroup)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<TransportMessage> GetClaimMessagesAsync(string key, string consumerGroup, long minIdle, IEnumerable<string> ids, CancellationToken token)
        {
            throw new NotImplementedException();
        }

        public string GetIdByTransportMessage(TransportMessage message)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<TransportMessage> GetNewMessagesAsync(string key, string consumerGroup, int count, CancellationToken token)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<PendingMessage> GetPendingMessagesAsync(string key, string consumerGroup, CancellationToken token)
        {
            throw new NotImplementedException();
        }

        public Task PublishAsync(string key, IEnumerable<TransportMessage> messages)
        {
            throw new NotImplementedException();
        }
    }
}
