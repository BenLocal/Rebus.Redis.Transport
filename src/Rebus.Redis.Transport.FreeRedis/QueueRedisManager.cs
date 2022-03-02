using FreeRedis;
using Newtonsoft.Json;
using Rebus.Messages;
using System;

namespace Rebus.Redis.Transport.FreeRedis
{
    public class QueueRedisManager : IRedisManager
    {
        private readonly RedisClient _redisClient;
        private readonly RedisOptions _options;

        public QueueRedisManager(RedisOptions options)
        {
            _options = options;
            _redisClient = new RedisClient(_options.ConnectionString);
        }

        public void Ack(string key, string consumerGroup, string messageId)
        {
            var pendingMapKey = $"{key}:{consumerGroup}";
            _redisClient.HDel(pendingMapKey, messageId);

            _redisClient.Eval(@"redis.call('HDEL', KEYS[2], KEYS[1]..':'..KEYS[3])
redis.call('DEL', KEYS[1]..':'..KEYS[3])", new string[] { key, pendingMapKey, messageId });
        }

        public void CreateConsumerGroup(string key, string consumerGroup)
        {
        }

        public IEnumerable<TransportMessage> GetClaimMessagesAsync(string key, string consumerGroup, long minIdle, IEnumerable<string> ids, CancellationToken token)
        {
            var keys = ids.Select(x => $"{key}:{x}");

            var res = _redisClient.MGet(keys.ToArray());

            if (res == null)
            {
                yield break;
            }

            foreach (var item in res)
            {
                var message = JsonConvert.DeserializeObject<TransportMessage>(item);

                yield return message;
            }
        }

        public IEnumerable<TransportMessage> GetNewMessagesAsync(string key, string consumerGroup, CancellationToken token)
        {
            var pendingMapKey = $"{key}:{consumerGroup}";
            // Redis version >= 6.2.0: Added the `count` argument.
            var result = _redisClient.Eval(@"local vals, i = {}, 1
while (i <= tonumber(ARGV[1]))
do
    local message_id = redis.call('RPOP', KEYS[1])
    if (message_id == false) then
        return vals
    else
        local message = redis.call('GET', KEYS[1]..':'..message_id)
        redis.call('HSET', KEYS[2], message_id, '')
        vals[i] = {message_id,message}
    end
    i = i + 1
end
return vals", new string[] { key, pendingMapKey }, new object[] { _options.QueueDepth });

            if (result is object[] vals)
            {
                foreach (var item in vals)
                {
                    if (item is object[] messages && messages.Length == 2)
                    {
                        var message = JsonConvert.DeserializeObject<TransportMessage>(messages[1].ToString());
                        yield return message;
                    }
                }
            }
        }

        public IEnumerable<PendingMessage> GetPendingMessagesAsync(string key, string consumerGroup, CancellationToken token)
        {
            var pendingMapKey = $"{key}:{consumerGroup}";

            var keys = _redisClient.HGetAll(pendingMapKey);

            return keys.Select(x => new PendingMessage()
            {
                Id = x.Key,
            });
        }

        public Task PublishAsync(string key, IEnumerable<TransportMessage> messages)
        {
            foreach (var message in messages)
            {
                var messageId = Guid.NewGuid().ToString("N");
                message.Headers?.Add("redis-id", messageId);

                _redisClient.Eval(@"redis.call('SET', KEYS[1]..':'..ARGV[1], ARGV[2])
redis.call('LPUSH', KEYS[1], ARGV[1])", new string[] { key }, new object[] { messageId, MessageTransform.AsStringData(message) });
            }

            return Task.CompletedTask;
        }
    }
}
