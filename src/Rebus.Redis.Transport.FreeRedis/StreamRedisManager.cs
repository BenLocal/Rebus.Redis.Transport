using FreeRedis;
using Rebus.Messages;

namespace Rebus.Redis.Transport.FreeRedis
{
    /// <summary>
    /// https://redis.io/topics/streams-intro
    /// </summary>
    public class StreamRedisManager : IRedisManager
    {
        private readonly RedisClient _redisClient;
        private readonly RedisOptions _options;

        public StreamRedisManager(RedisOptions options)
        {
            _options = options;
            _redisClient = new RedisClient(options.ConnectionString);
        }

        public void Ack(string key, string consumerGroup, string messageId)
        {
            _redisClient.XAck(key, consumerGroup, messageId);
        }

        public void CreateConsumerGroup(string key, string consumerGroup)
        {
            //what was the last message ID when the group was just created.
            //If we provide $ as we did, then only new messages arriving in the stream
            //from now on will be provided to the consumers in the group. If we
            //specify 0 instead the consumer group will consume all the messages
            //in the stream history to start with. Of course, you can specify any
            //other valid ID. What you know is that the consumer group will start
            //delivering messages that are greater than the ID you specify.
            //Because $ means the current greatest ID in the stream,
            //specifying $ will have the effect of consuming only new messages.
            try
            {
                _redisClient.XGroupCreate(key, consumerGroup, "0", MkStream: true);
            }
            catch (RedisServerException ex)
            {
                if (ex.Message.Contains("BUSYGROUP"))
                {
                    return;
                }

                throw ex;
            }
        }

        public IEnumerable<TransportMessage> GetClaimMessagesAsync(string key, string consumerGroup,
            long minIdle,
            IEnumerable<string> ids,
            CancellationToken token)
        {
            var streamsEntries = _redisClient.XClaim(key, consumerGroup, consumerGroup, minIdle, ids.ToArray());

            if (streamsEntries == null)
            {
                yield break;
            }

            foreach (var streamsEntry in streamsEntries)
            {
                if (streamsEntry == null)
                {
                    continue;
                }

                var message = MessageTransform.ToMessage(streamsEntry);
                if (message == null)
                {
                    continue;
                }

                yield return message;
            }

        }

        public IEnumerable<TransportMessage> GetNewMessagesAsync(string key, string consumerGroup, CancellationToken token)
        {
            //If the ID is the special ID > then the command will return only new messages never delivered to other consumers
            //so far, and as a side effect, will update the consumer group's last ID.

            // If the ID is any other valid numerical ID, then the command will let us access our history of pending messages.
            // That is, the set of messages that were delivered to this specified consumer (identified by the provided name), and
            // never acknowledged so far with XACK.
            var streamsEntries = _redisClient.XReadGroup(consumerGroup,
                consumerGroup,
                _options.QueueDepth,
                2,
                false,
                key, ">");

            if (streamsEntries == null)
            {
                yield break;
            }

            foreach (var streamsEntry in streamsEntries)
            {
                if (streamsEntry == null)
                {
                    continue;
                }

                foreach (var item in streamsEntry.entries)
                {
                    var message = MessageTransform.ToMessage(item);
                    if (message == null)
                    {
                        continue;
                    }

                    yield return message;
                }
            }
        }

        public IEnumerable<PendingMessage> GetPendingMessagesAsync(string key, string consumerGroup, CancellationToken token)
        {
            // XPENDING <key> <groupname> [[IDLE <min-idle-time>] <start-id> <end-id> <count> [<consumer-name>]]
            // By providing a start and end ID (that can be just - and + as in XRANGE) and a count to control the amount of
            // information returned by the command, we are able to know more about the pending messages. 
            var streamsEntries = _redisClient.XPending(key,
                    consumerGroup,
                    start: "-",
                    end: "+",
                    count: _options.QueueDepth,
                    consumer: consumerGroup);

            if (streamsEntries == null)
            {
                yield break;
            }

            foreach (var item in streamsEntries)
            {
                yield return new PendingMessage()
                {
                    Id = item.id,
                    Consumer = item.consumer,
                    Idle = item.idle,
                    DeliveredTimes = item.deliveredTimes,
                };
            }
        }

        public Task PublishAsync(string key, IEnumerable<TransportMessage> messages)
        {
            foreach (var msg in messages)
            {
                _redisClient.XAdd(key, MessageTransform.AsData(msg));
            }
            return Task.CompletedTask;
        }
    }
}
