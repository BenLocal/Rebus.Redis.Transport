using CSRedis;
using Newtonsoft.Json;
using Rebus.Messages;
using System.Linq;

namespace Rebus.Redis.Transport.CSRedis
{
    /// <summary>
    /// https://redis.io/topics/streams-intro
    /// </summary>
    public class StreamRedisManager : IRedisManager
    {
        private readonly CSRedisClient _redisClient;
        private readonly RedisOptions _options;

        public StreamRedisManager(RedisOptions options)
        {
            _options = options;
            _redisClient = new CSRedisClient(options.ConnectionString);
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
            catch (RedisException ex)
            {
                if (ex.Message.Contains("BUSYGROUP"))
                {
                    return;
                }

                throw ex;
            }
        }

        public IEnumerable<TransportMessage> GetNewMessagesAsync(string key, string consumerGroup, TimeSpan pollDelay, CancellationToken token)
        {
            //If the ID is the special ID > then the command will return only new messages never delivered to other consumers
            //so far, and as a side effect, will update the consumer group's last ID.

            // If the ID is any other valid numerical ID, then the command will let us access our history of pending messages.
            // That is, the set of messages that were delivered to this specified consumer (identified by the provided name), and
            // never acknowledged so far with XACK.
            var streamsEntries = _redisClient.XReadGroup(consumerGroup, consumerGroup,
                                _options.StreamEntriesCount,
                              pollDelay.Milliseconds, (key, ">"));

            if (streamsEntries == null)
            {
                yield break;
            }

            foreach ((var k, var data) in streamsEntries)
            {
                if (data == null)
                {
                    continue;
                }

                foreach ((var id, var items) in data)
                {
                    var message = MessageTransform.ToMessage(k, id, items);
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
            var streamsEntries = _redisClient.XPending(key,
                    consumerGroup,
                    start: "-",
                    end: "+",
                    count: _options.StreamEntriesCount,
                    consumer: consumerGroup);

            if (streamsEntries == null)
            {
                yield break;
            }

            foreach ((var id, var consumer, var idle, var transferTimes) in streamsEntries)
            {
                yield return new PendingMessage()
                {
                    Id = id,
                    Consumer = consumer,
                    Idle = idle,
                    TransferTimes = transferTimes,
                };
            }
        }

        public Task PublishAsync(string key, IEnumerable<TransportMessage> messages)
        {
            var values = messages.Select(x => MessageTransform.AsData(x).Select(x => (x.Key, x.Value)).FirstOrDefault());

            _redisClient.XAdd(key, values.ToArray());

            return Task.CompletedTask;
        }
    }
}
