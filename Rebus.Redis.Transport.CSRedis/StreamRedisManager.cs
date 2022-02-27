using CSRedis;
using Newtonsoft.Json;
using Rebus.Config;
using Rebus.Messages;
using Rebus.Sagas.Idempotent;
using System.IO;

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
            var streamExist = _redisClient.Type(key);
            if (streamExist == KeyType.None)
            {
                _redisClient.XGroupCreate(key, consumerGroup, "$", MkStream: true);
            }
            else
            {
                var groupInfo = _redisClient.XInfoGroups(key);
                if (groupInfo.Any(g => g.name == consumerGroup))
                {
                    return;
                }
                //what was the last message ID when the group was just created.
                //If we provide $ as we did, then only new messages arriving in the stream
                //from now on will be provided to the consumers in the group. If we
                //specify 0 instead the consumer group will consume all the messages
                //in the stream history to start with. Of course, you can specify any
                //other valid ID. What you know is that the consumer group will start
                //delivering messages that are greater than the ID you specify.
                //Because $ means the current greatest ID in the stream,
                //specifying $ will have the effect of consuming only new messages.
                _redisClient.XGroupCreate(key, consumerGroup, "$");
            }
        }

        public IEnumerable<TransportMessage> PollStreamsLatestMessagesAsync(string key, string consumerGroup, TimeSpan pollDelay, CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            { 
                var streamsEntries = _redisClient.XReadGroup(consumerGroup, consumerGroup,
                    _options.StreamEntriesCount,
                  pollDelay.Milliseconds, (key, "0"));

                // TODO
                //return Enumerable.Empty<TransportMessage>();

                //if (streamsEntries.All(x => x.entries.Length < _options.StreamEntriesCount))
                //{
                //    break;
                //}
            }

            return Enumerable.Empty<TransportMessage>();
        }

        public IEnumerable<TransportMessage> PollStreamsPendingMessagesAsync(string key, string consumerGroup, TimeSpan pollDelay, CancellationToken token)
        {
            //If the ID is the special ID > then the command will return only new messages never delivered to other consumers
            //so far, and as a side effect, will update the consumer group's last ID.

            // If the ID is any other valid numerical ID, then the command will let us access our history of pending messages.
            // That is, the set of messages that were delivered to this specified consumer (identified by the provided name), and
            // never acknowledged so far with XACK.
            var streamsEntries = _redisClient.XReadGroup(consumerGroup, consumerGroup,
                _options.StreamEntriesCount,
                pollDelay.Milliseconds, (key, ">"));

            // TODO
            return Enumerable.Empty<TransportMessage>();
        }

        public Task PublishAsync(string key, IEnumerable<TransportMessage> messages)
        {
            _redisClient.XAdd(key, 
                messages.Select(x => ("data", JsonConvert.SerializeObject(x))).ToArray());

            return Task.CompletedTask;
        }
    }
}
