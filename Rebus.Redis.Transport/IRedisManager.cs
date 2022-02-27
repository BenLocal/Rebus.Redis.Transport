using Rebus.Messages;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Rebus.Redis.Transport
{
    public interface IRedisManager
    {
        void CreateConsumerGroup(string key, string consumerGroup);

        Task PublishAsync(string key, IEnumerable<TransportMessage> messages);

        IEnumerable<TransportMessage> PollStreamsLatestMessagesAsync(string key, string consumerGroup,
            TimeSpan pollDelay, CancellationToken token);

        IEnumerable<TransportMessage> PollStreamsPendingMessagesAsync(string key, string consumerGroup,
            TimeSpan pollDelay, CancellationToken token);

        void Ack(string key, string consumerGroup, string messageId);
    }
}
