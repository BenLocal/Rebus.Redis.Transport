using Rebus.Messages;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Rebus.Redis.Transport
{
    public interface IRedisManager
    {
        void CreateConsumerGroup(string key, string consumerGroup);

        Task PublishAsync(string key, IEnumerable<TransportMessage> messages);

        IEnumerable<TransportMessage> GetNewMessagesAsync(string key, string consumerGroup,
            int count,
            CancellationToken token);

        IEnumerable<PendingMessage> GetPendingMessagesAsync(string key, string consumerGroup,
            CancellationToken token);

        /// <summary>
        /// Reclaims pending messages
        /// </summary>
        /// <param name="key"></param>
        /// <param name="consumerGroup"></param>
        /// <param name="minIdle">Set the idle time (last time it was delivered) of the message.</param>
        /// <param name="ids"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        IEnumerable<TransportMessage> GetClaimMessagesAsync(string key, string consumerGroup,
            long minIdle,
            IEnumerable<string> ids,
            CancellationToken token);

        void Ack(string key, string consumerGroup, string messageId);

        string GetIdByTransportMessage(TransportMessage message);
    }
}
