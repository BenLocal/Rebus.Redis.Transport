using FreeRedis;
using Newtonsoft.Json;
using Rebus.Messages;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Rebus.Redis.Transport.FreeRedis
{
    internal static class MessageTransform
    {
        public static TransportMessage? ToMessage(StreamsEntry streamEntry)
        {
            if (streamEntry?.fieldValues == null)
            {
                return null;
            }

            if (streamEntry.fieldValues.Length != 2)
            {
                throw new ArgumentException($"Redis stream entry with id {streamEntry.id} missing data");
            }

            var messageType = streamEntry.fieldValues[0]?.ToString();
            if (messageType != "d")
            {
                throw new ArgumentException($"Redis stream entry with id {streamEntry.id} data issues");
            }

            var str = streamEntry.fieldValues[1]?.ToString();
            if (str == null)
            {
                throw new ArgumentException($"Redis stream entry with messge null data issues");
            }

            var message = JsonConvert.DeserializeObject<TransportMessage>(str);

            message.Headers?.Add("redis-id", streamEntry.id);

            return message;
        }

        public static Dictionary<string, string> AsData(TransportMessage message)
        {
            return new Dictionary<string, string>
            {
                { "d", JsonConvert.SerializeObject(message) }
            };
        }
    }
}
