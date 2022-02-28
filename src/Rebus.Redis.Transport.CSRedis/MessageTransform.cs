using Newtonsoft.Json;
using Rebus.Messages;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Rebus.Redis.Transport.CSRedis
{
    internal static class MessageTransform
    {
        public static TransportMessage? ToMessage(string key, string id, string[] items)
        {
            if (items == null || items.Count() == 0)
            {
                return null;
            }

            if (items.Length != 2 || items[0] != "d")
            {
                return null;
            }

            var message = JsonConvert.DeserializeObject<TransportMessage>(items[1]);

            message.Headers?.Add("redis-id", id);

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
