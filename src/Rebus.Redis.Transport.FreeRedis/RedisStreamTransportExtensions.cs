using Rebus.Config;
using Rebus.Transport;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Rebus.Redis.Transport.FreeRedis
{
    public static class RedisStreamTransportExtensions
    {
        public static void UseRedisStreamMq(this StandardConfigurer<ITransport> configurer, Action<RedisOptions> action)
        {
            var options = new RedisOptions();
            action.Invoke(options);


            if (options.QueueType == QueueType.STREAM)
            {
                configurer
                    .OtherService<IRedisManager>()
                    .Register(c =>
                    {
                        return new StreamRedisManager(options);
                    });
            }
            else if (options.QueueType == QueueType.LIST)
            {
                configurer
                    .OtherService<IRedisManager>()
                    .Register(c =>
                    {
                        return new QueueRedisManager(options);
                    });
            }
            else
            {
                throw new ArgumentException("Options's QueueType is required");
            }

            configurer.OtherService<ReidsTransport>().Register(x =>
            {
                var manager = x.Get<IRedisManager>();
                return new ReidsTransport(manager, options);
            });

            configurer.Register(x => x.Get<ReidsTransport>());
        }
    }
}
