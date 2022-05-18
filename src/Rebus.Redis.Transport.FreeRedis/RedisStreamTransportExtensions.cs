using Rebus.Config;
using Rebus.Logging;
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
        public static void UseRedisMq(this StandardConfigurer<ITransport> configurer, Action<RedisOptions> action)
            => BuildInternal(configurer, false, action);

        public static void UseRedisMqAsOneWayClient(this StandardConfigurer<ITransport> configurer, Action<RedisOptions> action)
            => BuildInternal(configurer, true, action);

        private static void BuildInternal(this StandardConfigurer<ITransport> configurer, bool oneway, Action<RedisOptions> action)
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

            configurer.OtherService<RedisTransport>().Register(x =>
            {
                var manager = x.Get<IRedisManager>();
                var inQueueName = oneway ? null : options.QueueName;
                return new RedisTransport(x.Get<IRebusLoggerFactory>(), manager, options, inQueueName);
            });

            configurer.Register(x => x.Get<RedisTransport>());

            if (oneway)
            {
                OneWayClientBackdoor.ConfigureOneWayClient(configurer);
            }
        }
    }
}
