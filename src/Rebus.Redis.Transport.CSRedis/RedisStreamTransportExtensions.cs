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

            configurer
                .OtherService<IRedisManager>()
                .Register(c =>
                {
                    return new StreamRedisManager(options);
                });


            configurer.OtherService<ReidsTransport>().Register(x =>
            {
                var manager = x.Get<IRedisManager>();
                return new ReidsTransport(options.QueueName, manager, options);
            });

            configurer.Register(x => x.Get<ReidsTransport>());
        }
    }
}
