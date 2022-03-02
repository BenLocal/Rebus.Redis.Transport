using FreeRedis;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Rebus.Config;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Rebus.Redis.Transport.FreeRedis.Test
{
    [TestClass]
    public class RedisLuaTest
    {
        private RedisClient _redisClient = default!;


        [TestInitialize]
        public void Initialize()
        {
            _redisClient = new RedisClient(RedisCommon.ConnectionString);
        }

        [TestCleanup]
        public void Cleanup()
        {
            // warning!!!!!!
            _redisClient.FlushDb();
        }

        [TestMethod]
        public void AddListTest()
        {
            var key = "RedisLuaTest:AddListTest";

            var list = AddListInner(key, 10);
            var len = _redisClient.LLen(key);
            Assert.AreEqual(len, list.Count);

            for (var i = 0; i < 10; i++)
            { 
                var val = _redisClient.Get(list[i]);
                Assert.AreEqual($"{i}", val);
            }
        }

        [TestMethod]
        public void RPOPList()
        {
            var key = "RedisLuaTest:RPOPList";
            var pendingMapKey = "RedisLuaTest:pending";

            var keys = AddListInner(key, 2);

            // Redis version >= 6.2.0: Added the `count` argument.
            var result = _redisClient.Eval(@"local vals, i = {}, 1
while (i <= tonumber(ARGV[1]))
do
    local message_id = redis.call('RPOP', KEYS[1])
    if (message_id == false) then
        return vals
    else
        local message = redis.call('GET', message_id)
        redis.call('HSET', KEYS[2], message_id, '')
        vals[i] = {message_id,message}
    end
    i = i + 1
end
return vals", new string[] { key, pendingMapKey }, new object[] { 5 });

            Console.WriteLine(result);
        }

        private List<string> AddListInner(string key, int count)
        {
            var list = new List<string>();
            for (var i = 0; i < count; i++)
            {
                var messageId = Guid.NewGuid().ToString("N");

                list.Add(messageId);
                _redisClient.Eval(@"
redis.call('SET', ARGV[1], ARGV[2])
redis.call('LPUSH', KEYS[1], ARGV[1])", new string[] { key },
        new object[] { messageId, $"{i}" });

            }

            return list;
        }
    }
}
