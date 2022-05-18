using FreeRedis;
using NUnit.Framework;
using System;
using System.Collections.Generic;

namespace Rebus.Redis.Transport.FreeRedis.Test
{
    public class RedisLuaTest
    {
        private RedisClient _redisClient = default!;

        [SetUp]
        public void Initialize()
        {
            _redisClient = new RedisClient(RedisCommon.ConnectionString);
        }

        private string Get_Script = @"local vals, i = {}, 1
while (i <= tonumber(ARGV[1]))
do
    local message_id = redis.call('RPOP', KEYS[1])
    if (message_id == false) then
        return vals
    else
        local message = redis.call('GET', KEYS[1]..':'..message_id)
        if message then
            redis.call('HSET', KEYS[2], message_id, '')
            vals[i] = {message_id,message}
            i = i + 1
        end
    end
end
return vals";

        private string Set_Script = @"redis.call('SET', KEYS[1]..':'..ARGV[1], ARGV[2])
redis.call('EXPIRE', KEYS[1]..':'..ARGV[1], 86400)
redis.call('LPUSH', KEYS[1], ARGV[1])";

        private string Set_Script_Error = @"redis.call('LPUSH', KEYS[1], ARGV[1])";

        [TearDown]
        public void Cleanup()
        {
            // warning!!!!!!
            _redisClient.FlushDb();
        }

        [Test]
        public void AddListTest()
        {
            var key = "RedisLuaTest:AddListTest";

            var list = AddListInner(key, 10);
            var len = _redisClient.LLen(key);
            Assert.AreEqual(len, list.Count);

            for (var i = 0; i < 10; i++)
            { 
                var val = _redisClient.Get($"{key}:{list[i]}");
                Assert.AreEqual($"{i}", val);
            }
        }

        [Test]
        public void RPOPList_Error()
        {
            var key = "RedisLuaTest:RPOPList";
            var pendingMapKey = "RedisLuaTest:pending";

            var keys = AddListInnerError(key, 2);

            // Redis version >= 6.2.0: Added the `count` argument.
            var result = _redisClient.Eval(Get_Script, new string[] { key, pendingMapKey }, new object[] { 5 });
            if (result is object[] res)
            {
                Assert.AreEqual(res.Length, 0);
            }
            else
            {
                Assert.Fail();
            }

            Console.WriteLine(result);
        }

        [Test]
        public void RPOPList()
        {
            var key = "RedisLuaTest:RPOPList";
            var pendingMapKey = "RedisLuaTest:pending";

            var keys = AddListInner(key, 2);

            // Redis version >= 6.2.0: Added the `count` argument.
            var result = _redisClient.Eval(Get_Script, new string[] { key, pendingMapKey }, new object[] { 5 });

            if (result is object[] res)
            {
                Assert.AreEqual(res.Length, 2);
            }
            else
            {
                Assert.Fail();
            }

            Console.WriteLine(result);
        }

        private List<string> AddListInner(string key, int count)
        {
            var list = new List<string>();
            for (var i = 0; i < count; i++)
            {
                var messageId = Guid.NewGuid().ToString("N");

                list.Add(messageId);
                _redisClient.Eval(Set_Script, new string[] { key },
        new object[] { messageId, $"{i}" });

            }

            return list;
        }

        private List<string> AddListInnerError(string key, int count)
        {
            var list = new List<string>();
            for (var i = 0; i < count; i++)
            {
                var messageId = Guid.NewGuid().ToString("N");

                list.Add(messageId);
                _redisClient.Eval(Set_Script_Error, new string[] { key },
        new object[] { messageId, $"{i}" });

            }

            return list;
        }
    }
}
