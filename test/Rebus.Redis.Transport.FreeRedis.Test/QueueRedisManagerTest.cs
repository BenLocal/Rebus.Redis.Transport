using FreeRedis;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;
using Rebus.Config;
using Rebus.Messages;
using System.Collections.Generic;
using System.Linq;

namespace Rebus.Redis.Transport.FreeRedis.Test
{
    [TestClass]
    public class QueueRedisManagerTest
    {
        private QueueRedisManager _manager = default!;
        private RedisClient _redisClient = default!;
        private RedisOptions _options = default!;

        [TestInitialize]
        public void Initialize()
        {
            _options = new RedisOptions()
            {
                ConnectionString = RedisCommon.ConnectionString,
                QueueName = "inputTest",
                ConsumerName = "consumerTest",
                QueueDepth = 10,
            };

            _redisClient = new RedisClient(_options.ConnectionString);
            _manager = new QueueRedisManager(_options);

            Cleanup();
        }

        [TestCleanup]
        public void Cleanup()
        {
            var len = _redisClient.LLen(_options.QueueName);
            if (len > 0)
            {
                var keys = _redisClient.LRange(_options.QueueName, 0, len);
                _redisClient.Del(keys.Select(x => $"{_options.QueueName}:{x}").ToArray());
            }

            _redisClient.Del($"{_options.QueueName}:{_options.ConsumerName}");
            _redisClient.Del(_options.QueueName);

        }

        [TestMethod]
        public void TestPublishAsyncSuccess()
        {
            var messages = new List<TransportMessage>()
            {
                new TransportMessage(new Dictionary<string, string>()
                {
                    { "header1", "a"},
                    { "header2", "2"},
                }, new byte[] { 0,1,2,3,4,5}),

                new TransportMessage(new Dictionary<string, string>()
                {
                    { "header1", "b"},
                    { "header2", "3"},
                }, new byte[] { 5, 4, 3, 2, 1, 0 })
            };

            _manager.PublishAsync(_options.QueueName, messages);

            var length = _redisClient.LLen(_options.QueueName);
            Assert.AreEqual(messages.Count, length);

            var results = _redisClient.LRange(_options.QueueName, 0, length);
            Assert.AreEqual(length, results.Count());

            for (var i = 0; i< results.Length; i++)
            {
                var msgID = results[i];
                var str = _redisClient.Get($"{_options.QueueName}:{msgID}");
                Assert.AreEqual(str, JsonConvert.SerializeObject(messages[results.Length - 1 - i]));
            }


        }

        [TestMethod]
        public void TestNewMessagesSuccess()
        {
            var messages = new List<TransportMessage>()
            {
                new TransportMessage(new Dictionary<string, string>()
                {
                    { "header1", "a"},
                    { "header2", "2"},
                }, new byte[] { 0,1,2,3,4,5}),

                new TransportMessage(new Dictionary<string, string>()
                {
                    { "header1", "b"},
                    { "header2", "3"},
                }, new byte[] { 5, 4, 3, 2, 1, 0 })
            };

            _manager.PublishAsync(_options.QueueName, messages);

            var getMessages = _manager.GetNewMessagesAsync(_options.QueueName,
                _options.ConsumerName,
                default);

            var count = 0;
            var index = 0;

            var outMessages = new List<TransportMessage>();
            foreach (var message in getMessages)
            {
                var inStr = JsonConvert.SerializeObject(messages[index]);
                var outStr = JsonConvert.SerializeObject(message);

                Assert.AreEqual(inStr, outStr);
                index++;

                count++;

                outMessages.Add(message);
            }

            var pandingKey = $"{_options.QueueName}:{_options.ConsumerName}";
            var pendingLen = _redisClient.HLen(pandingKey);

            Assert.AreEqual(pendingLen, count);

            foreach (var message in outMessages)
            {
                // ack
                var res = message.Headers.TryGetValue("redis-id", out var messageId);
                Assert.IsTrue(res);
                Assert.IsNotNull(messageId);

                _manager.Ack(_options.QueueName, _options.ConsumerName, messageId);
            }

            var pendingLen1 = _redisClient.HLen(pandingKey);
            Assert.AreEqual(pendingLen1, 0);

            foreach (var message in outMessages)
            {
                var res = message.Headers.TryGetValue("redis-id", out var messageId);
                Assert.IsTrue(res);
                Assert.IsNotNull(messageId);

                var exists = _redisClient.Exists($"{_options.QueueName}:{messageId}");
                Assert.IsFalse(exists);
            }

            Assert.AreEqual(messages.Count, count);
        }

        [TestMethod]
        public void TestPendingMessagesSuccess()
        {
            var messages = new List<TransportMessage>()
            {
                new TransportMessage(new Dictionary<string, string>()
                {
                    { "header1", "a"},
                    { "header2", "2"},
                }, new byte[] { 0,1,2,3,4,5}),

                new TransportMessage(new Dictionary<string, string>()
                {
                    { "header1", "b"},
                    { "header2", "3"},
                }, new byte[] { 5, 4, 3, 2, 1, 0 })
            };

            _manager.PublishAsync(_options.QueueName, messages);

            var getMessages = _manager.GetNewMessagesAsync(_options.QueueName,
                _options.ConsumerName,
                default);

            var count = 0;
            foreach (var message in getMessages)
            {
                count++;
            }

            Assert.AreEqual(messages.Count, count);

            var ids = _manager.GetPendingMessagesAsync(_options.QueueName, _options.ConsumerName, default);

            Assert.AreEqual(ids.Count(), count);
            Assert.IsNotNull(ids);
            var outMessages = _manager.GetClaimMessagesAsync(_options.QueueName, _options.ConsumerName, 0, ids.Select(x => x.Id), default);

            foreach (var message in outMessages)
            {
                // ack
                var res = message.Headers.TryGetValue("redis-id", out var messageId);
                Assert.IsTrue(res);
                Assert.IsNotNull(messageId);

                _manager.Ack(_options.QueueName, _options.ConsumerName, messageId);
            }

        }
    }
}