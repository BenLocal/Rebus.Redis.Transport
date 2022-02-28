﻿using Newtonsoft.Json;
using Rebus.Config;
using Rebus.Messages;
using Rebus.Transport;
using System;
using System.Threading;
using System.Threading.Channels;

namespace Rebus.Redis.Transport
{
    public class ReidsTransport : AbstractRebusTransport
    {
        private readonly string _inputQueueName;
        private readonly IRedisManager _redisManager;
        private readonly RedisOptions _options;

        private readonly string consumerGroup = "aaa";
        private readonly Channel<TransportMessage> _receiveedChannel;

        private Task? receiveTask = null;

        public ReidsTransport(string inputQueueName,
            IRedisManager redisManager,
            RedisOptions options) : base(inputQueueName)
        {
            _inputQueueName = inputQueueName;
            _redisManager = redisManager;
            _options = options;
            _receiveedChannel = Channel.CreateUnbounded<TransportMessage>(new UnboundedChannelOptions() { SingleReader = true, SingleWriter = true });
        }

        public override void CreateQueue(string address)
        {
            _redisManager.CreateConsumerGroup(_inputQueueName, consumerGroup);
        }

        public override async Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken)
        {
            if (receiveTask == null)
            {
                receiveTask = Receiving(cancellationToken);
            }

            if (await _receiveedChannel.Reader.WaitToReadAsync(cancellationToken))
            {
                if (_receiveedChannel.Reader.TryRead(out var message))
                {
                    // ack
                    context.OnCommitted(ctx =>
                    {
                        if (message.Headers.TryGetValue("redis-id", out var id))
                        {
                            _redisManager.Ack(_inputQueueName, consumerGroup, id);
                        }

                        return Task.CompletedTask;
                    });

                    return message;
                }
            }
            
            return default!;
        }

        protected override async Task SendOutgoingMessages(IEnumerable<OutgoingMessage> outgoingMessages, ITransactionContext context)
        {
            var messages = outgoingMessages.Select(x => x.TransportMessage);
            await _redisManager.PublishAsync(_inputQueueName, messages);
        }

        private Task Receiving(CancellationToken cancellationToken)
        {
            // reclaim Pending Messages
            Task.Factory.StartNew(async () =>
            {
                try
                {
                    var pendMessages = _redisManager.GetPendingMessagesAsync(_inputQueueName,
                            consumerGroup, cancellationToken);

                    await ExecPendMessagesInner(pendMessages);

                    while (!cancellationToken.IsCancellationRequested)
                    {
                        await Task.Delay(_options.RedeliverInterval);

                        //first time, we want to read our pending messages, in case we crashed and are recovering.
                        pendMessages = _redisManager.GetPendingMessagesAsync(_inputQueueName,
                                consumerGroup, cancellationToken);

                        if (pendMessages == null)
                        {
                            continue;
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // TODO
                }
            }, cancellationToken, TaskCreationOptions.LongRunning, TaskScheduler.Default);

            return Task.Factory.StartNew(async () =>
            {
                try
                {
                    while (!cancellationToken.IsCancellationRequested)
                    {
                        var messages = _redisManager
                              .GetNewMessagesAsync(_inputQueueName,
                                  consumerGroup, TimeSpan.FromSeconds(2), cancellationToken);

                        await WriterAsync(messages);
                    }

                    async Task WriterAsync(IEnumerable<TransportMessage> messages)
                    {
                        if (messages != null)
                        {
                            foreach (var message in messages)
                            {
                                await _receiveedChannel.Writer.WriteAsync(message);
                            }
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // TODO
                }
            }, cancellationToken, TaskCreationOptions.LongRunning, TaskScheduler.Default);  
        }

        private Task ExecPendMessagesInner(IEnumerable<PendingMessage> pendMessages)
        { 
            
        }
    }
}