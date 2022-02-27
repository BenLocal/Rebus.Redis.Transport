using Newtonsoft.Json;
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

        private readonly string consumerGroup = "aaa";
        private readonly Channel<TransportMessage> _receiveedChannel;

        private Task? receiveTask = null;

        public ReidsTransport(string inputQueueName,
            IRedisManager redisManager) : base(inputQueueName)
        {
            _inputQueueName = inputQueueName;
            _redisManager = redisManager;
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
                        _redisManager.Ack(_inputQueueName, consumerGroup, "TODO");

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
            return Task.Factory.StartNew(async () =>
            {
                try
                {
                    //first time, we want to read our pending messages, in case we crashed and are recovering.
                    var pendingMessages = _redisManager.PollStreamsPendingMessagesAsync(_inputQueueName,
                        consumerGroup, TimeSpan.FromSeconds(2), cancellationToken);
                    await WriterAsync(pendingMessages);


                    while (!cancellationToken.IsCancellationRequested)
                    {
                        var messages = _redisManager
                            .PollStreamsPendingMessagesAsync(_inputQueueName,
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
    }
}
