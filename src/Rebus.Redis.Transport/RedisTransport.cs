using Rebus.Messages;
using Rebus.Transport;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Rebus.Redis.Transport
{
    public class RedisTransport : AbstractRebusTransport
    {
        private readonly IRedisManager _redisManager;
        private readonly RedisOptions _options;

        private readonly Channel<TransportMessage> _receivedChannel;
        private readonly CancellationTokenSource _cts = new CancellationTokenSource();

        public RedisTransport(IRedisManager redisManager,
            RedisOptions options) : base(options.QueueName)
        {
            _redisManager = redisManager;
            _options = options;
            _receivedChannel = Channel.CreateUnbounded<TransportMessage>(new UnboundedChannelOptions() { SingleReader = true, SingleWriter = true });
        }

        public override void CreateQueue(string address)
        {
            _redisManager.CreateConsumerGroup(_options.QueueName, _options.ConsumerName);

            ReclaimPendingMessagesLoop(_cts.Token);
            PollNewMessagesLoop(_cts.Token);
        }

        public override async Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken)
        {
            if (await _receivedChannel.Reader.WaitToReadAsync(cancellationToken))
            {
                if (_receivedChannel.Reader.TryRead(out var message))
                {
                    // ack
                    context.OnCommitted(ctx =>
                    {
                        var id = _redisManager.GetIdByTransportMessage(message);
                        if (string.IsNullOrWhiteSpace(id))
                        {
                            return Task.CompletedTask;
                        }

                        _redisManager.Ack(_options.QueueName, _options.ConsumerName, id);

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
            await _redisManager.PublishAsync(_options.QueueName, messages);
        }

        private Task PollNewMessagesLoop(CancellationToken cancellationToken)
        {
            return Task.Factory.StartNew(async () =>
            {
                try
                {
                    while (!cancellationToken.IsCancellationRequested)
                    {
                        var messages = _redisManager
                              .GetNewMessagesAsync(_options.QueueName,
                                  _options.ConsumerName,
                                  (int)_options.QueueDepth,
                                  cancellationToken);

                        await WriterAsync(messages);
                    }
                }
                catch (OperationCanceledException)
                {
                    // TODO
                }
            }, cancellationToken, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        }

        private Task ReclaimPendingMessagesLoop(CancellationToken cancellationToken)
        {
            return Task.Factory.StartNew(async () =>
            {
                try
                {
                    var pendingMessages = _redisManager.GetPendingMessagesAsync(_options.QueueName,
                        _options.ConsumerName, cancellationToken);

                    await ExecPendingMessagesInner(pendingMessages, _options.ConsumerName, cancellationToken);


                    if (_options.RedeliverInterval == TimeSpan.Zero ||
                        _options.ProcessingTimeout == TimeSpan.Zero)
                    {
                        return;
                    }


                    while (!cancellationToken.IsCancellationRequested)
                    {
                        await Task.Delay(_options.RedeliverInterval);

                        //first time, we want to read our pending messages, in case we crashed and are recovering.
                        pendingMessages = _redisManager.GetPendingMessagesAsync(_options.QueueName,
                                _options.ConsumerName, cancellationToken);

                        await ExecPendingMessagesInner(pendingMessages, _options.ConsumerName, cancellationToken);
                    }
                }
                catch (OperationCanceledException)
                {
                    // TODO
                }
            }, cancellationToken, TaskCreationOptions.LongRunning, TaskScheduler.Default);

        }

        private async Task ExecPendingMessagesInner(IEnumerable<PendingMessage> pendingMessages,
            string consumerGroup,
            CancellationToken cancellationToken)
        {
            if (pendingMessages == null)
            {
                return;
            }

            var ids = new List<string>();
            foreach (var pending in pendingMessages)
            {
                // Filter out messages that have not timed out yet
                if (pending.Idle >= _options.ProcessingTimeout.TotalMilliseconds)
                {
                    if (pending.Id != null)
                    {
                        ids.Add(pending.Id);
                    }
                }
            }

            if (!ids.Any())
            {
                return;
            }

            try
            {
                var claimResult = _redisManager.GetClaimMessagesAsync(_options.QueueName,
                    consumerGroup,
                    _options.QueueDepth, ids, cancellationToken);

                if (claimResult == null)
                { 
                    return;
                }

                // Enqueue claimed messages
                await WriterAsync(claimResult);
            }
            catch
            {
                // TODO logger
                return;
            }


        }

        private async Task WriterAsync(IEnumerable<TransportMessage> messages)
        {
            if (messages != null)
            {
                foreach (var message in messages)
                {
                    await _receivedChannel.Writer.WriteAsync(message);
                }
            }
        }
    }
}
