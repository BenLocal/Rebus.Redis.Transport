using Rebus.Exceptions;
using Rebus.Logging;
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
        private readonly CancellationTokenSource _cts = new CancellationTokenSource();
        private readonly ILog _log;

        private Channel<TransportMessage> _receivedChannel;

        public RedisTransport(IRebusLoggerFactory rebusLoggerFactory, IRedisManager redisManager,
            RedisOptions options, string inputQueueName) : base(inputQueueName)
        {
            _redisManager = redisManager;
            _options = options;
            _receivedChannel = Channel.CreateUnbounded<TransportMessage>();

            _log = rebusLoggerFactory.GetLogger<TransportMessage>();
        }

        public override void CreateQueue(string address)
        {
            _redisManager.CreateConsumerGroup(_options.QueueName, _options.ConsumerName);

            // Address null is oneway model
            if (Address != null)
            {
                ReclaimPendingMessagesLoop(_cts.Token);
                PollNewMessagesLoop(_cts.Token);
            }
        }

        public override async Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken)
        {
            try
            {
                TransportMessage result;
                try
                {
                    result = await _receivedChannel.Reader.ReadAsync(cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    _log.Debug("Reading from queue was cancelled");
                    return null;
                }
                catch (ChannelClosedException)
                {
                    _receivedChannel = Channel.CreateUnbounded<TransportMessage>();
                    return null;
                }

                if (result == null)
                {
                    return null;
                }

                result.Headers.TryGetValue("redis-id", out var id);
                _log.Debug($"dequeue redis message success, id :{id}");
                context.OnCompleted(_ =>
                {
                    _redisManager.Ack(_options.QueueName, _options.ConsumerName, id);
                    _log.Debug($"redis message completed, id :{id}");

                    return Task.CompletedTask;
                });

                context.OnAborted(ctx =>
                {
                });

                return result;
            }
            catch (Exception exception)
            {
                await Task.Delay(1000, cancellationToken);
                throw new RebusApplicationException(exception,
                $"Unexpected exception thrown while trying to dequeue a message from redis, queue address: {Address}");
            }

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
                        try
                        {
                            var messages = _redisManager
                                  .GetNewMessagesAsync(_options.QueueName,
                                      _options.ConsumerName, cancellationToken);

                            await WriterAsync(messages);
                        }
                        catch(Exception)
                        {
                            await Task.Delay(1000, cancellationToken);
                            //throw new RebusApplicationException(exception,
                            //    $"Unexpected exception thrown while polling to new message from redis, queue address: {Address}");
                        }
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

                    await ExecPendingMessagesInner(pendingMessages, 
                        _options.ConsumerName,
                        false,
                        cancellationToken);


                    if (_options.RedeliverInterval == TimeSpan.Zero ||
                        _options.ProcessingTimeout == TimeSpan.Zero)
                    {
                        return;
                    }


                    while (!cancellationToken.IsCancellationRequested)
                    {
                        try
                        {
                            await Task.Delay(_options.RedeliverInterval);

                            //first time, we want to read our pending messages, in case we crashed and are recovering.
                            pendingMessages = _redisManager.GetPendingMessagesAsync(_options.QueueName,
                                    _options.ConsumerName, cancellationToken);

                            await ExecPendingMessagesInner(pendingMessages, 
                                _options.ConsumerName,
                                true,
                                cancellationToken);
                        }
                        catch(Exception)
                        {
                            await Task.Delay(1000, cancellationToken);
                            //throw new RebusApplicationException(exception,
                            //    $"Unexpected exception thrown while polling to pending message from redis, queue address: {Address}");
                        }
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
            bool timeout,
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
                if ((timeout && pending.Idle >= _options.ProcessingTimeout.TotalMilliseconds)
                    || !timeout)
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

                if (_options.QueueType == QueueType.LIST)
                {
                    claimResult = claimResult.Where(x =>
                    {
                        var local = DateTime.Now;
                        var a = -1d;
                        if (x.Headers.TryGetValue(Headers.SentTime, out var timeStr))
                        {
                            a = (local - DateTime.Parse(timeStr)).TotalMilliseconds - _options.ProcessingTimeout.TotalMilliseconds;
                        }

                        return a >= 0;
                    });
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
