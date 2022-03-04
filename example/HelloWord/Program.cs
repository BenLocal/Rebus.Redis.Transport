// See https://aka.ms/new-console-template for more information
using Rebus.Activation;
using Rebus.Config;
using Rebus.Handlers;
using Rebus.Routing.TypeBased;
using Rebus.Redis.Transport.FreeRedis;
using Rebus.Retry.Simple;
using Rebus.Bus;
using Rebus.Exceptions;
using Rebus.Messages;
using Rebus.Persistence.InMem;

using var activator = new BuiltinHandlerActivator();
using var timer = new System.Timers.Timer();

activator.Register((bus, ctx) =>
{
    return new PrintGuid(bus);
});

var bus = Configure.With(activator)
    .Logging(l => l.Trace())
    .Transport(t => t.UseRedisStreamMq(o => {
        o.QueueName = "input";
        o.ConsumerName = "output";
        o.QueueDepth = 10;
        o.ConnectionString = "127.0.0.1:6379";
        o.QueueType = Rebus.Redis.Transport.QueueType.LIST;
    }))
    .Subscriptions(s => s.StoreInMemory())
    .Routing(r => r.TypeBased().Map<GuidMessage>("input"))
    .Options(b => b.SimpleRetryStrategy(secondLevelRetriesEnabled: true))
    .Start();

timer.Elapsed += delegate {
    //var data = DateTimeOffset.Now;
    //Console.WriteLine("Send message: {0}", data);   
    //bus.Send(new CurrentTimeMessage(data)).Wait();

    var id = Guid.NewGuid().ToString("N");
    Console.WriteLine("Send message: {0}", id);
    //bus.Send(new GuidMessage(id)).Wait();
    bus.Send(new GuidMessage(id)).Wait();
    //bus.Reply(new GuidMessage(id)).Wait();

};
timer.Interval = 1000;
timer.Start();

Console.WriteLine("Press enter to quit");
Console.ReadLine();

public class CurrentTimeMessage
{
    public DateTimeOffset Time { get; }

    public CurrentTimeMessage(DateTimeOffset time)
    {
        Time = time;
    }
}

public class GuidMessage
{ 
    public string TextA { get; set; }

    // {"$type":"GuidMessage, HelloWord","TextA":"f52e736cff4e45b99073526bf003f26c"}
    // body is bytes array
    public GuidMessage(string textA)
    {
        TextA = textA;
    }
}

class PrintDateTime : IHandleMessages<CurrentTimeMessage>
{
    public Task Handle(CurrentTimeMessage message)
    {
        Console.WriteLine("Receive the time is {0}", message.Time);

        return Task.CompletedTask;
    }
}

class PrintGuid : IHandleMessages<GuidMessage>, IHandleMessages<IFailed<GuidMessage>>
{
    readonly IBus _bus;

    public PrintGuid(IBus bus)
    {
        _bus = bus;
    }

    public async Task Handle(GuidMessage message)
    {
        Console.WriteLine($"Receive the guid is {message.TextA}");
        //await Task.Delay(2000);
    }

    public async Task Handle(IFailed<GuidMessage> message)
    {
        const int maxDeferCount = 5;
        var deferCount = Convert.ToInt32(message.Headers.GetValueOrDefault(Headers.DeferCount));
        if (deferCount >= maxDeferCount)
        {
            await _bus.Advanced.TransportMessage.Deadletter($"Failed after {deferCount} deferrals\n\n{message.ErrorDescription}");
            return;
        }
        await _bus.Advanced.TransportMessage.Defer(TimeSpan.FromSeconds(30));
    }
}