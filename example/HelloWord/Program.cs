// See https://aka.ms/new-console-template for more information
using Rebus.Activation;
using Rebus.Config;
using Rebus.Handlers;
using Rebus.Redis.Transport.CSRedis;
using Rebus.Routing.TypeBased;

Console.WriteLine("Hello, World!");


using var activator = new BuiltinHandlerActivator();
using var timer = new System.Timers.Timer();

activator.Register(() => new PrintGuid());

var bus = Configure.With(activator)
    .Logging(l => l.Trace())
    .Transport(t => t.UseRedisStreamMq(o => {
        o.QueueName = "input";
        o.StreamEntriesCount = 10;
        o.ConnectionString = "localhost:6379";
    }))
    .Routing(r => r.TypeBased().Map<GuidMessage>("input"))
    .Start();

timer.Elapsed += delegate {
    //var data = DateTimeOffset.Now;
    //Console.WriteLine("Send message: {0}", data);   
    //bus.Send(new CurrentTimeMessage(data)).Wait();

    var id = Guid.NewGuid().ToString("N");
    Console.WriteLine("Send message: {0}", id);
    bus.Send(new GuidMessage(id)).Wait();

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

class PrintGuid : IHandleMessages<GuidMessage>
{
    public Task Handle(GuidMessage message)
    {
        Console.WriteLine($"Receive the guid is {message.TextA}");

        return Task.CompletedTask;
    }
}