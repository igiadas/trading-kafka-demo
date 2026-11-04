using Confluent.Kafka;
using Trading.OrderProcessor.Kafka;
using Trading.OrderProcessor.Processing;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.Configure<TradingOptions>(builder.Configuration.GetSection("Trading"));
builder.Services.Configure<RetryOptions>(builder.Configuration.GetSection("Retry"));

// Producer for the "orders.executed" topic and the dead-letter topic. Same settings as the API.
builder.Services.AddSingleton<IProducer<string, string>>(sp =>
{
    var config = new ProducerConfig
    {
        BootstrapServers = builder.Configuration["Kafka:BootstrapServers"],
        Acks = Acks.All,
        EnableIdempotence = true,
        MessageTimeoutMs = 10_000
    };
    var logger = sp.GetRequiredService<ILogger<Program>>();
    return new ProducerBuilder<string, string>(config)
        .SetErrorHandler((_, e) => logger.LogError("Kafka producer error: {Reason}", e.Reason))
        .Build();
});

builder.Services.AddSingleton<IExecutionPublisher, KafkaExecutionPublisher>();
builder.Services.AddSingleton<IDeadLetterPublisher, KafkaDeadLetterPublisher>();
builder.Services.AddSingleton<IProcessedOrderStore, InMemoryProcessedOrderStore>();
builder.Services.AddSingleton<PositionBook>();
builder.Services.AddSingleton<IOrderProcessor, OrderProcessor>();
builder.Services.AddSingleton<OrderMessageHandler>();
builder.Services.AddHostedService<OrderConsumerWorker>();

var host = builder.Build();
host.Run();
