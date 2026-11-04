using Confluent.Kafka;
using Kotiz.MarketData;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.Configure<MarketDataOptions>(builder.Configuration.GetSection("Market"));

var source = builder.Configuration["Market:Source"] ?? "Simulated";
if (source.Equals("Finnhub", StringComparison.OrdinalIgnoreCase))
    builder.Services.AddSingleton<IQuoteSource, FinnhubQuoteSource>();
else if (source.Equals("Simulated", StringComparison.OrdinalIgnoreCase))
    builder.Services.AddSingleton<IQuoteSource, SimulatedQuoteSource>();
else
    throw new InvalidOperationException($"Unknown Market:Source '{source}'. Use Simulated or Finnhub.");

builder.Services.AddSingleton<IProducer<string, string>>(sp =>
{
    var config = new ProducerConfig
    {
        BootstrapServers = builder.Configuration["Kafka:BootstrapServers"],
        Acks = Acks.All,
        EnableIdempotence = true
    };
    var logger = sp.GetRequiredService<ILogger<Program>>();
    return new ProducerBuilder<string, string>(config)
        .SetErrorHandler((_, e) => logger.LogError("Kafka producer error: {Reason}", e.Reason))
        .Build();
});

builder.Services.AddHostedService<QuotePublisherWorker>();

var host = builder.Build();
host.Run();
