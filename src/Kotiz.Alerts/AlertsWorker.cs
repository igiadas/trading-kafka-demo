using Confluent.Kafka;
using Kotiz.Contracts;

namespace Kotiz.Alerts;

/// <summary>
/// Third, independent consumer of the same topics, in its own group ("alerts"). It deliberately makes
/// different choices from the order processor, because an alert is not an order:
/// - auto-commit: an alert may (rarely) be lost or repeated after a crash, and that is acceptable;
/// - starts at the latest offset: on its first run, alerts about the past are of no use.
/// </summary>
public sealed class AlertsWorker(AlertService alerts, IConfiguration configuration, ILogger<AlertsWorker> logger) : BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken stoppingToken) =>
        Task.Run(() => ConsumeLoopAsync(stoppingToken), stoppingToken);

    private async Task ConsumeLoopAsync(CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = configuration["Kafka:BootstrapServers"],
            GroupId = configuration["Kafka:GroupId"],
            EnableAutoCommit = true,
            AutoOffsetReset = AutoOffsetReset.Latest
        };

        using var consumer = new ConsumerBuilder<string, string>(config)
            .SetErrorHandler((_, error) => logger.LogWarning("Kafka consumer error: {Reason}", error.Reason))
            .Build();
        consumer.Subscribe([Topics.OrdersExecuted, Topics.MarketQuotes]);
        logger.LogInformation("Watching {Executions} and {Quotes} as group {GroupId}", Topics.OrdersExecuted, Topics.MarketQuotes, config.GroupId);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var result = consumer.Consume(stoppingToken);
                await alerts.HandleAsync(result.Topic, result.Message.Value, stoppingToken);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Normal shutdown.
        }
        finally
        {
            consumer.Close();
        }
    }
}
