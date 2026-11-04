using System.Text.Json;
using Confluent.Kafka;
using Kotiz.Contracts;

namespace Kotiz.OrderApi.Portfolio;

/// <summary>
/// Builds <see cref="PortfolioState"/> by reading orders.executed and market.quotes from the beginning
/// every time the API starts (a "projection" of the event log). Nothing is stored anywhere else:
/// the Kafka log is the source of truth, so a restart rebuilds exactly the same portfolio.
/// </summary>
public sealed class PortfolioProjectionWorker(
    PortfolioState portfolio,
    IConfiguration configuration,
    ILogger<PortfolioProjectionWorker> logger) : BackgroundService
{
    // Consume() blocks, so the loop runs on its own thread (same as in the order processor).
    protected override Task ExecuteAsync(CancellationToken stoppingToken) =>
        Task.Run(() => ConsumeLoop(stoppingToken), stoppingToken);

    private void ConsumeLoop(CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = configuration["Kafka:BootstrapServers"],
            // A new, unique group on every start, with no commits: it always starts with no offsets,
            // so AutoOffsetReset.Earliest makes it read both topics from the beginning.
            GroupId = $"portfolio-view-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        using var consumer = new ConsumerBuilder<string, string>(config)
            .SetErrorHandler((_, error) => logger.LogWarning("Kafka consumer error: {Reason}", error.Reason))
            .Build();
        consumer.Subscribe([Topics.OrdersExecuted, Topics.MarketQuotes]);
        logger.LogInformation("Rebuilding the portfolio from {Executions} and {Quotes}", Topics.OrdersExecuted, Topics.MarketQuotes);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var result = consumer.Consume(stoppingToken);
                try
                {
                    if (result.Topic == Topics.OrdersExecuted)
                        portfolio.ApplyExecution(KafkaJson.Deserialize<OrderExecuted>(result.Message.Value));
                    else
                        portfolio.UpdatePrice(KafkaJson.Deserialize<Quote>(result.Message.Value));
                }
                catch (JsonException ex)
                {
                    // A read model only reads: a bad message is reported and skipped. (Bad input
                    // never reaches these topics: the order processor sends it to the DLT.)
                    logger.LogError("Skipping unreadable message from {Topic} [partition {Partition}] @ offset {Offset}: {Error}",
                        result.Topic, result.Partition.Value, result.Offset.Value, ex.Message);
                }
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
