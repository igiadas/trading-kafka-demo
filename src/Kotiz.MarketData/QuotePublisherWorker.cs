using Confluent.Kafka;
using Kotiz.Contracts;
using Microsoft.Extensions.Options;

namespace Kotiz.MarketData;

/// <summary>Takes prices from the configured source and publishes them to "market.quotes".</summary>
public sealed class QuotePublisherWorker(
    IQuoteSource source,
    IProducer<string, string> producer,
    IOptions<MarketDataOptions> options,
    ILogger<QuotePublisherWorker> logger) : BackgroundService
{
    private readonly QuoteThrottle _throttle = new(options.Value.PublishInterval);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("Publishing {Source} prices to {Topic}", options.Value.Source, Topics.MarketQuotes);
        try
        {
            await source.RunAsync(Publish, stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Normal shutdown.
        }
        finally
        {
            // Produce() only queues messages in memory; send what is queued before exiting.
            producer.Flush(TimeSpan.FromSeconds(5));
        }
    }

    private void Publish(Quote quote)
    {
        if (!_throttle.ShouldPublish(quote.Symbol, quote.Timestamp))
            return;

        try
        {
            // Unlike the order API, we do not await each delivery (fire-and-forget with a callback):
            // prices are high volume, and if one is lost the next one replaces it a second later.
            // An order, on the other hand, must never be lost, so the API awaits ProduceAsync.
            producer.Produce(Topics.MarketQuotes, new Message<string, string>
            {
                Key = quote.Symbol,
                Value = KafkaJson.Serialize(quote)
            }, report =>
            {
                if (report.Error.IsError)
                    logger.LogWarning("Price for {Symbol} not delivered: {Reason}", quote.Symbol, report.Error.Reason);
            });
        }
        catch (ProduceException<string, string> ex)
        {
            // E.g. the local queue is full because Kafka is down. Dropping a price is acceptable.
            logger.LogWarning("Price for {Symbol} dropped: {Reason}", quote.Symbol, ex.Error.Reason);
        }
    }
}
