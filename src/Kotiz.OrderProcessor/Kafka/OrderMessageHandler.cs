using System.Text.Json;
using Confluent.Kafka;
using Kotiz.Contracts;
using Kotiz.OrderProcessor.Processing;
using Microsoft.Extensions.Options;

namespace Kotiz.OrderProcessor.Kafka;

/// <summary>
/// Decides what happens to one consumed message (an order or a quote): process it, retry it,
/// or dead-letter it. When this method returns normally the message is "done" and its offset
/// can be committed.
/// </summary>
public sealed class OrderMessageHandler(
    IOrderProcessor processor,
    IDeadLetterPublisher deadLetters,
    IOptions<RetryOptions> retryOptions,
    ILogger<OrderMessageHandler> logger)
{
    public async Task HandleAsync(ConsumeResult<string, string> message, CancellationToken cancellationToken)
    {
        Func<CancellationToken, Task> process;
        try
        {
            process = message.Topic switch
            {
                Topics.OrdersPlaced => ProcessOrder(KafkaJson.Deserialize<OrderPlaced>(message.Message.Value)),
                Topics.MarketQuotes => ProcessQuote(KafkaJson.Deserialize<Quote>(message.Message.Value)),
                _ => throw new InvalidOperationException($"Unexpected topic '{message.Topic}'.")
            };
        }
        catch (JsonException ex)
        {
            // A "poison message": retrying can never fix malformed JSON.
            await SendToDeadLetterAsync(message, ex, attempts: 1, cancellationToken);
            return;
        }

        var options = retryOptions.Value;
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await process(cancellationToken);
                return;
            }
            catch (InvalidMessageException ex)
            {
                // Permanent business error: no point retrying.
                await SendToDeadLetterAsync(message, ex, attempt, cancellationToken);
                return;
            }
            catch (Exception ex) when (attempt < options.MaxAttempts && !cancellationToken.IsCancellationRequested)
            {
                // Possibly transient (e.g. broker briefly unavailable): wait and try again.
                // These retries block this consumer, so keep the total wait well below
                // max.poll.interval.ms (5 min by default) or the group will kick the consumer out.
                var delay = options.DelayBeforeRetry(attempt);
                logger.LogWarning(ex, "Attempt {Attempt}/{MaxAttempts} failed for {Topic} key {Key}, retrying in {Delay} ms",
                    attempt, options.MaxAttempts, message.Topic, message.Message.Key, delay.TotalMilliseconds);
                await Task.Delay(delay, cancellationToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // Out of attempts.
                await SendToDeadLetterAsync(message, ex, attempt, cancellationToken);
                return;
            }
        }
    }

    private Func<CancellationToken, Task> ProcessOrder(OrderPlaced order) => ct => processor.ProcessOrderAsync(order, ct);

    private Func<CancellationToken, Task> ProcessQuote(Quote quote) => ct => processor.ProcessQuoteAsync(quote, ct);

    private async Task SendToDeadLetterAsync(ConsumeResult<string, string> message, Exception error, int attempts, CancellationToken cancellationToken)
    {
        logger.LogError("Sending message from {Topic} [partition {Partition}] @ offset {Offset} to the DLT after {Attempts} attempt(s): {Error}",
            message.Topic, message.Partition.Value, message.Offset.Value, attempts, error.Message);
        await deadLetters.PublishAsync(message, error, attempts, cancellationToken);
    }
}
