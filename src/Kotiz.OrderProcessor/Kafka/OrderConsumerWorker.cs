using Confluent.Kafka;
using Kotiz.Contracts;

namespace Kotiz.OrderProcessor.Kafka;

/// <summary>
/// The consume loop: read a message, handle it, commit its offset. One message at a time.
/// </summary>
public sealed class OrderConsumerWorker(
    OrderMessageHandler handler,
    IConfiguration configuration,
    ILogger<OrderConsumerWorker> logger) : BackgroundService
{
    // Consume() blocks the calling thread, so the loop runs on its own thread;
    // otherwise the host would never finish starting.
    protected override Task ExecuteAsync(CancellationToken stoppingToken) =>
        Task.Run(() => ConsumeLoopAsync(stoppingToken), stoppingToken);

    private async Task ConsumeLoopAsync(CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = configuration["Kafka:BootstrapServers"],
            // Every instance with the same GroupId shares the work: each partition
            // is assigned to exactly one consumer of the group.
            GroupId = configuration["Kafka:GroupId"],
            // We commit ourselves, only after a message is fully handled (at-least-once).
            EnableAutoCommit = false,
            // A brand-new group starts from the beginning of the topic, not just new messages.
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using var consumer = new ConsumerBuilder<string, string>(config)
            .SetPartitionsAssignedHandler((_, partitions) =>
                logger.LogInformation("Partitions assigned: [{Partitions}]", string.Join(", ", partitions.Select(p => p.Partition.Value))))
            .SetPartitionsRevokedHandler((_, partitions) =>
                logger.LogInformation("Partitions revoked: [{Partitions}]", string.Join(", ", partitions.Select(p => p.Partition.Value))))
            .SetErrorHandler((_, error) => logger.LogWarning("Kafka consumer error: {Reason}", error.Reason))
            .Build();

        consumer.Subscribe(Topics.OrdersPlaced);
        logger.LogInformation("Subscribed to {Topic} as group {GroupId}", Topics.OrdersPlaced, config.GroupId);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<string, string> result;
                try
                {
                    result = consumer.Consume(stoppingToken);
                }
                catch (ConsumeException ex) when (!ex.Error.IsFatal)
                {
                    logger.LogWarning("Consume failed, will try again: {Reason}", ex.Error.Reason);
                    continue;
                }

                logger.LogInformation("Received key {Key} from partition {Partition} @ offset {Offset}",
                    result.Message.Key, result.Partition.Value, result.Offset.Value);

                // Handles the message completely: processed, skipped as duplicate, or sent to the DLT.
                // It only throws if even the DLT is unreachable; then we stop WITHOUT committing,
                // and the message is consumed again when the service restarts.
                await handler.HandleAsync(result, stoppingToken);

                // Commit AFTER handling. If we crash between the two lines, the message is
                // delivered again (at-least-once), and the idempotency check makes that harmless.
                consumer.Commit(result);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Normal shutdown.
        }
        catch (Exception ex)
        {
            logger.LogCritical(ex, "Consumer loop stopped by an unexpected error; uncommitted messages will be redelivered");
            throw;
        }
        finally
        {
            // Leave the group cleanly so its partitions are reassigned immediately.
            consumer.Close();
        }
    }
}
