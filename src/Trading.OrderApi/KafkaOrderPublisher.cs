using Confluent.Kafka;
using Trading.Contracts;

namespace Trading.OrderApi;

public interface IOrderPublisher
{
    Task<TopicPartitionOffset> PublishAsync(OrderPlaced order, CancellationToken cancellationToken);
}

public sealed class KafkaOrderPublisher(IProducer<string, string> producer) : IOrderPublisher
{
    public async Task<TopicPartitionOffset> PublishAsync(OrderPlaced order, CancellationToken cancellationToken)
    {
        var message = new Message<string, string>
        {
            // KEY = symbol. Kafka hashes the key to pick the partition, so every order
            // for "SAN" goes to the same partition and keeps its relative order.
            Key = order.Symbol,
            Value = KafkaJson.Serialize(order)
        };

        // ProduceAsync completes when the broker acknowledges the write (acks=all, see Program.cs).
        // Awaiting each message keeps the code simple; for high throughput use Produce() with a
        // delivery handler and let the client batch messages.
        var result = await producer.ProduceAsync(Topics.OrdersPlaced, message, cancellationToken);
        return result.TopicPartitionOffset;
    }
}
