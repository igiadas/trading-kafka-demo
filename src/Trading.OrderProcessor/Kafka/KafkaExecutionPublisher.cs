using Confluent.Kafka;
using Trading.Contracts;

namespace Trading.OrderProcessor.Kafka;

public interface IExecutionPublisher
{
    Task PublishAsync(OrderExecuted execution, CancellationToken cancellationToken);
}

public sealed class KafkaExecutionPublisher(IProducer<string, string> producer) : IExecutionPublisher
{
    public Task PublishAsync(OrderExecuted execution, CancellationToken cancellationToken) =>
        producer.ProduceAsync(Topics.OrdersExecuted, new Message<string, string>
        {
            Key = execution.Symbol,
            Value = KafkaJson.Serialize(execution)
        }, cancellationToken);
}
