using System.Text;
using Confluent.Kafka;
using Kotiz.Contracts;

namespace Kotiz.OrderProcessor.Kafka;

public interface IDeadLetterPublisher
{
    Task PublishAsync(ConsumeResult<string, string> failed, Exception error, int attempts, CancellationToken cancellationToken);
}

/// <summary>
/// Copies a message that could not be processed to its dead-letter topic ("{topic}.dlt"), unchanged,
/// plus headers saying where it came from and why it failed. Someone can then inspect it
/// (e.g. in kafka-ui), fix the cause and replay it, while the main topic keeps flowing.
/// </summary>
public sealed class KafkaDeadLetterPublisher(IProducer<string, string> producer) : IDeadLetterPublisher
{
    public async Task PublishAsync(ConsumeResult<string, string> failed, Exception error, int attempts, CancellationToken cancellationToken)
    {
        var headers = new Headers();
        foreach (var header in failed.Message.Headers ?? [])
            headers.Add(header.Key, header.GetValueBytes());

        Add(headers, "dlt-original-topic", failed.Topic);
        Add(headers, "dlt-original-partition", failed.Partition.Value.ToString());
        Add(headers, "dlt-original-offset", failed.Offset.Value.ToString());
        Add(headers, "dlt-exception-type", error.GetType().Name);
        Add(headers, "dlt-exception-message", error.Message);
        Add(headers, "dlt-attempts", attempts.ToString());

        await producer.ProduceAsync(Topics.DeadLetterFor(failed.Topic), new Message<string, string>
        {
            Key = failed.Message.Key,
            Value = failed.Message.Value,
            Headers = headers
        }, cancellationToken);
    }

    private static void Add(Headers headers, string key, string value) => headers.Add(key, Encoding.UTF8.GetBytes(value));
}
