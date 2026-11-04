using System.Text;
using Confluent.Kafka;
using FluentAssertions;
using Moq;
using Trading.Contracts;
using Trading.OrderProcessor.Kafka;

namespace Trading.Tests;

public class KafkaDeadLetterPublisherTests
{
    [Fact]
    public async Task Copies_the_message_unchanged_to_the_DLT_with_failure_headers()
    {
        Message<string, string>? sent = null;
        string? sentTopic = null;
        var producer = new Mock<IProducer<string, string>>();
        producer
            .Setup(p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<string, string>>(), It.IsAny<CancellationToken>()))
            .Callback<string, Message<string, string>, CancellationToken>((topic, message, _) => (sentTopic, sent) = (topic, message))
            .ReturnsAsync(new DeliveryResult<string, string>());

        var failed = new ConsumeResult<string, string>
        {
            Topic = Topics.OrdersPlaced,
            Partition = new Partition(2),
            Offset = new Offset(17),
            Message = new Message<string, string> { Key = "XYZ", Value = "{\"symbol\":\"XYZ\"}" }
        };

        await new KafkaDeadLetterPublisher(producer.Object)
            .PublishAsync(failed, new InvalidOperationException("boom"), attempts: 3, CancellationToken.None);

        sentTopic.Should().Be(Topics.OrdersPlacedDeadLetter);
        sent!.Key.Should().Be("XYZ");
        sent.Value.Should().Be("{\"symbol\":\"XYZ\"}");

        string Header(string key) => Encoding.UTF8.GetString(sent.Headers.GetLastBytes(key));
        Header("dlt-original-topic").Should().Be(Topics.OrdersPlaced);
        Header("dlt-original-partition").Should().Be("2");
        Header("dlt-original-offset").Should().Be("17");
        Header("dlt-exception-type").Should().Be(nameof(InvalidOperationException));
        Header("dlt-exception-message").Should().Be("boom");
        Header("dlt-attempts").Should().Be("3");
    }
}
