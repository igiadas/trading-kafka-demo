using System.Text.Json;
using Confluent.Kafka;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Kotiz.Contracts;
using Kotiz.OrderProcessor.Kafka;
using Kotiz.OrderProcessor.Processing;

namespace Kotiz.Tests;

public class OrderMessageHandlerTests
{
    private readonly Mock<IOrderProcessor> _processor = new();
    private readonly Mock<IDeadLetterPublisher> _deadLetters = new();
    private readonly OrderMessageHandler _handler;

    public OrderMessageHandlerTests()
    {
        // Zero delay so the retry tests run instantly.
        var retry = Options.Create(new RetryOptions { MaxAttempts = 3, BaseDelay = TimeSpan.Zero });
        _handler = new OrderMessageHandler(_processor.Object, _deadLetters.Object, retry, NullLogger<OrderMessageHandler>.Instance);
    }

    private static ConsumeResult<string, string> Consumed(string? value) => new()
    {
        Topic = Topics.OrdersPlaced,
        Partition = new Partition(1),
        Offset = new Offset(42),
        // null simulates a tombstone (a message with no value).
        Message = new Message<string, string> { Key = "SAN", Value = value! }
    };

    private static string ValidOrderJson() =>
        KafkaJson.Serialize(new OrderPlaced(Guid.NewGuid(), "SAN", OrderSide.Buy, 100, 4.25m, DateTimeOffset.UtcNow));

    private void VerifyNothingDeadLettered() =>
        _deadLetters.Verify(d => d.PublishAsync(It.IsAny<ConsumeResult<string, string>>(), It.IsAny<Exception>(), It.IsAny<int>(), It.IsAny<CancellationToken>()), Times.Never);

    [Fact]
    public async Task Valid_message_is_processed_once_and_not_dead_lettered()
    {
        await _handler.HandleAsync(Consumed(ValidOrderJson()), CancellationToken.None);

        _processor.Verify(p => p.ProcessAsync(It.Is<OrderPlaced>(o => o.Symbol == "SAN" && o.Quantity == 100), It.IsAny<CancellationToken>()), Times.Once);
        VerifyNothingDeadLettered();
    }

    [Theory]
    [InlineData("{ this is not json")]
    [InlineData("""{"orderId":"8b0e0c1e-5a43-4a0e-9a7e-2f6a4f1e9c11","symbol":"SAN","side":"Hold","quantity":1,"price":1}""")]
    [InlineData(null)]
    public async Task Unreadable_message_goes_straight_to_the_DLT_without_processing(string? value)
    {
        var message = Consumed(value);

        await _handler.HandleAsync(message, CancellationToken.None);

        _processor.Verify(p => p.ProcessAsync(It.IsAny<OrderPlaced>(), It.IsAny<CancellationToken>()), Times.Never);
        _deadLetters.Verify(d => d.PublishAsync(message, It.IsAny<JsonException>(), 1, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task Invalid_order_goes_to_the_DLT_without_retrying()
    {
        var message = Consumed(ValidOrderJson());
        var error = new InvalidOrderException("Symbol 'XYZ' is not tradable.");
        _processor.Setup(p => p.ProcessAsync(It.IsAny<OrderPlaced>(), It.IsAny<CancellationToken>())).ThrowsAsync(error);

        await _handler.HandleAsync(message, CancellationToken.None);

        _processor.Verify(p => p.ProcessAsync(It.IsAny<OrderPlaced>(), It.IsAny<CancellationToken>()), Times.Once);
        _deadLetters.Verify(d => d.PublishAsync(message, error, 1, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task Transient_failure_is_retried_and_then_succeeds()
    {
        _processor.SetupSequence(p => p.ProcessAsync(It.IsAny<OrderPlaced>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new TimeoutException("broker unavailable"))
            .ReturnsAsync(ProcessingOutcome.Executed);

        await _handler.HandleAsync(Consumed(ValidOrderJson()), CancellationToken.None);

        _processor.Verify(p => p.ProcessAsync(It.IsAny<OrderPlaced>(), It.IsAny<CancellationToken>()), Times.Exactly(2));
        VerifyNothingDeadLettered();
    }

    [Fact]
    public async Task Message_that_keeps_failing_goes_to_the_DLT_after_max_attempts()
    {
        var message = Consumed(ValidOrderJson());
        var error = new TimeoutException("broker unavailable");
        _processor.Setup(p => p.ProcessAsync(It.IsAny<OrderPlaced>(), It.IsAny<CancellationToken>())).ThrowsAsync(error);

        await _handler.HandleAsync(message, CancellationToken.None);

        _processor.Verify(p => p.ProcessAsync(It.IsAny<OrderPlaced>(), It.IsAny<CancellationToken>()), Times.Exactly(3));
        _deadLetters.Verify(d => d.PublishAsync(message, error, 3, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task Shutdown_during_processing_is_not_dead_lettered_so_the_message_is_redelivered()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        _processor.Setup(p => p.ProcessAsync(It.IsAny<OrderPlaced>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new OperationCanceledException(cts.Token));

        var act = () => _handler.HandleAsync(Consumed(ValidOrderJson()), cts.Token);

        await act.Should().ThrowAsync<OperationCanceledException>();
        _processor.Verify(p => p.ProcessAsync(It.IsAny<OrderPlaced>(), It.IsAny<CancellationToken>()), Times.Once);
        VerifyNothingDeadLettered();
    }

    [Fact]
    public void Backoff_doubles_on_each_retry()
    {
        var options = new RetryOptions { BaseDelay = TimeSpan.FromMilliseconds(500) };

        options.DelayBeforeRetry(1).Should().Be(TimeSpan.FromMilliseconds(500));
        options.DelayBeforeRetry(2).Should().Be(TimeSpan.FromSeconds(1));
        options.DelayBeforeRetry(3).Should().Be(TimeSpan.FromSeconds(2));
    }
}
