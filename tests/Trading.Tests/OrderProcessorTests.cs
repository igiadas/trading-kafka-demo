using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Trading.Contracts;
using Trading.OrderProcessor.Kafka;
using Trading.OrderProcessor.Processing;
using Processor = Trading.OrderProcessor.Processing.OrderProcessor;

namespace Trading.Tests;

public class OrderProcessorTests
{
    private readonly Mock<IExecutionPublisher> _executions = new();
    private readonly PositionBook _positions = new();
    private readonly Processor _processor;

    public OrderProcessorTests()
    {
        var trading = Options.Create(new TradingOptions { TradableSymbols = ["SAN", "BBVA"] });
        _processor = new Processor(
            new InMemoryProcessedOrderStore(), _positions, _executions.Object, trading,
            NullLogger<Processor>.Instance);
    }

    private static OrderPlaced Order(OrderSide side = OrderSide.Buy, int quantity = 100, string symbol = "SAN", decimal price = 4.25m) =>
        new(Guid.NewGuid(), symbol, side, quantity, price, DateTimeOffset.UtcNow);

    [Fact]
    public async Task New_order_publishes_an_execution_and_updates_the_position()
    {
        var order = Order();

        var outcome = await _processor.ProcessAsync(order, CancellationToken.None);

        outcome.Should().Be(ProcessingOutcome.Executed);
        _positions.GetNetQuantity("SAN").Should().Be(100);
        _executions.Verify(p => p.PublishAsync(
            It.Is<OrderExecuted>(e => e.OrderId == order.OrderId && e.Symbol == "SAN" && e.Quantity == 100 && e.Price == 4.25m),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task Sell_reduces_the_net_position()
    {
        await _processor.ProcessAsync(Order(OrderSide.Buy, 100), CancellationToken.None);
        await _processor.ProcessAsync(Order(OrderSide.Sell, 30), CancellationToken.None);

        _positions.GetNetQuantity("SAN").Should().Be(70);
    }

    [Fact]
    public async Task Redelivered_order_is_skipped_and_not_executed_twice()
    {
        var order = Order();

        await _processor.ProcessAsync(order, CancellationToken.None);
        var second = await _processor.ProcessAsync(order, CancellationToken.None);

        second.Should().Be(ProcessingOutcome.Duplicate);
        _positions.GetNetQuantity("SAN").Should().Be(100);
        _executions.Verify(p => p.PublishAsync(It.IsAny<OrderExecuted>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Theory]
    [InlineData("XYZ", 100, 4.25)]
    [InlineData("", 100, 4.25)]
    [InlineData("SAN", 0, 4.25)]
    [InlineData("SAN", 100, 0)]
    public async Task Invalid_order_throws_InvalidOrderException_and_changes_nothing(string symbol, int quantity, decimal price)
    {
        var act = () => _processor.ProcessAsync(Order(symbol: symbol, quantity: quantity, price: price), CancellationToken.None);

        await act.Should().ThrowAsync<InvalidOrderException>();
        _positions.GetNetQuantity(symbol).Should().Be(0);
        _executions.Verify(p => p.PublishAsync(It.IsAny<OrderExecuted>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task When_publishing_fails_nothing_changes_so_a_retry_is_safe()
    {
        var order = Order();
        _executions.SetupSequence(p => p.PublishAsync(It.IsAny<OrderExecuted>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new TimeoutException("broker unavailable"))
            .Returns(Task.CompletedTask);

        var firstAttempt = () => _processor.ProcessAsync(order, CancellationToken.None);
        await firstAttempt.Should().ThrowAsync<TimeoutException>();
        _positions.GetNetQuantity("SAN").Should().Be(0, "a failed attempt must not leave a half-applied order");

        var retry = await _processor.ProcessAsync(order, CancellationToken.None);

        retry.Should().Be(ProcessingOutcome.Executed, "the failed attempt must not mark the order as processed");
        _positions.GetNetQuantity("SAN").Should().Be(100);
    }
}
