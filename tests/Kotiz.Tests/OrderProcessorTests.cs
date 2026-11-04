using FluentAssertions;
using Kotiz.Contracts;
using Kotiz.OrderProcessor.Kafka;
using Kotiz.OrderProcessor.Processing;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Processor = Kotiz.OrderProcessor.Processing.OrderProcessor;

namespace Kotiz.Tests;

public class OrderProcessorTests
{
    private readonly Mock<IExecutionPublisher> _executions = new();
    private readonly MatchingEngine _engine = new();
    private readonly Processor _processor;

    public OrderProcessorTests()
    {
        var market = Options.Create(new MarketOptions { Symbols = "AAPL,MSFT" });
        _processor = new Processor(new InMemoryProcessedOrderStore(), _engine, _executions.Object, market,
            NullLogger<Processor>.Instance);
    }

    private static OrderPlaced Order(OrderType type = OrderType.Market, OrderSide side = OrderSide.Buy,
        decimal? price = null, int quantity = 10, string symbol = "AAPL") =>
        new(Guid.NewGuid(), symbol, side, type, quantity, price, DateTimeOffset.UtcNow);

    private Task Price(decimal price, string symbol = "AAPL") =>
        _processor.ProcessQuoteAsync(new Quote(symbol, price, DateTimeOffset.UtcNow), CancellationToken.None);

    private void VerifyExecuted(OrderPlaced order, decimal atPrice) =>
        _executions.Verify(p => p.PublishAsync(
            It.Is<OrderExecuted>(e => e.OrderId == order.OrderId && e.Price == atPrice && e.Quantity == order.Quantity && e.Type == order.Type),
            It.IsAny<CancellationToken>()), Times.Once);

    private void VerifyNothingExecuted() =>
        _executions.Verify(p => p.PublishAsync(It.IsAny<OrderExecuted>(), It.IsAny<CancellationToken>()), Times.Never);

    [Fact]
    public async Task Market_order_executes_immediately_at_the_last_price()
    {
        await Price(230m);
        var order = Order();

        var outcome = await _processor.ProcessOrderAsync(order, CancellationToken.None);

        outcome.Should().Be(OrderOutcome.Executed);
        VerifyExecuted(order, 230m);
    }

    [Fact]
    public async Task Market_order_without_a_price_yet_waits_for_the_first_quote()
    {
        var order = Order();

        (await _processor.ProcessOrderAsync(order, CancellationToken.None)).Should().Be(OrderOutcome.Pending);
        VerifyNothingExecuted();

        await Price(231m);

        VerifyExecuted(order, 231m);
        _engine.PendingOrders("AAPL").Should().BeEmpty();
    }

    [Fact]
    public async Task Limit_buy_waits_until_the_price_falls_to_the_limit()
    {
        await Price(230m);
        var order = Order(OrderType.Limit, OrderSide.Buy, price: 225m);

        (await _processor.ProcessOrderAsync(order, CancellationToken.None)).Should().Be(OrderOutcome.Pending);
        await Price(226m);
        VerifyNothingExecuted();

        await Price(224.50m);

        VerifyExecuted(order, 224.50m);
    }

    [Fact]
    public async Task Stop_sell_executes_when_the_price_falls_to_the_stop()
    {
        await Price(230m);
        var stopLoss = Order(OrderType.Stop, OrderSide.Sell, price: 220m);
        await _processor.ProcessOrderAsync(stopLoss, CancellationToken.None);

        await Price(221m);
        VerifyNothingExecuted();
        await Price(219m);

        VerifyExecuted(stopLoss, 219m);
    }

    [Fact]
    public async Task Limit_order_already_satisfied_by_the_current_price_executes_immediately()
    {
        await Price(230m);
        var order = Order(OrderType.Limit, OrderSide.Sell, price: 225m);

        (await _processor.ProcessOrderAsync(order, CancellationToken.None)).Should().Be(OrderOutcome.Executed);
        VerifyExecuted(order, 230m);
    }

    [Fact]
    public async Task A_quote_only_affects_orders_of_its_own_symbol()
    {
        var aapl = Order(OrderType.Limit, OrderSide.Buy, price: 225m);
        await _processor.ProcessOrderAsync(aapl, CancellationToken.None);

        await Price(100m, symbol: "MSFT");

        VerifyNothingExecuted();
        _engine.PendingOrders("AAPL").Should().ContainSingle();
    }

    [Fact]
    public async Task Redelivered_order_is_skipped_and_not_executed_twice()
    {
        await Price(230m);
        var order = Order();

        await _processor.ProcessOrderAsync(order, CancellationToken.None);
        var second = await _processor.ProcessOrderAsync(order, CancellationToken.None);

        second.Should().Be(OrderOutcome.Duplicate);
        _executions.Verify(p => p.PublishAsync(It.IsAny<OrderExecuted>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Theory]
    [InlineData("XYZ", OrderType.Market, null, 10)]
    [InlineData("", OrderType.Market, null, 10)]
    [InlineData("AAPL", OrderType.Market, null, 0)]
    [InlineData("AAPL", OrderType.Limit, null, 10)]
    [InlineData("AAPL", OrderType.Stop, -1.0, 10)]
    public async Task Invalid_order_throws_InvalidMessageException_and_changes_nothing(string symbol, OrderType type, double? price, int quantity)
    {
        var act = () => _processor.ProcessOrderAsync(Order(type, price: (decimal?)price, quantity: quantity, symbol: symbol), CancellationToken.None);

        await act.Should().ThrowAsync<InvalidMessageException>();
        _engine.PendingOrders(symbol).Should().BeEmpty();
        VerifyNothingExecuted();
    }

    [Fact]
    public async Task Invalid_quote_throws_InvalidMessageException()
    {
        var act = () => Price(0m);

        await act.Should().ThrowAsync<InvalidMessageException>();
        _engine.LastPrice("AAPL").Should().BeNull();
    }

    [Fact]
    public async Task When_publishing_fails_the_order_stays_pending_so_a_retry_is_safe()
    {
        var first = Order(OrderType.Limit, OrderSide.Buy, price: 225m);
        var second = Order(OrderType.Limit, OrderSide.Buy, price: 225m);
        await _processor.ProcessOrderAsync(first, CancellationToken.None);
        await _processor.ProcessOrderAsync(second, CancellationToken.None);

        // The first execution is published, the second fails.
        _executions.SetupSequence(p => p.PublishAsync(It.IsAny<OrderExecuted>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask)
            .ThrowsAsync(new TimeoutException("broker unavailable"))
            .Returns(Task.CompletedTask);

        var attempt = () => Price(224m);
        await attempt.Should().ThrowAsync<TimeoutException>();
        _engine.PendingOrders("AAPL").Should().ContainSingle(o => o.OrderId == second.OrderId,
            "the order whose execution was published is removed; the failed one is still pending");

        await Price(224m); // the retry of the same quote

        _executions.Verify(p => p.PublishAsync(It.Is<OrderExecuted>(e => e.OrderId == first.OrderId), It.IsAny<CancellationToken>()), Times.Once);
        _executions.Verify(p => p.PublishAsync(It.Is<OrderExecuted>(e => e.OrderId == second.OrderId), It.IsAny<CancellationToken>()), Times.Exactly(2));
        _engine.PendingOrders("AAPL").Should().BeEmpty();
    }
}
