using FluentAssertions;
using Kotiz.Contracts;
using Kotiz.OrderProcessor.Processing;

namespace Kotiz.Tests;

public class MatchingEngineTests
{
    [Theory]
    // Market: always.
    [InlineData(OrderType.Market, OrderSide.Buy, null, 100d, true)]
    [InlineData(OrderType.Market, OrderSide.Sell, null, 100d, true)]
    // Limit buy: at the limit or cheaper.
    [InlineData(OrderType.Limit, OrderSide.Buy, 100d, 100.01d, false)]
    [InlineData(OrderType.Limit, OrderSide.Buy, 100d, 100d, true)]
    [InlineData(OrderType.Limit, OrderSide.Buy, 100d, 99d, true)]
    // Limit sell (take profit): at the limit or higher.
    [InlineData(OrderType.Limit, OrderSide.Sell, 100d, 99.99d, false)]
    [InlineData(OrderType.Limit, OrderSide.Sell, 100d, 100d, true)]
    // Stop sell (stop-loss): price fell to the stop.
    [InlineData(OrderType.Stop, OrderSide.Sell, 90d, 90.01d, false)]
    [InlineData(OrderType.Stop, OrderSide.Sell, 90d, 90d, true)]
    [InlineData(OrderType.Stop, OrderSide.Sell, 90d, 85d, true)]
    // Stop buy: price broke up through the stop.
    [InlineData(OrderType.Stop, OrderSide.Buy, 110d, 109.99d, false)]
    [InlineData(OrderType.Stop, OrderSide.Buy, 110d, 110d, true)]
    public void IsTriggered_follows_the_rules_of_each_order_type(OrderType type, OrderSide side, double? orderPrice, double marketPrice, bool expected)
    {
        var order = new OrderPlaced(Guid.NewGuid(), "AAPL", side, type, 1, (decimal?)orderPrice, DateTimeOffset.UtcNow);

        MatchingEngine.IsTriggered(order, (decimal)marketPrice).Should().Be(expected);
    }

    [Fact]
    public void Pending_orders_are_kept_in_arrival_order_per_symbol()
    {
        var engine = new MatchingEngine();
        var first = new OrderPlaced(Guid.NewGuid(), "AAPL", OrderSide.Buy, OrderType.Limit, 1, 100m, DateTimeOffset.UtcNow);
        var second = first with { OrderId = Guid.NewGuid() };
        var other = first with { OrderId = Guid.NewGuid(), Symbol = "MSFT" };

        engine.AddPending(first);
        engine.AddPending(other);
        engine.AddPending(second);
        engine.RemovePending(first);

        engine.PendingOrders("AAPL").Should().Equal(second);
        engine.PendingOrders("MSFT").Should().Equal(other);
    }
}
