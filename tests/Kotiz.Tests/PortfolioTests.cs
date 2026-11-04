using FluentAssertions;
using Kotiz.Contracts;
using Kotiz.OrderApi.Portfolio;

namespace Kotiz.Tests;

public class PositionTests
{
    [Fact]
    public void Buying_twice_averages_the_price()
    {
        var position = new Position("AAPL");

        position.Apply(OrderSide.Buy, 10, 100m);
        position.Apply(OrderSide.Buy, 10, 110m);

        position.Quantity.Should().Be(20);
        position.AveragePrice.Should().Be(105m);
        position.RealizedPnl.Should().Be(0m);
    }

    [Fact]
    public void Selling_part_realizes_profit_and_keeps_the_average()
    {
        var position = new Position("AAPL");
        position.Apply(OrderSide.Buy, 20, 105m);

        position.Apply(OrderSide.Sell, 5, 120m);

        position.Quantity.Should().Be(15);
        position.AveragePrice.Should().Be(105m);
        position.RealizedPnl.Should().Be(75m); // (120 - 105) x 5
    }

    [Fact]
    public void Unrealized_pnl_uses_the_market_price()
    {
        var position = new Position("AAPL");
        position.Apply(OrderSide.Buy, 15, 105m);

        position.UnrealizedPnl(100m).Should().Be(-75m);
    }

    [Fact]
    public void Closing_the_position_resets_the_average()
    {
        var position = new Position("AAPL");
        position.Apply(OrderSide.Buy, 10, 100m);

        position.Apply(OrderSide.Sell, 10, 90m);

        position.Quantity.Should().Be(0);
        position.AveragePrice.Should().Be(0m);
        position.RealizedPnl.Should().Be(-100m);
    }

    [Fact]
    public void Selling_more_than_held_flips_to_a_short_at_the_sell_price()
    {
        var position = new Position("AAPL");
        position.Apply(OrderSide.Buy, 10, 100m);

        position.Apply(OrderSide.Sell, 15, 110m);

        position.Quantity.Should().Be(-5);
        position.AveragePrice.Should().Be(110m);
        position.RealizedPnl.Should().Be(100m); // only the 10 shares held are closed
    }

    [Fact]
    public void A_short_position_earns_when_the_price_falls()
    {
        var position = new Position("AAPL");
        position.Apply(OrderSide.Sell, 5, 100m);

        position.UnrealizedPnl(90m).Should().Be(50m);
        position.Apply(OrderSide.Buy, 5, 90m);

        position.Quantity.Should().Be(0);
        position.RealizedPnl.Should().Be(50m);
    }
}

public class PortfolioStateTests
{
    private static readonly DateTimeOffset T0 = new(2026, 11, 4, 15, 30, 0, TimeSpan.Zero);

    private static OrderExecuted Execution(OrderSide side, int quantity, decimal price, DateTimeOffset at, string symbol = "AAPL") =>
        new(Guid.NewGuid(), symbol, side, OrderType.Market, quantity, price, at);

    [Fact]
    public void Portfolio_combines_positions_with_the_latest_prices()
    {
        var portfolio = new PortfolioState();
        portfolio.ApplyExecution(Execution(OrderSide.Buy, 10, 100m, T0));
        portfolio.ApplyExecution(Execution(OrderSide.Buy, 2, 400m, T0, "MSFT"));
        portfolio.UpdatePrice(new Quote("AAPL", 103m, T0.AddSeconds(1)));

        var view = portfolio.GetPortfolio();

        view.Positions.Should().HaveCount(2);
        var aapl = view.Positions.Single(p => p.Symbol == "AAPL");
        aapl.LastPrice.Should().Be(103m);
        aapl.MarketValue.Should().Be(1030m);
        aapl.UnrealizedPnl.Should().Be(30m);
        view.UnrealizedPnl.Should().Be(30m, "MSFT is still at its execution price");
        view.TotalPnl.Should().Be(30m);
    }

    [Fact]
    public void The_same_execution_read_twice_is_applied_once()
    {
        var portfolio = new PortfolioState();
        var execution = Execution(OrderSide.Buy, 10, 100m, T0);

        portfolio.ApplyExecution(execution).Should().BeTrue();
        portfolio.ApplyExecution(execution).Should().BeFalse();

        portfolio.GetPortfolio().Positions.Single().Quantity.Should().Be(10);
    }

    [Fact]
    public void An_older_price_never_replaces_a_newer_one()
    {
        var portfolio = new PortfolioState();
        portfolio.UpdatePrice(new Quote("AAPL", 105m, T0.AddSeconds(10)));

        portfolio.ApplyExecution(Execution(OrderSide.Buy, 10, 100m, T0));

        portfolio.GetPrices()["AAPL"].Price.Should().Be(105m);
    }
}
