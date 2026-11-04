using Kotiz.Contracts;

namespace Kotiz.OrderApi.Portfolio;

public sealed record PositionView(
    string Symbol,
    int Quantity,
    decimal AveragePrice,
    decimal? LastPrice,
    decimal? MarketValue,
    decimal? UnrealizedPnl,
    decimal RealizedPnl);

public sealed record PortfolioView(
    IReadOnlyList<PositionView> Positions,
    decimal UnrealizedPnl,
    decimal RealizedPnl,
    decimal TotalPnl);

public sealed record PriceView(decimal Price, DateTimeOffset Timestamp);

/// <summary>
/// The portfolio read model: built only from events (executions and prices), never written to directly.
/// Written by the projection's consumer thread and read by HTTP requests, hence the lock.
/// </summary>
public sealed class PortfolioState
{
    private readonly object _lock = new();
    private readonly Dictionary<string, Position> _positions = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, PriceView> _prices = new(StringComparer.OrdinalIgnoreCase);

    // orders.executed is delivered at least once, so the same execution can be read twice.
    private readonly HashSet<Guid> _appliedOrderIds = [];

    /// <returns>False if this execution was already applied (a duplicate).</returns>
    public bool ApplyExecution(OrderExecuted execution)
    {
        lock (_lock)
        {
            if (!_appliedOrderIds.Add(execution.OrderId))
                return false;

            if (!_positions.TryGetValue(execution.Symbol, out var position))
                _positions[execution.Symbol] = position = new Position(execution.Symbol);
            position.Apply(execution.Side, execution.Quantity, execution.Price);

            // An execution is also a market price observation.
            UpdatePriceLocked(execution.Symbol, execution.Price, execution.ExecutedAt);
            return true;
        }
    }

    public void UpdatePrice(Quote quote)
    {
        lock (_lock)
            UpdatePriceLocked(quote.Symbol, quote.Price, quote.Timestamp);
    }

    public PortfolioView GetPortfolio()
    {
        lock (_lock)
        {
            var positions = _positions.Values
                .OrderBy(p => p.Symbol)
                .Select(p =>
                {
                    decimal? lastPrice = _prices.TryGetValue(p.Symbol, out var price) ? price.Price : null;
                    return new PositionView(
                        p.Symbol,
                        p.Quantity,
                        Math.Round(p.AveragePrice, 4),
                        lastPrice,
                        lastPrice * p.Quantity,
                        lastPrice is { } last ? Math.Round(p.UnrealizedPnl(last), 2) : null,
                        Math.Round(p.RealizedPnl, 2));
                })
                .ToList();

            var unrealized = positions.Sum(p => p.UnrealizedPnl ?? 0);
            var realized = positions.Sum(p => p.RealizedPnl);
            return new PortfolioView(positions, unrealized, realized, unrealized + realized);
        }
    }

    public IReadOnlyDictionary<string, PriceView> GetPrices()
    {
        lock (_lock)
            return new SortedDictionary<string, PriceView>(_prices);
    }

    private void UpdatePriceLocked(string symbol, decimal price, DateTimeOffset timestamp)
    {
        // The two topics are read in parallel, so an older price can arrive after a newer one.
        if (_prices.TryGetValue(symbol, out var current) && current.Timestamp > timestamp)
            return;
        _prices[symbol] = new PriceView(price, timestamp);
    }
}
