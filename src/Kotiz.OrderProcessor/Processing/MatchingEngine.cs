using Kotiz.Contracts;

namespace Kotiz.OrderProcessor.Processing;

/// <summary>
/// Keeps the latest price of each symbol and the orders waiting for their price.
/// It only holds state and answers questions; <see cref="OrderProcessor"/> decides when to change it.
///
/// Not thread-safe, and it does not need to be: the consumer loop handles one message at a time.
/// Both topics it reads (orders.placed and market.quotes) are keyed by symbol and have the same
/// number of partitions, so the orders and the prices of one symbol reach the same consumer.
///
/// In memory: pending orders are lost if the service restarts (see the README).
/// </summary>
public sealed class MatchingEngine
{
    private readonly Dictionary<string, decimal> _lastPrices = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, List<OrderPlaced>> _pending = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>True when an order should be filled at this market price.</summary>
    public static bool IsTriggered(OrderPlaced order, decimal marketPrice) => (order.Type, order.Side) switch
    {
        (OrderType.Market, _) => true,
        (OrderType.Limit, OrderSide.Buy) => marketPrice <= order.Price,  // buy at the limit or cheaper
        (OrderType.Limit, OrderSide.Sell) => marketPrice >= order.Price, // sell at the limit or higher (take profit)
        (OrderType.Stop, OrderSide.Buy) => marketPrice >= order.Price,   // price broke up through the stop
        (OrderType.Stop, OrderSide.Sell) => marketPrice <= order.Price,  // price fell to the stop (stop-loss)
        _ => false
    };

    public decimal? LastPrice(string symbol) => _lastPrices.TryGetValue(symbol, out var price) ? price : null;

    public void SetLastPrice(string symbol, decimal price) => _lastPrices[symbol] = price;

    /// <summary>A copy, in arrival order, so the caller can remove orders while iterating.</summary>
    public IReadOnlyList<OrderPlaced> PendingOrders(string symbol) =>
        _pending.TryGetValue(symbol, out var orders) ? orders.ToList() : [];

    public void AddPending(OrderPlaced order)
    {
        if (!_pending.TryGetValue(order.Symbol, out var orders))
            _pending[order.Symbol] = orders = [];
        orders.Add(order);
    }

    public void RemovePending(OrderPlaced order)
    {
        if (_pending.TryGetValue(order.Symbol, out var orders))
            orders.RemoveAll(o => o.OrderId == order.OrderId);
    }
}
