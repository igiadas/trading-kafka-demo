using Trading.Contracts;

namespace Trading.OrderProcessor.Processing;

/// <summary>
/// Net position (shares held) per symbol. Not thread-safe, and it does not need to be:
/// the consumer loop handles one message at a time, and because the message key is the
/// symbol, a given symbol is only ever handled by one consumer of the group.
/// </summary>
public sealed class PositionBook
{
    private readonly Dictionary<string, long> _netQuantity = new(StringComparer.OrdinalIgnoreCase);

    /// <returns>The net position for the symbol after applying the order.</returns>
    public long Apply(OrderPlaced order)
    {
        var delta = order.Side == OrderSide.Buy ? order.Quantity : -order.Quantity;
        var net = GetNetQuantity(order.Symbol) + delta;
        _netQuantity[order.Symbol] = net;
        return net;
    }

    public long GetNetQuantity(string symbol) => _netQuantity.GetValueOrDefault(symbol);
}
