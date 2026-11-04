using Kotiz.Contracts;

namespace Kotiz.OrderApi;

/// <summary>
/// Body of POST /orders.
/// <see cref="Price"/> is the limit or stop price, and must be empty for market orders.
/// <see cref="OrderId"/> is optional: send the same id twice to see the processor's
/// idempotency (the second message is skipped as a duplicate).
/// </summary>
public sealed record PlaceOrderRequest(
    string Symbol,
    OrderSide Side,
    int Quantity,
    OrderType Type = OrderType.Market,
    decimal? Price = null,
    Guid? OrderId = null)
{
    /// <summary>
    /// Shape validation only. Whether the symbol is tradable is decided by the
    /// processor (it owns the symbol list); that is how an order can reach the DLT.
    /// </summary>
    public Dictionary<string, string[]> Validate()
    {
        var errors = new Dictionary<string, string[]>();

        if (string.IsNullOrWhiteSpace(Symbol))
            errors[nameof(Symbol)] = ["Symbol is required."];
        if (!Enum.IsDefined(Side))
            errors[nameof(Side)] = ["Side must be Buy or Sell."];
        if (!Enum.IsDefined(Type))
            errors[nameof(Type)] = ["Type must be Market, Limit or Stop."];
        if (Quantity <= 0)
            errors[nameof(Quantity)] = ["Quantity must be greater than zero."];

        if (Type == OrderType.Market && Price is not null)
            errors[nameof(Price)] = ["Market orders execute at the current price; leave Price empty."];
        else if (Type is OrderType.Limit or OrderType.Stop && Price is not > 0)
            errors[nameof(Price)] = [$"A {Type} order needs a Price greater than zero."];

        return errors;
    }
}
