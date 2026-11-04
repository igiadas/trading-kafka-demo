using Kotiz.Contracts;

namespace Kotiz.OrderApi;

/// <summary>
/// Body of POST /orders. <see cref="OrderId"/> is optional: send the same id twice
/// to see the processor's idempotency (the second message is skipped as a duplicate).
/// </summary>
public sealed record PlaceOrderRequest(
    string Symbol,
    OrderSide Side,
    int Quantity,
    decimal Price,
    Guid? OrderId = null)
{
    /// <summary>
    /// Shape validation only. Whether the symbol is tradable is decided by the
    /// processor (it owns the reference data); that is how an order can reach the DLT.
    /// </summary>
    public Dictionary<string, string[]> Validate()
    {
        var errors = new Dictionary<string, string[]>();

        if (string.IsNullOrWhiteSpace(Symbol))
            errors[nameof(Symbol)] = ["Symbol is required."];
        if (!Enum.IsDefined(Side))
            errors[nameof(Side)] = ["Side must be Buy or Sell."];
        if (Quantity <= 0)
            errors[nameof(Quantity)] = ["Quantity must be greater than zero."];
        if (Price <= 0)
            errors[nameof(Price)] = ["Price must be greater than zero."];

        return errors;
    }
}
