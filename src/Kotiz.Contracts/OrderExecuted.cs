namespace Kotiz.Contracts;

/// <summary>
/// Event published by the order processor to the "orders.executed" topic
/// once an order has been (simulated) filled.
/// </summary>
public sealed record OrderExecuted(
    Guid OrderId,
    string Symbol,
    OrderSide Side,
    int Quantity,
    decimal Price,
    DateTimeOffset ExecutedAt);
