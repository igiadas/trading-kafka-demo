namespace Kotiz.Contracts;

/// <summary>
/// Event published by the order processor to the "orders.executed" topic
/// when an order is filled (paper trading: no real money, no real exchange).
/// </summary>
/// <param name="Price">The market price the order was filled at.</param>
public sealed record OrderExecuted(
    Guid OrderId,
    string Symbol,
    OrderSide Side,
    OrderType Type,
    int Quantity,
    decimal Price,
    DateTimeOffset ExecutedAt);
