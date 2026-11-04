namespace Trading.Contracts;

/// <summary>
/// Event published by the API to the "orders.placed" topic.
/// The Kafka message key is <see cref="Symbol"/>, so all orders for one stock
/// land in the same partition and are consumed in the order they were sent.
/// </summary>
public sealed record OrderPlaced(
    Guid OrderId,
    string Symbol,
    OrderSide Side,
    int Quantity,
    decimal Price,
    DateTimeOffset PlacedAt);
