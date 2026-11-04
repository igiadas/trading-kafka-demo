namespace Kotiz.Contracts;

/// <summary>
/// Topic names, shared by producers and consumers. The topics are created up front
/// by the "kafka-init" container in docker-compose.yml (auto-creation is disabled).
/// </summary>
public static class Topics
{
    public const string OrdersPlaced = "orders.placed";
    public const string OrdersExecuted = "orders.executed";

    /// <summary>Compacted topic: Kafka keeps at least the latest price of every symbol.</summary>
    public const string MarketQuotes = "market.quotes";

    /// <summary>Dead-letter topic for messages from <paramref name="topic"/> that could not be processed.</summary>
    public static string DeadLetterFor(string topic) => topic + ".dlt";
}
