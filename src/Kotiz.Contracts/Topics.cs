namespace Kotiz.Contracts;

/// <summary>
/// Topic names, shared by producer and consumer. The topics are created up front
/// by the "kafka-init" container in docker-compose.yml (auto-creation is disabled).
/// </summary>
public static class Topics
{
    public const string OrdersPlaced = "orders.placed";
    public const string OrdersExecuted = "orders.executed";

    /// <summary>Dead-letter topic: orders the processor could not handle.</summary>
    public const string OrdersPlacedDeadLetter = "orders.placed.dlt";
}
