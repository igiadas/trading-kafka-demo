namespace Trading.OrderProcessor.Processing;

/// <summary>Remembers which OrderIds were already processed (the idempotency check).</summary>
public interface IProcessedOrderStore
{
    bool HasBeenProcessed(Guid orderId);
    void MarkAsProcessed(Guid orderId);
}

/// <summary>
/// In-memory on purpose, to keep the demo small. It protects against redeliveries while the
/// process is running, but it is lost on restart. In production this is a database table with a
/// unique key on OrderId, written in the same transaction as the position update.
/// </summary>
public sealed class InMemoryProcessedOrderStore : IProcessedOrderStore
{
    private readonly HashSet<Guid> _processed = [];

    public bool HasBeenProcessed(Guid orderId) => _processed.Contains(orderId);

    public void MarkAsProcessed(Guid orderId) => _processed.Add(orderId);
}
