using Microsoft.Extensions.Options;
using Trading.Contracts;
using Trading.OrderProcessor.Kafka;

namespace Trading.OrderProcessor.Processing;

public enum ProcessingOutcome
{
    Executed,
    Duplicate
}

public interface IOrderProcessor
{
    Task<ProcessingOutcome> ProcessAsync(OrderPlaced order, CancellationToken cancellationToken);
}

/// <summary>Business logic for one order. Knows nothing about offsets, retries or the DLT.</summary>
public sealed class OrderProcessor : IOrderProcessor
{
    private readonly IProcessedOrderStore _processedOrders;
    private readonly PositionBook _positions;
    private readonly IExecutionPublisher _executions;
    private readonly ILogger<OrderProcessor> _logger;
    private readonly HashSet<string> _tradableSymbols;

    public OrderProcessor(
        IProcessedOrderStore processedOrders,
        PositionBook positions,
        IExecutionPublisher executions,
        IOptions<TradingOptions> tradingOptions,
        ILogger<OrderProcessor> logger)
    {
        _processedOrders = processedOrders;
        _positions = positions;
        _executions = executions;
        _logger = logger;
        _tradableSymbols = new HashSet<string>(tradingOptions.Value.TradableSymbols, StringComparer.OrdinalIgnoreCase);
    }

    public async Task<ProcessingOutcome> ProcessAsync(OrderPlaced order, CancellationToken cancellationToken)
    {
        // 1. Idempotency. With at-least-once delivery the same message can arrive twice
        //    (e.g. we crashed after processing it but before committing its offset).
        if (_processedOrders.HasBeenProcessed(order.OrderId))
        {
            _logger.LogWarning("Order {OrderId} already processed, skipping duplicate", order.OrderId);
            return ProcessingOutcome.Duplicate;
        }

        // 2. Never trust the producer: anything with access can write to the topic.
        Validate(order);

        // 3. The only step that can fail for transient reasons. Nothing has changed yet,
        //    so if it throws, retrying the whole method is safe.
        var executed = new OrderExecuted(order.OrderId, order.Symbol, order.Side, order.Quantity, order.Price, DateTimeOffset.UtcNow);
        await _executions.PublishAsync(executed, cancellationToken);

        // 4. Local state changes only after the fallible step succeeded.
        var netPosition = _positions.Apply(order);
        _processedOrders.MarkAsProcessed(order.OrderId);

        _logger.LogInformation("Executed {Side} {Quantity} {Symbol} @ {Price}. Net position: {NetPosition}",
            order.Side, order.Quantity, order.Symbol, order.Price, netPosition);
        return ProcessingOutcome.Executed;
    }

    private void Validate(OrderPlaced order)
    {
        if (string.IsNullOrWhiteSpace(order.Symbol) || !_tradableSymbols.Contains(order.Symbol))
            throw new InvalidOrderException($"Symbol '{order.Symbol}' is not tradable.");
        if (!Enum.IsDefined(order.Side))
            throw new InvalidOrderException($"Side '{order.Side}' is not valid.");
        if (order.Quantity <= 0)
            throw new InvalidOrderException($"Quantity {order.Quantity} must be greater than zero.");
        if (order.Price <= 0)
            throw new InvalidOrderException($"Price {order.Price} must be greater than zero.");
    }
}
