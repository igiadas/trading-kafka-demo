using Kotiz.Contracts;
using Kotiz.OrderProcessor.Kafka;
using Microsoft.Extensions.Options;

namespace Kotiz.OrderProcessor.Processing;

public enum OrderOutcome
{
    Executed,
    Pending,
    Duplicate
}

public interface IOrderProcessor
{
    Task<OrderOutcome> ProcessOrderAsync(OrderPlaced order, CancellationToken cancellationToken);

    /// <returns>How many pending orders the new price executed.</returns>
    Task<int> ProcessQuoteAsync(Quote quote, CancellationToken cancellationToken);
}

/// <summary>
/// Business logic: matches orders against market prices. Knows nothing about offsets, retries or the DLT.
/// Rule followed everywhere: first the step that can fail (publishing to Kafka), then the state change.
/// If publishing throws, nothing has changed and the whole message can be retried safely.
/// </summary>
public sealed class OrderProcessor : IOrderProcessor
{
    private readonly IProcessedOrderStore _processedOrders;
    private readonly MatchingEngine _engine;
    private readonly IExecutionPublisher _executions;
    private readonly ILogger<OrderProcessor> _logger;
    private readonly HashSet<string> _tradableSymbols;

    public OrderProcessor(
        IProcessedOrderStore processedOrders,
        MatchingEngine engine,
        IExecutionPublisher executions,
        IOptions<MarketOptions> marketOptions,
        ILogger<OrderProcessor> logger)
    {
        _processedOrders = processedOrders;
        _engine = engine;
        _executions = executions;
        _logger = logger;
        _tradableSymbols = new HashSet<string>(marketOptions.Value.SymbolList, StringComparer.OrdinalIgnoreCase);
    }

    public async Task<OrderOutcome> ProcessOrderAsync(OrderPlaced order, CancellationToken cancellationToken)
    {
        // 1. Idempotency. With at-least-once delivery the same message can arrive twice
        //    (e.g. we crashed after processing it but before committing its offset).
        if (_processedOrders.HasBeenProcessed(order.OrderId))
        {
            _logger.LogWarning("Order {OrderId} already processed, skipping duplicate", order.OrderId);
            return OrderOutcome.Duplicate;
        }

        // 2. Never trust the producer: anything with access can write to the topic.
        Validate(order);

        // 3. Executable right now? (a market order, or a limit/stop the current price already satisfies)
        if (_engine.LastPrice(order.Symbol) is { } price && MatchingEngine.IsTriggered(order, price))
        {
            await ExecuteAsync(order, price, cancellationToken);
            _processedOrders.MarkAsProcessed(order.OrderId);
            return OrderOutcome.Executed;
        }

        // 4. Otherwise it waits for a price that triggers it (see ProcessQuoteAsync).
        _engine.AddPending(order);
        _processedOrders.MarkAsProcessed(order.OrderId);
        _logger.LogInformation("Order {OrderId} pending: {Type} {Side} {Quantity} {Symbol} @ {Price}",
            order.OrderId, order.Type, order.Side, order.Quantity, order.Symbol, order.Price?.ToString() ?? "market");
        return OrderOutcome.Pending;
    }

    public async Task<int> ProcessQuoteAsync(Quote quote, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(quote.Symbol) || quote.Price <= 0)
            throw new InvalidMessageException($"Quote for '{quote.Symbol}' at {quote.Price} is not valid.");

        var executed = 0;
        foreach (var order in _engine.PendingOrders(quote.Symbol))
        {
            if (!MatchingEngine.IsTriggered(order, quote.Price))
                continue;

            // Publish, then remove. If a later publish fails, the orders already removed are not
            // executed again when the quote is retried; the retry only sees what is still pending.
            await ExecuteAsync(order, quote.Price, cancellationToken);
            _engine.RemovePending(order);
            executed++;
        }

        _engine.SetLastPrice(quote.Symbol, quote.Price);
        return executed;
    }

    private async Task ExecuteAsync(OrderPlaced order, decimal price, CancellationToken cancellationToken)
    {
        var execution = new OrderExecuted(order.OrderId, order.Symbol, order.Side, order.Type, order.Quantity, price, DateTimeOffset.UtcNow);
        await _executions.PublishAsync(execution, cancellationToken);

        _logger.LogInformation("Executed {Type} {Side} {Quantity} {Symbol} @ {Price} (order {OrderId})",
            order.Type, order.Side, order.Quantity, order.Symbol, price, order.OrderId);
    }

    private void Validate(OrderPlaced order)
    {
        if (string.IsNullOrWhiteSpace(order.Symbol) || !_tradableSymbols.Contains(order.Symbol))
            throw new InvalidMessageException($"Symbol '{order.Symbol}' is not tradable.");
        if (!Enum.IsDefined(order.Side))
            throw new InvalidMessageException($"Side '{order.Side}' is not valid.");
        if (!Enum.IsDefined(order.Type))
            throw new InvalidMessageException($"Order type '{order.Type}' is not valid.");
        if (order.Quantity <= 0)
            throw new InvalidMessageException($"Quantity {order.Quantity} must be greater than zero.");
        if (order.Type != OrderType.Market && order.Price is not > 0)
            throw new InvalidMessageException($"A {order.Type} order needs a price greater than zero.");
    }
}
