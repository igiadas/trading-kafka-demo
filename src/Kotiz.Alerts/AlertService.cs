using System.Text.Json;
using Kotiz.Contracts;
using Microsoft.Extensions.Options;

namespace Kotiz.Alerts;

/// <summary>Turns one Kafka message into zero or one alert.</summary>
public sealed class AlertService
{
    private readonly INotifier _notifier;
    private readonly PriceMoveDetector _priceMoves;
    private readonly TimeSpan _window;
    private readonly ILogger<AlertService> _logger;

    public AlertService(INotifier notifier, IOptions<AlertsOptions> options, ILogger<AlertService> logger)
    {
        _notifier = notifier;
        _window = options.Value.PriceMoveWindow;
        _priceMoves = new PriceMoveDetector(options.Value.PriceMoveThresholdPercent, _window);
        _logger = logger;
    }

    public async Task HandleAsync(string topic, string? value, CancellationToken cancellationToken)
    {
        string? alert;
        try
        {
            alert = topic switch
            {
                Topics.OrdersExecuted => AlertMessages.ForExecution(KafkaJson.Deserialize<OrderExecuted>(value)),
                Topics.MarketQuotes => _priceMoves.Check(KafkaJson.Deserialize<Quote>(value)) is { } move
                    ? AlertMessages.ForPriceMove(move, _window)
                    : null,
                _ => null
            };
        }
        catch (JsonException ex)
        {
            _logger.LogError("Skipping unreadable message from {Topic}: {Error}", topic, ex.Message);
            return;
        }

        if (alert is null)
            return;

        try
        {
            await _notifier.SendAsync(alert, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Best effort: a failed notification is reported, but never stops the stream.
            _logger.LogWarning("Could not send alert '{Alert}': {Error}", alert, ex.Message);
        }
    }
}
