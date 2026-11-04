using Kotiz.Contracts;

namespace Kotiz.Alerts;

public sealed record PriceMove(string Symbol, decimal FromPrice, decimal ToPrice, decimal ChangePercent);

/// <summary>
/// Detects sharp price moves: a change of at least the threshold against a reference price
/// taken at most one window ago. After an alert, or when the window expires, the current price
/// becomes the new reference. Simple on purpose: a move that straddles two windows can be missed.
/// Not thread-safe: called from a single consumer loop.
/// </summary>
public sealed class PriceMoveDetector(decimal thresholdPercent, TimeSpan window)
{
    private readonly Dictionary<string, (decimal Price, DateTimeOffset Timestamp)> _reference = new(StringComparer.OrdinalIgnoreCase);

    public PriceMove? Check(Quote quote)
    {
        if (!_reference.TryGetValue(quote.Symbol, out var reference) || quote.Timestamp - reference.Timestamp > window)
        {
            _reference[quote.Symbol] = (quote.Price, quote.Timestamp);
            return null;
        }

        var changePercent = (quote.Price - reference.Price) / reference.Price * 100;
        if (Math.Abs(changePercent) < thresholdPercent)
            return null;

        _reference[quote.Symbol] = (quote.Price, quote.Timestamp);
        return new PriceMove(quote.Symbol, reference.Price, quote.Price, Math.Round(changePercent, 2));
    }
}
