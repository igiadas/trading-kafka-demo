namespace Kotiz.MarketData;

/// <summary>
/// Lets through at most one price per symbol per interval. A busy stock can trade many times a
/// second; nobody needs every tick for paper trading, and it keeps the topic and the consumers calm.
/// The prices dropped are not lost information: the next one published supersedes them.
/// Not thread-safe: called from a single source loop.
/// </summary>
public sealed class QuoteThrottle(TimeSpan interval)
{
    private readonly Dictionary<string, DateTimeOffset> _lastPublished = new(StringComparer.OrdinalIgnoreCase);

    public bool ShouldPublish(string symbol, DateTimeOffset timestamp)
    {
        if (_lastPublished.TryGetValue(symbol, out var last) && timestamp - last < interval)
            return false;

        _lastPublished[symbol] = timestamp;
        return true;
    }
}
