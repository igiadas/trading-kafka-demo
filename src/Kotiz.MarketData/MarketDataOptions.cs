namespace Kotiz.MarketData;

/// <summary>Bound from the "Market" section of appsettings.json (overridden by environment variables in docker compose).</summary>
public sealed class MarketDataOptions
{
    /// <summary>"Simulated" (default, no account needed) or "Finnhub" (real prices, needs an API key).</summary>
    public string Source { get; set; } = "Simulated";

    /// <summary>Comma-separated list of symbols, e.g. "AAPL,MSFT,BINANCE:BTCUSDT".</summary>
    public string Symbols { get; set; } = "";

    /// <summary>Only needed when Source is "Finnhub". Never commit it: use .env or user secrets.</summary>
    public string? FinnhubApiKey { get; set; }

    /// <summary>At most one price per symbol is published per interval (see <see cref="QuoteThrottle"/>).</summary>
    public TimeSpan PublishInterval { get; set; } = TimeSpan.FromSeconds(1);
}
