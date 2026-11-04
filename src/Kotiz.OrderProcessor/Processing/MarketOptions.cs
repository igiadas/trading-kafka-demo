using Kotiz.Contracts;

namespace Kotiz.OrderProcessor.Processing;

/// <summary>Bound from the "Market" section of appsettings.json (SYMBOLS in docker compose).</summary>
public sealed class MarketOptions
{
    /// <summary>Comma-separated list of tradable symbols, e.g. "AAPL,MSFT".</summary>
    public string Symbols { get; set; } = "";

    public IReadOnlyList<string> SymbolList => Contracts.SymbolList.Parse(Symbols);
}
