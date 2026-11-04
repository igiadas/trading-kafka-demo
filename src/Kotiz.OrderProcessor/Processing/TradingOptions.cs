namespace Kotiz.OrderProcessor.Processing;

/// <summary>Reference data, bound from the "Trading" section of appsettings.json.</summary>
public sealed class TradingOptions
{
    public List<string> TradableSymbols { get; set; } = [];
}
