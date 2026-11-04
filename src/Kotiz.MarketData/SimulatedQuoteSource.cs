using Kotiz.Contracts;
using Microsoft.Extensions.Options;

namespace Kotiz.MarketData;

/// <summary>
/// Fake prices, so anyone can run Kotiz with "docker compose up" without signing up anywhere.
/// Each tick moves every price randomly by up to ±0.1%, and now and then by ±3% (a "sharp move",
/// so the alerts service has something to report).
/// </summary>
public sealed class SimulatedQuoteSource(IOptions<MarketDataOptions> options, ILogger<SimulatedQuoteSource> logger) : IQuoteSource
{
    private const double MaxStep = 0.001;
    private const double JumpProbability = 0.002;
    private const double JumpSize = 0.03;

    // Rough starting prices; any other symbol starts at 100.
    private static readonly Dictionary<string, decimal> StartingPrices = new(StringComparer.OrdinalIgnoreCase)
    {
        ["AAPL"] = 230m, ["MSFT"] = 420m, ["NVDA"] = 180m, ["TSLA"] = 250m, ["AMZN"] = 200m
    };

    public async Task RunAsync(Action<Quote> onQuote, CancellationToken cancellationToken)
    {
        var symbols = SymbolList.Parse(options.Value.Symbols);
        var prices = symbols.ToDictionary(s => s, s => StartingPrices.GetValueOrDefault(s, 100m));
        logger.LogInformation("Simulating prices for {Symbols}", string.Join(", ", symbols));

        using var timer = new PeriodicTimer(options.Value.PublishInterval);
        while (await timer.WaitForNextTickAsync(cancellationToken))
        {
            foreach (var symbol in symbols)
            {
                prices[symbol] = NextPrice(prices[symbol]);
                onQuote(new Quote(symbol, prices[symbol], DateTimeOffset.UtcNow));
            }
        }
    }

    private static decimal NextPrice(decimal price)
    {
        var change = Random.Shared.NextDouble() < JumpProbability
            ? (Random.Shared.Next(2) == 0 ? -JumpSize : JumpSize)
            : (Random.Shared.NextDouble() * 2 - 1) * MaxStep;
        return Math.Max(0.01m, Math.Round(price * (decimal)(1 + change), 2));
    }
}
