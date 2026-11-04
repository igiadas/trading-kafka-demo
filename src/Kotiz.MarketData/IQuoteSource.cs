using Kotiz.Contracts;

namespace Kotiz.MarketData;

/// <summary>Where prices come from: the simulator or Finnhub. Both feed the same publisher.</summary>
public interface IQuoteSource
{
    /// <summary>Runs until cancelled, calling <paramref name="onQuote"/> for every new price.</summary>
    Task RunAsync(Action<Quote> onQuote, CancellationToken cancellationToken);
}
