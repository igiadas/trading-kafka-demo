namespace Kotiz.Contracts;

/// <summary>
/// Latest traded price of a symbol, published to the "market.quotes" topic with the symbol as key.
/// It is the last trade price, not a bid/ask quote: that is what the free Finnhub feed provides.
/// </summary>
public sealed record Quote(string Symbol, decimal Price, DateTimeOffset Timestamp);
