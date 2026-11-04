using System.Text.Json;
using Kotiz.Contracts;

namespace Kotiz.MarketData;

/// <summary>
/// Reads the messages of Finnhub's WebSocket (https://finnhub.io/docs/api/websocket-trades).
/// A trade message looks like:
/// {"type":"trade","data":[{"s":"AAPL","p":230.15,"t":1730000000000,"v":100}]}
/// Finnhub also sends {"type":"ping"} to keep the connection alive, and {"type":"error","msg":"..."}.
/// </summary>
public static class FinnhubMessageParser
{
    /// <returns>One quote per trade in the message; empty for pings and anything else.</returns>
    /// <exception cref="FinnhubErrorException">Finnhub reported an error (e.g. an invalid symbol or API key).</exception>
    public static IReadOnlyList<Quote> Parse(string json)
    {
        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;
        var type = root.TryGetProperty("type", out var t) ? t.GetString() : null;

        if (type == "error")
            throw new FinnhubErrorException(root.TryGetProperty("msg", out var msg) ? msg.GetString() ?? "" : json);
        if (type != "trade" || !root.TryGetProperty("data", out var data))
            return [];

        var quotes = new List<Quote>();
        foreach (var trade in data.EnumerateArray())
        {
            quotes.Add(new Quote(
                trade.GetProperty("s").GetString()!,
                trade.GetProperty("p").GetDecimal(),
                DateTimeOffset.FromUnixTimeMilliseconds(trade.GetProperty("t").GetInt64())));
        }
        return quotes;
    }
}

public sealed class FinnhubErrorException(string message) : Exception($"Finnhub error: {message}");
