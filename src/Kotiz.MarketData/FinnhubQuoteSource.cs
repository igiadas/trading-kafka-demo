using System.Net.WebSockets;
using System.Text;
using Kotiz.Contracts;
using Microsoft.Extensions.Options;

namespace Kotiz.MarketData;

/// <summary>
/// Real prices from Finnhub's free WebSocket: US stocks and crypto (e.g. BINANCE:BTCUSDT, which
/// trades 24/7, handy outside US market hours). One connection per API key. Reconnects with
/// exponential backoff whenever the connection drops.
/// </summary>
public sealed class FinnhubQuoteSource(IOptions<MarketDataOptions> options, ILogger<FinnhubQuoteSource> logger) : IQuoteSource
{
    private static readonly TimeSpan MaxReconnectDelay = TimeSpan.FromSeconds(30);

    public async Task RunAsync(Action<Quote> onQuote, CancellationToken cancellationToken)
    {
        var apiKey = options.Value.FinnhubApiKey;
        if (string.IsNullOrWhiteSpace(apiKey))
            throw new InvalidOperationException("Market:Source is Finnhub but no API key is set (FINNHUB_API_KEY).");

        var symbols = SymbolList.Parse(options.Value.Symbols);
        var reconnectDelay = TimeSpan.FromSeconds(1);

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                using var socket = new ClientWebSocket();
                await socket.ConnectAsync(new Uri($"wss://ws.finnhub.io?token={apiKey}"), cancellationToken);
                foreach (var symbol in symbols)
                    await SendAsync(socket, $$"""{"type":"subscribe","symbol":"{{symbol}}"}""", cancellationToken);

                logger.LogInformation("Connected to Finnhub, subscribed to {Symbols}", string.Join(", ", symbols));
                reconnectDelay = TimeSpan.FromSeconds(1);

                await ReceiveLoopAsync(socket, onQuote, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex) when (ex is WebSocketException or FinnhubErrorException or IOException)
            {
                // The API key is deliberately not logged: it is part of the URL, never of the message.
                logger.LogWarning("Finnhub connection lost ({Error}). Reconnecting in {Delay} s",
                    ex.Message, reconnectDelay.TotalSeconds);
            }

            await Task.Delay(reconnectDelay, cancellationToken);
            reconnectDelay = TimeSpan.FromTicks(Math.Min(reconnectDelay.Ticks * 2, MaxReconnectDelay.Ticks));
        }
    }

    private static async Task ReceiveLoopAsync(ClientWebSocket socket, Action<Quote> onQuote, CancellationToken cancellationToken)
    {
        var buffer = new byte[8 * 1024];
        using var message = new MemoryStream();

        while (socket.State == WebSocketState.Open)
        {
            var result = await socket.ReceiveAsync(buffer, cancellationToken);
            if (result.MessageType == WebSocketMessageType.Close)
                throw new WebSocketException($"Closed by Finnhub: {result.CloseStatusDescription}");

            // A message can arrive in several frames; put them together first.
            message.Write(buffer, 0, result.Count);
            if (!result.EndOfMessage)
                continue;

            foreach (var quote in FinnhubMessageParser.Parse(Encoding.UTF8.GetString(message.GetBuffer(), 0, (int)message.Length)))
                onQuote(quote);
            message.SetLength(0);
        }
    }

    private static Task SendAsync(ClientWebSocket socket, string text, CancellationToken cancellationToken) =>
        socket.SendAsync(Encoding.UTF8.GetBytes(text), WebSocketMessageType.Text, endOfMessage: true, cancellationToken);
}
