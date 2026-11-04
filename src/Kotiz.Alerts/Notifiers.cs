using System.Net.Http.Json;
using Microsoft.Extensions.Options;

namespace Kotiz.Alerts;

public interface INotifier
{
    Task SendAsync(string text, CancellationToken cancellationToken);
}

/// <summary>Used when Telegram is not configured: the alert only goes to the log.</summary>
public sealed class LogNotifier(ILogger<LogNotifier> logger) : INotifier
{
    public Task SendAsync(string text, CancellationToken cancellationToken)
    {
        logger.LogInformation("ALERT: {Text}", text);
        return Task.CompletedTask;
    }
}

/// <summary>
/// Sends the alert to a Telegram chat through the Bot API (https://core.telegram.org/bots/api#sendmessage).
/// The bot token is part of the URL, so this class never logs the URL.
/// </summary>
public sealed class TelegramNotifier(HttpClient http, IOptions<AlertsOptions> options, ILogger<TelegramNotifier> logger) : INotifier
{
    public async Task SendAsync(string text, CancellationToken cancellationToken)
    {
        var url = $"https://api.telegram.org/bot{options.Value.TelegramBotToken}/sendMessage";
        using var response = await http.PostAsJsonAsync(url, new { chat_id = options.Value.TelegramChatId, text }, cancellationToken);

        if (!response.IsSuccessStatusCode)
            throw new HttpRequestException($"Telegram returned {(int)response.StatusCode} {response.ReasonPhrase}.");

        logger.LogInformation("ALERT sent to Telegram: {Text}", text);
    }
}
