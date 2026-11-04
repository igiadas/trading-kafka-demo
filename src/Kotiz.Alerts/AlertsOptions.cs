namespace Kotiz.Alerts;

/// <summary>Bound from the "Alerts" section of appsettings.json (overridden by environment variables).</summary>
public sealed class AlertsOptions
{
    /// <summary>Optional. Without a token and chat id, alerts are only written to the log.</summary>
    public string? TelegramBotToken { get; set; }

    public string? TelegramChatId { get; set; }

    /// <summary>A price move of at least this percentage within <see cref="PriceMoveWindow"/> raises an alert.</summary>
    public decimal PriceMoveThresholdPercent { get; set; } = 2m;

    public TimeSpan PriceMoveWindow { get; set; } = TimeSpan.FromMinutes(5);

    public bool TelegramEnabled => !string.IsNullOrWhiteSpace(TelegramBotToken) && !string.IsNullOrWhiteSpace(TelegramChatId);
}
