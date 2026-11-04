namespace Trading.OrderProcessor.Kafka;

/// <summary>Bound from the "Retry" section of appsettings.json.</summary>
public sealed class RetryOptions
{
    /// <summary>Total attempts, including the first one.</summary>
    public int MaxAttempts { get; set; } = 3;

    public TimeSpan BaseDelay { get; set; } = TimeSpan.FromMilliseconds(500);

    /// <summary>Exponential backoff: BaseDelay, 2 x BaseDelay, 4 x BaseDelay, ...</summary>
    public TimeSpan DelayBeforeRetry(int failedAttempt) => BaseDelay * Math.Pow(2, failedAttempt - 1);
}
