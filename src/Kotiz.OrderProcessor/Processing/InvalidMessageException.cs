namespace Kotiz.OrderProcessor.Processing;

/// <summary>
/// A permanent error (an invalid order or quote): retrying will never fix it,
/// so the message goes straight to the dead-letter topic.
/// </summary>
public sealed class InvalidMessageException(string message) : Exception(message);
