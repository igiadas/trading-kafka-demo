namespace Kotiz.OrderProcessor.Processing;

/// <summary>
/// A permanent (business) error: retrying will never fix it,
/// so the message goes straight to the dead-letter topic.
/// </summary>
public sealed class InvalidOrderException(string message) : Exception(message);
