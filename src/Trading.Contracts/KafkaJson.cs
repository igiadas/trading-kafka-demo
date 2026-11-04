using System.Text.Json;
using System.Text.Json.Serialization;

namespace Trading.Contracts;

/// <summary>
/// Message values are plain JSON strings so they are easy to read in kafka-ui.
/// (In production a schema registry with Avro or Protobuf is the usual choice.)
/// </summary>
public static class KafkaJson
{
    public static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter() }
    };

    public static string Serialize<T>(T value) => JsonSerializer.Serialize(value, Options);

    /// <exception cref="JsonException">The value is not valid JSON for <typeparamref name="T"/>.</exception>
    public static T Deserialize<T>(string? json) =>
        json is null ? throw new JsonException("Message value is null.") :
        JsonSerializer.Deserialize<T>(json, Options)
        ?? throw new JsonException($"Message value deserialized to null ({typeof(T).Name}).");
}
