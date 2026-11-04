using System.Text.Json.Serialization;
using Confluent.Kafka;
using Kotiz.Contracts;
using Kotiz.OrderApi;

var builder = WebApplication.CreateBuilder(args);

builder.Services.ConfigureHttpJsonOptions(o =>
    o.SerializerOptions.Converters.Add(new JsonStringEnumConverter()));
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

// One producer for the whole app: IProducer is thread-safe and expensive to create.
builder.Services.AddSingleton<IProducer<string, string>>(sp =>
{
    var config = new ProducerConfig
    {
        BootstrapServers = builder.Configuration["Kafka:BootstrapServers"],
        // Wait until all in-sync replicas have the message before treating it as written.
        Acks = Acks.All,
        // The broker discards duplicates caused by the producer's own retries.
        EnableIdempotence = true,
        // Give up after 10 s instead of the 5-minute default, so the HTTP call fails fast.
        MessageTimeoutMs = 10_000
    };
    var logger = sp.GetRequiredService<ILogger<Program>>();
    return new ProducerBuilder<string, string>(config)
        .SetErrorHandler((_, e) => logger.LogError("Kafka producer error: {Reason}", e.Reason))
        .Build();
});
builder.Services.AddSingleton<IOrderPublisher, KafkaOrderPublisher>();

var app = builder.Build();

app.UseSwagger();
app.UseSwaggerUI();

app.MapPost("/orders", async (PlaceOrderRequest request, IOrderPublisher publisher,
    ILogger<Program> logger, CancellationToken ct) =>
{
    var errors = request.Validate();
    if (errors.Count > 0)
        return Results.ValidationProblem(errors);

    var order = new OrderPlaced(
        request.OrderId ?? Guid.NewGuid(),
        request.Symbol.Trim().ToUpperInvariant(),
        request.Side,
        request.Quantity,
        request.Price,
        DateTimeOffset.UtcNow);

    try
    {
        var written = await publisher.PublishAsync(order, ct);
        logger.LogInformation("Order {OrderId} ({Symbol}) written to {Topic} [partition {Partition}] @ offset {Offset}",
            order.OrderId, order.Symbol, written.Topic, written.Partition.Value, written.Offset.Value);

        // 202 Accepted: the order is safely in Kafka, but it has not been processed yet.
        return Results.Accepted(value: new
        {
            order.OrderId,
            Partition = written.Partition.Value,
            Offset = written.Offset.Value
        });
    }
    catch (ProduceException<string, string> ex)
    {
        logger.LogError(ex, "Could not publish order {OrderId}", order.OrderId);
        return Results.Problem("The order could not be written to Kafka.", statusCode: StatusCodes.Status503ServiceUnavailable);
    }
});

app.Run();
