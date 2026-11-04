using System.Net;
using FluentAssertions;
using Kotiz.Alerts;
using Kotiz.Contracts;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;

namespace Kotiz.Tests;

public class PriceMoveDetectorTests
{
    private static readonly DateTimeOffset T0 = new(2026, 11, 4, 15, 30, 0, TimeSpan.Zero);
    private readonly PriceMoveDetector _detector = new(thresholdPercent: 2m, window: TimeSpan.FromMinutes(5));

    private PriceMove? At(int secondsAfterT0, decimal price, string symbol = "AAPL") =>
        _detector.Check(new Quote(symbol, price, T0.AddSeconds(secondsAfterT0)));

    [Fact]
    public void Small_moves_raise_nothing()
    {
        At(0, 100m).Should().BeNull();
        At(60, 101.9m).Should().BeNull();
        At(120, 98.1m).Should().BeNull();
    }

    [Fact]
    public void A_move_of_at_least_the_threshold_within_the_window_is_detected()
    {
        At(0, 100m);

        var move = At(120, 97.5m);

        move.Should().Be(new PriceMove("AAPL", 100m, 97.5m, -2.5m));
    }

    [Fact]
    public void After_an_alert_the_new_price_becomes_the_reference()
    {
        At(0, 100m);
        At(60, 103m).Should().NotBeNull();

        At(90, 104m).Should().BeNull("+0.97% from 103 is below the threshold");
    }

    [Fact]
    public void A_move_spread_over_more_than_the_window_is_not_sharp()
    {
        At(0, 100m);

        At(6 * 60, 103m).Should().BeNull();
    }

    [Fact]
    public void Each_symbol_has_its_own_reference()
    {
        At(0, 100m, "AAPL");
        At(10, 400m, "MSFT");

        At(20, 102.5m, "AAPL").Should().NotBeNull();
        At(30, 401m, "MSFT").Should().BeNull();
    }
}

public class AlertServiceTests
{
    private readonly Mock<INotifier> _notifier = new();
    private readonly AlertService _service;

    public AlertServiceTests()
    {
        var options = Options.Create(new AlertsOptions { PriceMoveThresholdPercent = 2m, PriceMoveWindow = TimeSpan.FromMinutes(5) });
        _service = new AlertService(_notifier.Object, options, NullLogger<AlertService>.Instance);
    }

    private static string Execution(OrderType type, OrderSide side) =>
        KafkaJson.Serialize(new OrderExecuted(Guid.NewGuid(), "AAPL", side, type, 10, 219m, DateTimeOffset.UtcNow));

    [Theory]
    [InlineData(OrderType.Stop, OrderSide.Sell, "Stop-loss hit: sold 10 AAPL @ 219.00")]
    [InlineData(OrderType.Limit, OrderSide.Sell, "Target reached: sold 10 AAPL @ 219.00")]
    [InlineData(OrderType.Market, OrderSide.Buy, "Order executed: bought 10 AAPL @ 219.00")]
    public async Task Every_execution_raises_an_alert_describing_it(OrderType type, OrderSide side, string expected)
    {
        await _service.HandleAsync(Topics.OrdersExecuted, Execution(type, side), CancellationToken.None);

        _notifier.Verify(n => n.SendAsync(expected, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task Sharp_price_move_raises_an_alert_and_normal_prices_do_not()
    {
        var t0 = DateTimeOffset.UtcNow;
        await _service.HandleAsync(Topics.MarketQuotes, KafkaJson.Serialize(new Quote("AAPL", 100m, t0)), CancellationToken.None);
        await _service.HandleAsync(Topics.MarketQuotes, KafkaJson.Serialize(new Quote("AAPL", 100.5m, t0.AddSeconds(1))), CancellationToken.None);
        _notifier.Verify(n => n.SendAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);

        await _service.HandleAsync(Topics.MarketQuotes, KafkaJson.Serialize(new Quote("AAPL", 103m, t0.AddSeconds(2))), CancellationToken.None);

        _notifier.Verify(n => n.SendAsync(It.Is<string>(s => s.StartsWith("Sharp move: AAPL +3.00%")), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task A_failing_notifier_does_not_stop_the_stream()
    {
        _notifier.Setup(n => n.SendAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new HttpRequestException("Telegram is down"));

        var act = () => _service.HandleAsync(Topics.OrdersExecuted, Execution(OrderType.Market, OrderSide.Buy), CancellationToken.None);

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task Unreadable_message_is_skipped()
    {
        var act = () => _service.HandleAsync(Topics.OrdersExecuted, "{ not json", CancellationToken.None);

        await act.Should().NotThrowAsync();
        _notifier.Verify(n => n.SendAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
    }
}

public class TelegramNotifierTests
{
    private sealed class RecordingHandler : HttpMessageHandler
    {
        public HttpRequestMessage? Request { get; private set; }
        public string? Body { get; private set; }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Request = request;
            Body = await request.Content!.ReadAsStringAsync(cancellationToken);
            return new HttpResponseMessage(HttpStatusCode.OK);
        }
    }

    [Fact]
    public async Task Posts_the_text_to_the_configured_chat()
    {
        var handler = new RecordingHandler();
        var options = Options.Create(new AlertsOptions { TelegramBotToken = "123:abc", TelegramChatId = "42" });
        var notifier = new TelegramNotifier(new HttpClient(handler), options, NullLogger<TelegramNotifier>.Instance);

        await notifier.SendAsync("Stop-loss hit", CancellationToken.None);

        handler.Request!.Method.Should().Be(HttpMethod.Post);
        handler.Request.RequestUri.Should().Be(new Uri("https://api.telegram.org/bot123:abc/sendMessage"));
        handler.Body.Should().Be("""{"chat_id":"42","text":"Stop-loss hit"}""");
    }
}
