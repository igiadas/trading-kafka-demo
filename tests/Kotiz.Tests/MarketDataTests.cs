using FluentAssertions;
using Kotiz.MarketData;

namespace Kotiz.Tests;

public class FinnhubMessageParserTests
{
    [Fact]
    public void Trade_message_becomes_one_quote_per_trade()
    {
        const string json = """
            {"type":"trade","data":[
              {"s":"AAPL","p":230.15,"t":1730000000000,"v":100},
              {"s":"BINANCE:BTCUSDT","p":68250.5,"t":1730000000500,"v":0.01}]}
            """;

        var quotes = FinnhubMessageParser.Parse(json);

        quotes.Should().HaveCount(2);
        quotes[0].Symbol.Should().Be("AAPL");
        quotes[0].Price.Should().Be(230.15m);
        quotes[0].Timestamp.Should().Be(DateTimeOffset.FromUnixTimeMilliseconds(1730000000000));
        quotes[1].Symbol.Should().Be("BINANCE:BTCUSDT");
    }

    [Fact]
    public void Ping_produces_no_quotes()
    {
        FinnhubMessageParser.Parse("""{"type":"ping"}""").Should().BeEmpty();
    }

    [Fact]
    public void Error_message_throws_so_the_source_reconnects()
    {
        var act = () => FinnhubMessageParser.Parse("""{"type":"error","msg":"Invalid symbol"}""");

        act.Should().Throw<FinnhubErrorException>().WithMessage("*Invalid symbol*");
    }
}

public class QuoteThrottleTests
{
    private static readonly DateTimeOffset T0 = new(2026, 11, 4, 15, 30, 0, TimeSpan.Zero);

    [Fact]
    public void Lets_through_at_most_one_price_per_symbol_per_interval()
    {
        var throttle = new QuoteThrottle(TimeSpan.FromSeconds(1));

        throttle.ShouldPublish("AAPL", T0).Should().BeTrue();
        throttle.ShouldPublish("AAPL", T0.AddMilliseconds(400)).Should().BeFalse();
        throttle.ShouldPublish("AAPL", T0.AddMilliseconds(999)).Should().BeFalse();
        throttle.ShouldPublish("AAPL", T0.AddSeconds(1)).Should().BeTrue();
    }

    [Fact]
    public void Symbols_are_throttled_independently()
    {
        var throttle = new QuoteThrottle(TimeSpan.FromSeconds(1));

        throttle.ShouldPublish("AAPL", T0).Should().BeTrue();
        throttle.ShouldPublish("MSFT", T0.AddMilliseconds(10)).Should().BeTrue();
    }
}
