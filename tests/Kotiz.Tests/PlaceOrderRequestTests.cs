using System.Text.Json;
using FluentAssertions;
using Kotiz.Contracts;
using Kotiz.OrderApi;

namespace Kotiz.Tests;

public class PlaceOrderRequestTests
{
    [Theory]
    [InlineData(OrderType.Market, null)]
    [InlineData(OrderType.Limit, 225.5)]
    [InlineData(OrderType.Stop, 210d)]
    public void Valid_request_has_no_errors(OrderType type, double? price)
    {
        new PlaceOrderRequest("AAPL", OrderSide.Buy, 10, type, (decimal?)price).Validate().Should().BeEmpty();
    }

    [Theory]
    [InlineData("", OrderSide.Buy, 10, OrderType.Market, null, "Symbol")]
    [InlineData("AAPL", (OrderSide)0, 10, OrderType.Market, null, "Side")]
    [InlineData("AAPL", OrderSide.Sell, 0, OrderType.Market, null, "Quantity")]
    [InlineData("AAPL", OrderSide.Sell, 10, (OrderType)0, null, "Type")]
    [InlineData("AAPL", OrderSide.Sell, 10, OrderType.Market, 230d, "Price")]
    [InlineData("AAPL", OrderSide.Sell, 10, OrderType.Limit, null, "Price")]
    [InlineData("AAPL", OrderSide.Sell, 10, OrderType.Stop, -1d, "Price")]
    public void Invalid_field_is_reported(string symbol, OrderSide side, int quantity, OrderType type, double? price, string expectedField)
    {
        var errors = new PlaceOrderRequest(symbol, side, quantity, type, (decimal?)price).Validate();

        errors.Should().ContainSingle().Which.Key.Should().Be(expectedField);
    }

    [Fact]
    public void Type_defaults_to_Market_when_the_json_omits_it()
    {
        var options = new JsonSerializerOptions(JsonSerializerDefaults.Web) { Converters = { new System.Text.Json.Serialization.JsonStringEnumConverter() } };

        var request = JsonSerializer.Deserialize<PlaceOrderRequest>("""{"symbol":"AAPL","side":"Buy","quantity":10}""", options)!;

        request.Type.Should().Be(OrderType.Market);
        request.Validate().Should().BeEmpty();
    }

    [Fact]
    public void Unknown_symbol_is_accepted_because_the_processor_owns_the_symbol_list()
    {
        // Intentional: this is how an order can reach the dead-letter topic.
        new PlaceOrderRequest("XYZ", OrderSide.Buy, 1).Validate().Should().BeEmpty();
    }
}
