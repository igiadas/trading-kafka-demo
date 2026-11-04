using FluentAssertions;
using Trading.Contracts;
using Trading.OrderApi;

namespace Trading.Tests;

public class PlaceOrderRequestTests
{
    [Fact]
    public void Valid_request_has_no_errors()
    {
        new PlaceOrderRequest("SAN", OrderSide.Buy, 100, 4.25m).Validate().Should().BeEmpty();
    }

    [Theory]
    [InlineData("", OrderSide.Buy, 100, 4.25, "Symbol")]
    [InlineData("SAN", (OrderSide)0, 100, 4.25, "Side")]
    [InlineData("SAN", OrderSide.Sell, 0, 4.25, "Quantity")]
    [InlineData("SAN", OrderSide.Sell, 100, -1, "Price")]
    public void Invalid_field_is_reported(string symbol, OrderSide side, int quantity, decimal price, string expectedField)
    {
        var errors = new PlaceOrderRequest(symbol, side, quantity, price).Validate();

        errors.Should().ContainSingle().Which.Key.Should().Be(expectedField);
    }

    [Fact]
    public void Unknown_symbol_is_accepted_because_the_processor_owns_reference_data()
    {
        // Intentional: this is how an order can reach the dead-letter topic in the demo.
        new PlaceOrderRequest("XYZ", OrderSide.Buy, 1, 1m).Validate().Should().BeEmpty();
    }
}
