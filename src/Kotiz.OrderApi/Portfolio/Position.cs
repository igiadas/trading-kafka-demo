using Kotiz.Contracts;

namespace Kotiz.OrderApi.Portfolio;

/// <summary>
/// One symbol's position, average price and realized P&amp;L. Supports short positions
/// (selling more than you hold), so the math works whatever order the executions arrive in.
/// </summary>
public sealed class Position(string symbol)
{
    public string Symbol { get; } = symbol;

    /// <summary>Shares held: positive = long, negative = short, 0 = flat.</summary>
    public int Quantity { get; private set; }

    /// <summary>Average price paid for the shares currently held (0 when flat).</summary>
    public decimal AveragePrice { get; private set; }

    /// <summary>Profit or loss already locked in by closing (part of) the position.</summary>
    public decimal RealizedPnl { get; private set; }

    public void Apply(OrderSide side, int quantity, decimal price)
    {
        var signed = side == OrderSide.Buy ? quantity : -quantity;

        if (Quantity == 0 || Math.Sign(Quantity) == Math.Sign(signed))
        {
            // Opening or adding to the position: the average price is the weighted average.
            var newQuantity = Quantity + signed;
            AveragePrice = (AveragePrice * Math.Abs(Quantity) + price * quantity) / Math.Abs(newQuantity);
            Quantity = newQuantity;
            return;
        }

        // Reducing, closing or flipping: the part that closes realizes its profit or loss.
        var closed = Math.Min(quantity, Math.Abs(Quantity));
        RealizedPnl += (price - AveragePrice) * closed * Math.Sign(Quantity);
        Quantity += signed;

        if (Quantity == 0)
            AveragePrice = 0;
        else if (Math.Sign(Quantity) == Math.Sign(signed))
            AveragePrice = price; // flipped from long to short (or back): the rest opens at this price
    }

    /// <summary>Profit or loss if the position were closed at <paramref name="marketPrice"/>.</summary>
    public decimal UnrealizedPnl(decimal marketPrice) => (marketPrice - AveragePrice) * Quantity;
}
