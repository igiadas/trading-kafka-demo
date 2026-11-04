using System.Globalization;
using Kotiz.Contracts;

namespace Kotiz.Alerts;

/// <summary>The text of each alert. Invariant culture: "219.00", whatever the machine's language.</summary>
public static class AlertMessages
{
    public static string ForExecution(OrderExecuted e)
    {
        var what = string.Create(CultureInfo.InvariantCulture, $"{(e.Side == OrderSide.Buy ? "bought" : "sold")} {e.Quantity} {e.Symbol} @ {e.Price:0.00}");
        return (e.Type, e.Side) switch
        {
            (OrderType.Stop, OrderSide.Sell) => $"Stop-loss hit: {what}",
            (OrderType.Limit, OrderSide.Sell) => $"Target reached: {what}",
            (OrderType.Stop, OrderSide.Buy) => $"Buy stop triggered: {what}",
            (OrderType.Limit, OrderSide.Buy) => $"Limit buy filled: {what}",
            _ => $"Order executed: {what}"
        };
    }

    public static string ForPriceMove(PriceMove move, TimeSpan window) => string.Create(CultureInfo.InvariantCulture,
        $"Sharp move: {move.Symbol} {move.ChangePercent:+0.00;-0.00}% within {window.TotalMinutes:0} min ({move.FromPrice:0.00} -> {move.ToPrice:0.00})");
}
