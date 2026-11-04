namespace Kotiz.Contracts;

// Starts at 1 so a missing value (default 0) is never a valid type.
public enum OrderType
{
    /// <summary>Execute now, at the latest market price.</summary>
    Market = 1,

    /// <summary>Execute when the price is at the limit or better (buy: price &lt;= limit, sell: price &gt;= limit).</summary>
    Limit = 2,

    /// <summary>Execute when the price crosses the stop (buy: price &gt;= stop, sell: price &lt;= stop). Used as a stop-loss.</summary>
    Stop = 3
}
