namespace Kotiz.Contracts;

/// <summary>
/// The symbols Kotiz works with come from one comma-separated setting ("Market:Symbols",
/// or the SYMBOLS variable in docker compose), shared by every service.
/// </summary>
public static class SymbolList
{
    public static IReadOnlyList<string> Parse(string? csv) =>
        (csv ?? "")
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(s => s.ToUpperInvariant())
            .Distinct()
            .ToList();
}
