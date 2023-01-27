namespace CosmosToNeo4j;

using CosmosToNeo4j.Models;

public static class Extensions
{
    public static string? GetValue(this object value)
    {
        if (value == null)
            throw new ArgumentNullException(nameof(value));

        return value switch
        {
            int => value.ToString(),
            double => value.ToString(),
            _ => $"\"{value.ToString()?.Replace("\\\"", "\"").Replace("\"", "\\\"")}\""
        };
    }

    public static string? ToNeo4jMapping(this string value, IList<Map>? mappings)
    {
        if (string.IsNullOrWhiteSpace(value) || mappings == null) 
            return value;

        var map = mappings.SingleOrDefault(x => x.Cosmos == value);
        return map == null ? value : map.Neo4j;
    }
}
