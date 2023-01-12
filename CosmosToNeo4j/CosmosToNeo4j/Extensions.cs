namespace CosmosToNeo4j;

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
}
