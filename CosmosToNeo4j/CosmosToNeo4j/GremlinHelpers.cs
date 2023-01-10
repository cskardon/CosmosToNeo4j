namespace CosmosToNeo4j;

using System.Reflection;
using Gremlin.Net.Driver;
using Newtonsoft.Json;

public static class GremlinHelpers
{
    public static string Tidy(this string labels)
    {
        labels = labels.Replace($"{Environment.NewLine}", "").Trim();
        labels = labels.Replace("[", "").Replace("]", "").Trim();
        labels = labels.Replace("\"", "").Trim();
        return labels;
    }

    public static string ToPropertiesString<T>(this T instance, bool isEdge)
    {
        var type = typeof(T);
        var properties = type.GetProperties();

        var converted = properties
            .Select(propertyInfo => instance.ToPropertyString(propertyInfo, isEdge))
            .ToList();

        return string.Join("", converted);
    }

    private static string? ToPropertyString<T>(this T instance, PropertyInfo propertyInfo, bool isEdge)
    {
        const string propertyFormat = ".property(\"{0}\",{1})";

        var value = propertyInfo.GetMethod?.Invoke(instance, null);
        return value == null
            ? null
            : string.Format(propertyFormat, propertyInfo.Name, GetPropertyValue(value));
    }

    public static T? GetSingleResult<T>(this ResultSet<dynamic> resultSet)
    {
        var result = resultSet.SingleOrDefault();
        if (result == null)
            throw new NullReferenceException("No results were returned to parse.");

        var json = JsonConvert.SerializeObject(result);
        return JsonConvert.DeserializeObject<T>(json);
    }

    private static string? GetPropertyValue(object? obj)
    {
        switch (obj)
        {
            case null:
                return null;
            case int:
            case double:
                return obj.ToString();
            case string s:
                return $"\"{s.Replace("\"", "\\\"")}\"";
            default:
                return $"\"{obj}\"";
        }
    }

    public static async Task CleanDb(this IGremlinClient client)
    {
        await client.SubmitAsync("g.V().drop()");
        await client.SubmitAsync("g.E().drop()");
    }

    public static async Task InsertEdge(this IGremlinClient client, string traversalName, string type, string start, string end, RelationshipAll relationship)
    {
        var query = $"{traversalName}.V(\"{start}\").addE(\"{type}\"){relationship.ToPropertiesString(true)}.to({traversalName}.V(\"{end}\"))";
        Console.WriteLine($"Executing: {query}");
        await client.SubmitAsync(query);
    }

    public static async Task<string> InsertVertex<T>(this IGremlinClient client, string traversalName, int identifier, T node, string label)
    {
        var query = $"{traversalName}.addV('{label}').property(\"Identifier\",\"{identifier}\"){node.ToPropertiesString(false)}";
        Console.WriteLine($"Executing: {query}");
        var resultSet = await client.SubmitAsync<dynamic>(query);
        return GetId(resultSet);
    }

    private static string? GetId(ResultSet<dynamic> resultSet)
    {
        return (
                from result in resultSet
                where result != null
                where !string.IsNullOrWhiteSpace(result["id"])
                select result["id"])
            .FirstOrDefault();
    }
}