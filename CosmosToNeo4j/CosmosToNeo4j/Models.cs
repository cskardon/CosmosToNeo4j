namespace CosmosToNeo4j;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

public class Vertex
{
    [JsonIgnore] public object? Id { get; set; } = Guid.NewGuid().ToString();

    [JsonIgnore] public string? Label { get; set; }

    [JsonIgnore] public string PartitionKey { get; set; } = "PartitionKey";
}

public class Edge
{
    [JsonIgnore] public object? Id { get; set; } = Guid.NewGuid().ToString();

    [JsonIgnore] public string? Label { get; set; }
}

public class RelationshipAll : Edge
{
    // [JsonProperty("roles")] public string[]? Roles { get; set; }

    // [JsonProperty("summary")] public string? Summary { get; set; }
    //
    // [JsonProperty("rating")] public int? Rating { get; set; }
}

public class NodeAll : Vertex
{
    [JsonProperty("title")] public string? Title { get; set; }

    [JsonProperty("tagline")] public string? TagLine { get; set; }

    [JsonProperty("released")] public int? Released { get; set; }

    [JsonProperty("born")] public int? Born { get; set; }

    [JsonProperty("name")] public string? Name { get; set; }
}

#region For Cosmos

public class CosmosResponse : CosmosNode
{
}

public abstract class CosmosEntity
{
    [JsonProperty("id")] public string? Id { get; set; }
    [JsonProperty("label")] public string? Label { get; set; }
    [JsonProperty("type")] public string? Type { get; set; }
}

public class CosmosRelationship : CosmosEntity
{
    [JsonProperty("inVLabel")] public string? InVertexLabel { get; set; }

    [JsonProperty("outVLabel")] public string? OutVertexLabel { get; set; }

    [JsonProperty("inV")] public string? InVertexId { get; set; }

    [JsonProperty("outV")] public string? OutVertexId { get; set; }

    [JsonProperty("properties")] public IDictionary<string, object>? Properties { get; set; }
}

public class CosmosNode : CosmosEntity
{
    private JObject? _properties;
    public JObject? Properties
    {
        get => _properties;
        set
        {
            _properties = value;
            PropertiesAsDictionary = PropertiesToDictionary();
        }
    }

    public IDictionary<string, object>? PropertiesAsDictionary { get; private set; }

    private IDictionary<string, object> PropertiesToDictionary()
    {
        if (Properties == null)
            return new Dictionary<string, object>();

        var output = new Dictionary<string, object>();
        foreach (var element in Properties)
        {
            var prop = element.Value?.ToObject<List<CosmosProperty>>().First();
            output.Add(element.Key, prop.Value);
        }

        return output;
    }

    public class CosmosProperty
    {
        [JsonProperty("id")] public string? Id { get; set; }

        [JsonProperty("value")] public object? Value { get; set; }
    }
}

#endregion For Cosmos