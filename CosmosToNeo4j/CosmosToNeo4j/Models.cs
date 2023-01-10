namespace CosmosToNeo4j;

using Newtonsoft.Json;

public class Vertex
{
    [JsonIgnore]
    public object? Id { get; set; }  = Guid.NewGuid().ToString();
    
    [JsonIgnore]
    public string? Label { get; set; }

    [JsonIgnore]
    public string PartitionKey { get; set; } = "PartitionKey";
}

public class Edge
{
    [JsonIgnore]
    public object? Id { get; set; } = Guid.NewGuid().ToString();
    [JsonIgnore]
    public string? Label { get; set; }
}

public class RelationshipAll : Edge
{
    // [JsonProperty("roles")] public string[]? Roles { get; set; }
    
    // [JsonProperty("summary")] public string? Summary { get; set; }
    //
    // [JsonProperty("rating")] public int? Rating { get; set; }
}

public class NodeAll: Vertex
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
    [JsonProperty("id")] public Guid Id { get; set; }
    [JsonProperty("label")] public string Label { get; set; }
    [JsonProperty("type")] public string Type { get; set; }
}

public class CosmosRelationship : CosmosEntity
{
    [JsonProperty("inVLabel")] public string InVertexLabel { get; set; }

    [JsonProperty("outVLabel")] public string OutVertexLabel { get; set; }

    [JsonProperty("inV")] public Guid InVertexId { get; set; }

    [JsonProperty("outV")] public Guid OutVertexId { get; set; }

    [JsonProperty("properties")] public IDictionary<string, object> Properties { get; set; }
}

public class CosmosNode : CosmosEntity
{
    [JsonProperty("properties")] public IDictionary<string, IList<KeyValuePair<string, object>>> Properties { get; set; }
}
#endregion For Cosmos