namespace CosmosToNeo4j.Models;

using Newtonsoft.Json;

public class CosmosRelationship : CosmosEntity
{
    [JsonProperty("inVLabel")] public string? InVertexLabel { get; set; }

    [JsonProperty("outVLabel")] public string? OutVertexLabel { get; set; }

    [JsonProperty("inV")] public string? InVertexId { get; set; }

    [JsonProperty("outV")] public string? OutVertexId { get; set; }

    [JsonProperty("properties")] public IDictionary<string, object>? Properties { get; set; }
}