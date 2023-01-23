namespace CosmosToNeo4j.Models;

using Newtonsoft.Json;

public class Map
{
    public string? Cosmos { get; set; }
    public string? Neo4j { get; set; }
    [JsonProperty(DefaultValueHandling = DefaultValueHandling.Ignore)]
    public List<PropertyMap>? Properties { get; set; }
}