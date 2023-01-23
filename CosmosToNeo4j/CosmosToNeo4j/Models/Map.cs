namespace CosmosToNeo4j.Models;

public class Map
{
    public string? Cosmos { get; set; }
    public string? Neo4j { get; set; }
    public List<PropertyMap>? Properties { get; set; }
}