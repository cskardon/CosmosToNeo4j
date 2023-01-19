namespace CosmosToNeo4j.Models;

public class PropertyMap
{
    public string? Cosmos { get; set; }
    public string? Neo4j { get; set; }
    public bool Indexed { get; set; }
    public bool Ignored { get; set; }
}