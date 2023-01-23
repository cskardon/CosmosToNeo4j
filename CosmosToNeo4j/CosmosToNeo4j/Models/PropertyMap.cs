namespace CosmosToNeo4j.Models;

public class PropertyMap
{
    private string? _cosmos;

    public string? Cosmos
    {
        get => _cosmos;
        set
        {
            _cosmos = value;
            Neo4j = value;
        }
    }

    public string? Neo4j { get; set; }
    public bool Indexed { get; set; }
    public bool Ignored { get; set; }
}