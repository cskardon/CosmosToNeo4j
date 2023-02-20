namespace CosmosToNeo4j.Models;

public class Stats
{
    public long TotalNodes { get; set; }
    public long TotalRelationships { get; set; }

    public IDictionary<string, long> NodeLabelCounts { get; set; } = new Dictionary<string, long>();
    public IDictionary<string, long> RelationshipLabelCounts { get; set; } = new Dictionary<string, long>();

    public bool ContainsData()
    {
        return TotalNodes > 0 || TotalRelationships > 0;
    }


}