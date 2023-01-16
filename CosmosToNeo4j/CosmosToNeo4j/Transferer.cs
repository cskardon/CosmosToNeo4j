namespace CosmosToNeo4j;

public static class Transferer
{
    public static bool AreCosmosMappingsOk(Mappings mappings, Stats stats, out ICollection<string> missingNodeLabels, out ICollection<string> missingRelationshipTypes)
    {
        missingNodeLabels = CheckForMissingValues(mappings.Nodes.Select(x => x.Cosmos).ToList(), stats.NodeLabelCounts.Keys.ToList());
        missingRelationshipTypes = CheckForMissingValues(mappings.Relationships.Select(x => x.Cosmos).ToList(), stats.RelationshipLabelCounts.Keys.ToList());
        return !(missingNodeLabels.Any() || missingRelationshipTypes.Any());
    }

    private static ICollection<string> CheckForMissingValues(IReadOnlyCollection<string> actual, List<string> toCheck)
    {
        return toCheck.Where(value => actual.All(x => x != value)).ToList();
    }

    public static Stats CompareCounts(Stats cosmos, Stats neo4j)
    {
        var output = new Stats
        {
            TotalNodes = cosmos.TotalNodes - neo4j.TotalNodes,
            TotalRelationships = cosmos.TotalRelationships - neo4j.TotalRelationships
        };

        return output;
    }
}