namespace CosmosToNeo4j;

using CosmosToNeo4j.Models;
using Newtonsoft.Json;

public class FileHandler
{
    public static Mappings GenerateMappings(Stats stats)
    {
        var mappings = new Mappings
        {
            Nodes = GenerateMappings(stats.NodeLabelCounts.Keys),
            Relationships = GenerateMappings(stats.RelationshipLabelCounts.Keys)
        };
        return mappings;
    }

    private static List<Map> GenerateMappings(IEnumerable<string> keys)
    {
        return keys.Select(k => new Map { Cosmos = k, Neo4j = k }).ToList();
    }

    public static async Task WriteMappingsFile(string path, Mappings mappings)
    {
        await File.WriteAllTextAsync(path, JsonConvert.SerializeObject(mappings));
    }
}