namespace CosmosToNeo4j;

using Gremlin.Net.Driver;
using Gremlin.Net.Structure.IO.GraphSON;
using JetBrains.Annotations;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;

public class CosmosDb
{
    private static class CosmosSettings
    {
        private const string Base = "CosmosDB:";

        public const string AccessKey = $"{Base}AccessKey";
        public const string Database = $"{Base}Database";
        public const string Container = $"{Base}Container";
        public const string Host = $"{Base}Host";
        public const string Port = $"{Base}Port";
        public const string EnableSsl = $"{Base}EnableSsl";
    }

    private readonly IGremlinClient _client;
    private readonly IPaginator _paginator;

    public CosmosDb(IConfiguration config, IPaginator paginator = null)
    {
        var accountKey = config.GetValue<string>(CosmosSettings.AccessKey);
        var database = config.GetValue<string>(CosmosSettings.Database);
        var container = config.GetValue<string>(CosmosSettings.Container);
        var host = config.GetValue<string>(CosmosSettings.Host);
        var port = config.GetValue<int>(CosmosSettings.Port);
        var enableSsl =config.GetValue<bool>(CosmosSettings.EnableSsl);

        var containerLink = $"/dbs/{database}/colls/{container}";

        _client = new GremlinClient(new GremlinServer(host, port, enableSsl, containerLink, accountKey), new GraphSON2MessageSerializer(new CustomGraphSON2Reader()));
        _paginator = paginator ?? new GuidIdPaginator();
    }
    

    public async Task<CosmosReadOutput<TNode, TRelationship>> Read<TNode, TRelationship>(Mappings mappings, Stats cosmosStats)
        where TNode : CosmosNode
        where TRelationship : CosmosRelationship
    {
        var output = new CosmosReadOutput<TNode, TRelationship>();

        //TODO Hard coded page size - auto generation of count / 36?
        var nodePageSize = (int)cosmosStats.TotalNodes / 36;

        var nodes = await ReadNodes<TNode>(mappings, nodePageSize, cosmosStats);
        output.Nodes = nodes;

        var relationships = await ReadEdges<TRelationship>(mappings, cosmosStats, (int)cosmosStats.TotalRelationships / 36);
        output.Relationships = relationships;

        return output;
    }

    public async Task<Stats> GetStats()
    {
        var stats = new Stats
        {
            TotalNodes = (await _client.SubmitAsync<int>("g.V().count()")).SingleOrDefault(),
            TotalRelationships = (await _client.SubmitAsync<int>("g.E().count()")).SingleOrDefault(),
            RelationshipLabelCounts = await GetCounts(false),
            NodeLabelCounts = await GetCounts(true)
        };

        return stats;
    }

    private async Task<IDictionary<string, long>> GetCounts(bool forNodes)
    {
        var resultSet = await _client.SubmitAsync<dynamic>($"g.{(forNodes ? "V" : "E")}().groupCount().by(label).unfold().project('Label','Count').by(keys).by(values)");
        return resultSet
            .Select(result => JsonConvert.DeserializeObject<LabelCounts>(JsonConvert.SerializeObject(result)))
            .ToDictionary<dynamic, string, long>(count => count.Label, count => count.Count);
    }

    /// <summary>
    /// This is used in the <see cref="GetCounts"/> method.
    /// </summary>
    [UsedImplicitly]
    private class LabelCounts
    {
        [UsedImplicitly] public string? Label { get; set; }
        [UsedImplicitly] public long Count { get; set; }
    }

    private async Task<IDictionary<string, List<T>>> Read<T>(bool isNode, IList<Map> mappings, Stats stats, int pageSize)
        where T : CosmosEntity
    {
        var output = new Dictionary<string, List<T>>();

        foreach (var mapping in mappings)
        {
            output.Add(mapping.Neo4j, new List<T>());

            var numPages = GetNumberOfPages(stats, mapping.Cosmos, pageSize, isNode);
            var gremlinQueries = _paginator.GeneratePagingPairs(numPages)
                .Select(x => isNode ? x.ToGremlinNode(mapping.Cosmos) : x.ToGremlinRelationship(mapping.Cosmos));

            foreach (var gremlinQuery in gremlinQueries)
            {
                var entities = await _client.SubmitAsync<dynamic>(gremlinQuery);
                foreach (var entity in entities)
                    output[mapping.Neo4j].Add(JsonConvert.DeserializeObject<T>(JsonConvert.SerializeObject(entity)));
            }
        }

        return output;
    }

    private static int GetNumberOfPages(Stats stats, string label, int pageSize, bool isNode)
    {
        var count = isNode ? stats.NodeLabelCounts[label] : stats.RelationshipLabelCounts[label];
        return (int)Math.Min(count / pageSize, 36);
    }

    private async Task<IDictionary<string, List<TRelationship>>> ReadEdges<TRelationship>(Mappings mappings, Stats cosmosStats, int pageSize)
        where TRelationship : CosmosRelationship
    {
        return await Read<TRelationship>(false, mappings.Relationships, cosmosStats, pageSize);
    }

    /// <summary>
    /// </summary>
    /// <param name="mappings"></param>
    /// <param name="pageSize">
    ///     This is going to be used as a rough guide - we can't actually page, so we're using this to work
    ///     out the 'number' of pages.
    /// </param>
    /// <param name="cosmosStats"></param>
    /// <returns>A cypher statement</returns>
    private async Task<IDictionary<string, List<TNode>>> ReadNodes<TNode>(Mappings mappings, int pageSize, Stats cosmosStats)
        where TNode : CosmosNode
    {
        return await Read<TNode>(true, mappings.Nodes, cosmosStats, pageSize);
    }


}