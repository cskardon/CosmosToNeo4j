namespace CosmosToNeo4j;

using CosmosToNeo4j.Models;
using global::Neo4j.Driver;
using Microsoft.Extensions.Configuration;
using Neo4jClient;
using Neo4jClient.Cypher;

public class Neo4j
{
    private readonly IGraphClient _client;

    public Neo4j(IConfiguration config)
    {
        var uri = $"{config.GetValue<string>(Neo4jSettings.Host)}:{config.GetValue<int>(Neo4jSettings.Port)}";
        var user = config.GetValue<string>(Neo4jSettings.Username)!;
        var pass = config.GetValue<string>(Neo4jSettings.Password)!;
        var database = config.GetValue<string>(Neo4jSettings.Database)!;

        _client = new BoltGraphClient(uri, user, pass);
        Database = string.IsNullOrWhiteSpace(database) ? "neo4j" : database;

        Uri = uri;
        var connectTask = _client.ConnectAsync();
        connectTask.Wait();
    }

    internal string Database { get; }
    internal string Uri { get; }

    private IDriver Driver => ((BoltGraphClient)_client).Driver;

    private ICypherFluentQuery StartQuery => _client.Cypher.WithDatabase(Database);

    public async Task<Stats> GetStats()
    {
        var stats = new Stats
        {
            TotalNodes = (await StartQuery.Match("(n)").Return(n => n.Count()).ResultsAsync).Single(),
            TotalRelationships = (await StartQuery.Match("()-[r]->()").Return(r => r.Count()).ResultsAsync).Single()
        };

        var nodeLabels = await
            StartQuery
                .Match("(n)")
                .Return(n => new { Labels = Return.As<IEnumerable<string>>("DISTINCT labels(n)"), Count = n.Count() })
                .ResultsAsync;

        stats.NodeLabelCounts = nodeLabels.ToDictionary(result => string.Join(",", result.Labels), result => result.Count);

        var relationshipTypes = await StartQuery
            .Match("()-[r]->()")
            .Return(r => new { Labels = Return.As<string>("DISTINCT type(r)"), Count = r.Count() })
            .ResultsAsync;

        stats.RelationshipLabelCounts = relationshipTypes.ToDictionary(x => x.Labels, x => x.Count);

        return stats;
    }

    public async Task<TimingsAndBatches> Insert<TNode, TRelationship>(CosmosReadOutput<TNode, TRelationship> cosmosData, Mappings mappings, int batchSize = 4000)
        where TNode : CosmosNode
        where TRelationship : CosmosRelationship
    {
        var output = new TimingsAndBatches();

        var now = DateTime.Now;
        var nodeCypher = GenerateNodeQueries(cosmosData.Nodes, batchSize).ToList();
        foreach (var cypher in nodeCypher)
            await cypher.ExecuteWithoutResultsAsync();
        output.Nodes = new TimingAndBatchCount { NumberOfBatches = nodeCypher.Count, TimeTaken = DateTime.Now - now };

        now = DateTime.Now;
        var relationshipCypher = GenerateRelationshipQueries(cosmosData.Relationships, batchSize, mappings).ToList();
        foreach (var cypher in relationshipCypher)
            await cypher.ExecuteWithoutResultsAsync();
        output.Relationships = new TimingAndBatchCount { NumberOfBatches = relationshipCypher.Count, TimeTaken = DateTime.Now - now };

        return output;
    }

    private IEnumerable<ICypherFluentQuery> GenerateRelationshipQueries<TRelationship>(IDictionary<string, List<TRelationship>>? relationships, int batchSize, Mappings mappings)
        where TRelationship : CosmosRelationship
    {
        var output = new List<ICypherFluentQuery>();
        if (relationships == null)
            return output;

        foreach (var typeAndRelationships in relationships)
        {
            var typeRelationshipsGroupedByInOutLabels = typeAndRelationships.Value.GroupBy(x => new { x.InVertexLabel, x.OutVertexLabel });
            foreach (var grp in typeRelationshipsGroupedByInOutLabels)
            {
                var all = grp.ToList();

                var completed = 0;
                var batchNumber = 1;
                while (completed < all.Count)
                {
                    var toInsert = all.Skip((batchNumber - 1) * batchSize).Take(batchSize).ToList();

                    var query = StartQuery.Write
                        .Unwind(toInsert, "rel")
                        .Match($"(inN:{grp.Key.InVertexLabel?.ToNeo4jMapping(mappings.Nodes)} {{CosmosId: rel.inV}})")
                        .Match($"(outN:{grp.Key.OutVertexLabel?.ToNeo4jMapping(mappings.Nodes)} {{CosmosId: rel.outV}})")
                        .Merge($"(inN)<-[r:`{typeAndRelationships.Key}` {{CosmosId:rel.id}}]-(outN)")
                        .Set("r += rel.properties");

                    output.Add(query);

                    batchNumber++;
                    completed += toInsert.Count;
                }
            }
        }

        return output;
    }

    private IEnumerable<ICypherFluentQuery> GenerateNodeQueries<TNode>(IDictionary<string, List<TNode>>? nodes, int batchSize)
        where TNode : CosmosNode
    {
        var output = new List<ICypherFluentQuery>();
        if (nodes == null)
            return output;

        foreach (var labelAndNodes in nodes)
        {
            var completed = 0;
            var batchNumber = 1;
            while (completed < labelAndNodes.Value.Count)
            {
                var toInsert = labelAndNodes.Value.Skip((batchNumber - 1) * batchSize).Take(batchSize).ToList();
                var query = StartQuery.Write
                    .Unwind(toInsert, "node")
                    .Merge($"(n:`{labelAndNodes.Key}` {{CosmosId:node.id}})")
                    .Set($"n += node.{nameof(CosmosNode.PropertiesAsDictionary)}");

                output.Add(query);

                batchNumber++;
                completed += toInsert.Count;
            }
        }

        return output;
    }

    public async Task InsertIndexes(Mappings mappings)
    {
        var session = Driver.AsyncSession(config => config.WithDatabase(Database));
        if (mappings.Nodes != null)
            foreach (var node in mappings.Nodes)
            {
                var nodeTl = node.Neo4j.ToLowerInvariant().Replace(" ", "");
                await session.ExecuteWriteAsync(x => x.RunAsync($"CREATE INDEX {nodeTl}_node_cosmos IF NOT EXISTS FOR (n:`{node.Neo4j}`) ON n.CosmosId"));
                if (node.Properties == null || !node.Properties.Any())
                    continue;

                foreach (var index in node.Properties.Where(p => p.Indexed).Select(property => $"CREATE INDEX {nodeTl}_node_{property.Neo4j?.ToLowerInvariant().Replace(" ", "")} IF NOT EXISTS FOR (n:`{node.Neo4j}`) ON n.{property.Neo4j}"))
                    await session.ExecuteWriteAsync(x => x.RunAsync(index));
            }

        if (mappings.Relationships != null)
            foreach (var rel in mappings.Relationships)
            {
                var relTl = rel.Neo4j.ToLowerInvariant().Replace(" ", "");
                await session.ExecuteWriteAsync(x => x.RunAsync($"CREATE INDEX {relTl}_rel_cosmos IF NOT EXISTS FOR ()-[r:`{rel.Neo4j}`]-() ON r.CosmosId"));
                if (rel.Properties == null || !rel.Properties.Any())
                    continue;

                foreach (var index in rel.Properties.Where(p => p.Indexed).Select(property => $"CREATE INDEX {relTl}_rel_{property.Neo4j?.ToLowerInvariant().Replace(" ", "")} IF NOT EXISTS FOR ()-[r:`{relTl}`]-() ON r.{property.Neo4j}"))
                    await session.ExecuteWriteAsync(x => x.RunAsync(index));
            }
    }

    public class TimingsAndBatches
    {
        public TimingAndBatchCount? Nodes { get; set; }
        public TimingAndBatchCount? Relationships { get; set; }
    }

    public class TimingAndBatchCount
    {
        public TimeSpan TimeTaken { get; set; }
        public int? NumberOfBatches { get; set; }
    }

    private static class Neo4jSettings
    {
        private const string Base = "Neo4j:";

        public const string Host = $"{Base}Host";
        public const string Port = $"{Base}Port";
        public const string Database = $"{Base}Database";
        public const string Username = $"{Base}Username";
        public const string Password = $"{Base}Password";
    }
}