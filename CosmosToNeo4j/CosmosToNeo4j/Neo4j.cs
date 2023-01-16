namespace CosmosToNeo4j;

using global::Neo4j.Driver;
using Microsoft.Extensions.Configuration;
using Neo4jClient;
using Neo4jClient.Cypher;

public class Neo4j
{
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

    public async Task<TimingsAndBatches> Insert<TNode, TRelationship>(CosmosReadOutput<TNode, TRelationship> cosmosData, int batchSize = 4000)
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
        var relationshipCypher = GenerateRelationshipQueries(cosmosData.Relationships, batchSize).ToList();
        foreach (var cypher in relationshipCypher)
            await cypher.ExecuteWithoutResultsAsync();
        output.Relationships = new TimingAndBatchCount { NumberOfBatches = relationshipCypher.Count, TimeTaken = DateTime.Now - now };

        return output;
    }

    private IEnumerable<ICypherFluentQuery> GenerateRelationshipQueries<TRelationship>(IDictionary<string, List<TRelationship>>? relationships, int batchSize)
        where TRelationship : CosmosRelationship
    {
        var output = new List<ICypherFluentQuery>();
        if (relationships == null)
            return output;

        foreach (var typeAndRelationships in relationships)
        {
            var completed = 0;
            var batchNumber = 1;
            while (completed < typeAndRelationships.Value.Count)
            {
                var toInsert = typeAndRelationships.Value.Skip((batchNumber - 1) * batchSize).Take(batchSize).ToList();

                var query = StartQuery.Write
                    .Unwind(toInsert, "rel")
                    .Match("(inN {CosmosId: rel.inV })")
                    .Match("(outN {CosmosId: rel.outV })")
                    .Merge($"(inN)-[r:`{typeAndRelationships.Key}`]->(outN)")
                    .Set($"r += rel.properties");

                output.Add(query);

                batchNumber++;
                completed += toInsert.Count;
            }
        }

        return output;
    }

    private IEnumerable<ICypherFluentQuery> GenerateNodeQueries<TNode>(IDictionary<string, List<TNode>>? nodes, int batchSize)
        where TNode : CosmosNode
    {
        var output = new List<ICypherFluentQuery>();
        if(nodes == null) 
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
        foreach (var node in mappings.Nodes)
            await session.ExecuteWriteAsync(x => x.RunAsync($"CREATE INDEX {node.Neo4j}_node_cosmos IF NOT EXISTS FOR (n:{node.Neo4j}) ON n.CosmosId"));
        foreach (var rel in mappings.Relationships)
            await session.ExecuteWriteAsync(x => x.RunAsync($"CREATE INDEX {rel.Neo4j}_rel_cosmos IF NOT EXISTS FOR ()-[r:{rel.Neo4j}]-() ON r.CosmosId"));
    }
}