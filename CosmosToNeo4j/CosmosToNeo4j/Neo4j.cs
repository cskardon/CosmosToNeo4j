namespace CosmosToNeo4j;

using global::Neo4j.Driver;
using Neo4jClient;
using Neo4jClient.ApiModels.Cypher;
using Neo4jClient.Cypher;

public class Neo4j
{
    private readonly IGraphClient _client;

    public Neo4j(string uri, string username, string password, string database)
    {
        _client = new BoltGraphClient(uri, username, password);
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

    public void Read(out List<RelationshipWithId> relationships, out List<NodeWithId> nodes, out List<PathsResultBolt> paths)
    {
        var relationshipsQuery = StartQuery
            .Match("()-[r]->()")
            .Return(r => new RelationshipWithId
            {
                Identifier = Return.As<int>("id(r)"),
                Type = Return.As<string>("type(r)"),
                Rel = r.As<RelationshipAll>()
            });

        var nodesQuery = StartQuery
            .Match("(n)")
            .Return(n => new NodeWithId
                {
                    Identifier = Return.As<int>("id(n)"),
                    Labels = Return.As<string>("labels(n)"),
                    Node = n.As<NodeAll>()
                }
            );

        var pathsQuery = StartQuery
            .Match("p = ( (n)-[r]->() )")
            .Return(p => p.As<PathsResultBolt>());

        var relsTask = relationshipsQuery.ResultsAsync;
        var nodesTask = nodesQuery.ResultsAsync;
        var pathsTask = pathsQuery.ResultsAsync;

        Task.WaitAll(relsTask, nodesTask, pathsTask);

        relationships = relsTask.Result.ToList();
        nodes = nodesTask.Result.ToList();
        paths = pathsTask.Result.ToList();
    }

    public async Task Insert<TNode, TRelationship>(CosmosReadOutput<TNode, TRelationship> cosmosData, int batchSize = 4000)
        where TNode : CosmosNode
        where TRelationship : CosmosRelationship
    {
        Console.Write("\tNodes...");
        var nodeCypher = NodeQueries(cosmosData.Nodes, batchSize).ToList();
        foreach (var cypher in nodeCypher)
            await cypher.ExecuteWithoutResultsAsync();
        Console.WriteLine($"...Done ({nodeCypher.Count} batches)");

        Console.Write("\tRelationships...");
        var relationshipCypher = RelationshipQueries(cosmosData.Relationships, batchSize).ToList();
        foreach (var cypher in relationshipCypher)
            await cypher.ExecuteWithoutResultsAsync();
        Console.WriteLine($"...Done ({relationshipCypher.Count} batches)");
    }

    private IEnumerable<ICypherFluentQuery> RelationshipQueries<TRelationship>(IDictionary<string, List<TRelationship>> relationships, int batchSize)
        where TRelationship : CosmosRelationship
    {
        var output = new List<ICypherFluentQuery>();

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

    private IEnumerable<ICypherFluentQuery> NodeQueries<TNode>(IDictionary<string, List<TNode>> nodes, int batchSize)
        where TNode : CosmosNode
    {
        var output = new List<ICypherFluentQuery>();

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

    public abstract class ThingFromNeo
    {
        public string? CosmosId { get; set; }
        public int Identifier { get; set; }
        public bool HasBeenAdded { get; set; }
    }

    public class RelationshipWithId : ThingFromNeo
    {
        public string? Type { get; set; }
        public RelationshipAll? Rel { get; set; }
    }

    public class NodeWithId : ThingFromNeo
    {
        public string? Labels { get; set; }
        public NodeAll? Node { get; set; }
    }
}