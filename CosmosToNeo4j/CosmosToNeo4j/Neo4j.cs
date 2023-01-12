namespace CosmosToNeo4j;

using System.Runtime.CompilerServices;
using System.Text;
using System.Xml.Linq;
using global::Neo4j.Driver;
using Neo4jClient;
using Neo4jClient.ApiModels.Cypher;
using Neo4jClient.Cypher;

public class Neo4j
{
    private readonly IGraphClient _client;
    internal string Database { get; }
    internal string Uri { get; }
    
    public Neo4j(string uri, string username, string password, string database)
    {
        _client = new BoltGraphClient(uri, username, password);
        Database = string.IsNullOrWhiteSpace(database) ? "neo4j" : database;

        Uri = uri;
        var connectTask = _client.ConnectAsync();
        connectTask.Wait();
    }

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
        var nodeCypher = NodeCypher(cosmosData.Nodes, batchSize);
        foreach (var cypher in nodeCypher)
            await Insert(cypher);

        var relationshipCypher = RelationshipCypher(cosmosData.Relationships, batchSize);
        foreach (var cypher in relationshipCypher)
            await Insert(cypher);
    }

    private static IEnumerable<string> RelationshipCypher<TRelationship>(IDictionary<string, List<TRelationship>> relationships, int batchSize)
        where TRelationship : CosmosRelationship
    {
        var output = new List<string>();

        int relationshipCounter = 0;
        int nodeCounter = 0;

        var batchCypher = new StringBuilder();
        foreach (var typeAndRelationships in relationships)
        {
            foreach (var cosmosRelationship in typeAndRelationships.Value)
            {
                if (relationshipCounter >= batchSize)
                {
                    output.Add(batchCypher.ToString());
                    batchCypher = new StringBuilder();
                }

                var startNode = nodeCounter++;
                var endNode = nodeCounter++;

                var cypher = new StringBuilder()
                    .Append($"{(batchCypher.Length > 0 ? "WITH 0 AS _ " : string.Empty)}")
                    .Append($"MATCH (n{startNode} {{CosmosId:\"{cosmosRelationship.InVertexId}\"}}) ")
                    .Append($"MATCH (n{endNode} {{CosmosId:\"{cosmosRelationship.OutVertexId}\"}}) ")
                    .Append($"MERGE (n{startNode})-[r{relationshipCounter}:`{typeAndRelationships.Key}` {{CosmosId:\"{cosmosRelationship.Id}\"}}]->(n{endNode})");
                
                if (cosmosRelationship.Properties.Any())
                    cypher.Append(" SET");

                var properties = cosmosRelationship.Properties.Select(property => $" r{relationshipCounter}.{property.Key} = {GetValue(property.Value)}").ToList();
                cypher.Append(string.Join(",", properties));



                batchCypher.AppendLine(cypher.ToString());
                relationshipCounter++;
            }
        }
        output.Add(batchCypher.ToString());

        return output;    
    }

    private static IEnumerable<string> NodeCypher<TNode>(IDictionary<string, List<TNode>> nodes, int batchSize)
        where TNode : CosmosNode
    {
        var output = new List<string>();

        int nodeCounter = 0;

        var batchCypher = new StringBuilder();
        foreach (var labelAndNodes in nodes)
        {
            foreach (var cosmosNode in labelAndNodes.Value)
            {
                if (nodeCounter >= batchSize)
                {
                    output.Add(batchCypher.ToString());
                    batchCypher = new StringBuilder();
                }

                var cypher = new StringBuilder($"MERGE (n{nodeCounter}:`{labelAndNodes.Key}` {{CosmosId:\"{cosmosNode.Id}\"}})");
                if (cosmosNode.Properties.Any())
                    cypher.Append(" SET");

                var properties = cosmosNode.Properties.Select(property => $" n{nodeCounter}.{property.Key} = {GetValue(property.Value[0].Value)}").ToList();
                cypher.Append(string.Join(",", properties));
                batchCypher.AppendLine(cypher.ToString());
                nodeCounter++;
            }
        }
        output.Add(batchCypher.ToString());

        return output;
    }

    private static string? GetValue(object value)
    {
        if (value == null)
            throw new ArgumentNullException(nameof(value));

        return value switch
        {
            int => value.ToString(),
            double => value.ToString(),
            _ => $"\"{value.ToString()?.Replace("\"", "\\\"")}\""
        };
    }


    public async Task Insert(string cypher)
    {
        var session = Driver.AsyncSession(config => config.WithDatabase(Database));
        await session.ExecuteWriteAsync(x => x.RunAsync(cypher));
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

    public async Task InsertIndexes(Mappings mappings)
    {
        var session = Driver.AsyncSession(config => config.WithDatabase(Database));
        foreach (var node in mappings.Nodes)
            await session.ExecuteWriteAsync(x => x.RunAsync($"CREATE INDEX {node.Neo4j}_node_cosmos IF NOT EXISTS FOR (n:{node.Neo4j}) ON n.CosmosId"));
        foreach (var rel in mappings.Relationships)
            await session.ExecuteWriteAsync(x => x.RunAsync($"CREATE INDEX {rel.Neo4j}_rel_cosmos IF NOT EXISTS FOR ()-[r:{rel.Neo4j}]-() ON r.CosmosId"));
    }
}