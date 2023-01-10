namespace CosmosToNeo4j;

using System.Text;
using Gremlin.Net.Driver;
using Gremlin.Net.Structure.IO.GraphSON;
using Neo4jClient.ApiModels.Cypher;
using Newtonsoft.Json;

public class CosmosReadOutput<TNode, TRelationship>
    where TNode : CosmosNode
    where TRelationship : CosmosRelationship
{
    /// <summary>
    /// A dictionary of the nodes by *Neo4j* label.
    /// </summary>
    public IDictionary<string, List<TNode>> Nodes { get; set; }

    /// <summary>
    /// A dictionary of the relationships by *Neo4j* type.
    /// </summary>
    public IDictionary<string, List<TRelationship>> Relationships { get; set; }
}

public class CosmosDb
{
    private readonly IPaginator _paginator;
    private readonly IGremlinClient _client;

    public CosmosDb(string? host, int port, string username, string? accountKey, bool enableSsl, IPaginator? paginator = null)
    {
        _client = new GremlinClient(new GremlinServer(host, port, enableSsl, username, accountKey), new GraphSON2MessageSerializer(new CustomGraphSON2Reader()));
        _paginator = paginator ?? new GuidIdPaginator();
    }

    public async Task Insert(ICollection<PathsResultBolt> paths, ICollection<Neo4j.NodeWithId> nodes, ICollection<Neo4j.RelationshipWithId> relationships)
    {
        await _client.CleanDb();
        foreach (var path in paths)
        foreach (var relationship in path.Relationships)
        {
            var startNode = nodes.SingleOrDefault(x => x.Identifier == relationship.StartNodeId);
            var endNode = nodes.SingleOrDefault(x => x.Identifier == relationship.EndNodeId);
            var relationshipInstance = relationships.SingleOrDefault(x => x.Identifier == relationship.Id);
            if (relationshipInstance == null || startNode == null || endNode == null || startNode.Labels == null || endNode.Labels == null)
                continue;

            if (!startNode.HasBeenAdded)
                startNode.CosmosId = await _client.InsertVertex("g", startNode.Identifier, startNode.Node, startNode.Labels.Tidy());
            if (!endNode.HasBeenAdded)
                endNode.CosmosId = await _client.InsertVertex("g", endNode.Identifier, endNode.Node, endNode.Labels.Tidy());

            startNode.HasBeenAdded = true;
            endNode.HasBeenAdded = true;

            if (!relationshipInstance.HasBeenAdded)
                await _client.InsertEdge("g", relationshipInstance.Type!, startNode.CosmosId, endNode.CosmosId, relationshipInstance.Rel!);

            relationshipInstance.HasBeenAdded = true;
        }
    }

    public async Task<CosmosReadOutput<TNode, TRelationship>> Read<TNode, TRelationship>(Mappings mappings, Stats cosmosStats)
        where TNode : CosmosNode
        where TRelationship : CosmosRelationship
    {
        var output = new CosmosReadOutput<TNode, TRelationship>();

        //TODO Hard coded page size - auto generation of count / 36?
        var nodePageSize = (int) cosmosStats.TotalNodes / 36;

        var nodes = await ReadNodes<TNode>(mappings, nodePageSize, cosmosStats);
        output.Nodes = nodes;

        var relationships = await ReadEdges<TRelationship>(mappings, cosmosStats, (int) cosmosStats.TotalRelationships / 36);
        output.Relationships = relationships;

        return output;
    }

    public async Task<string> Read(Mappings mappings, Stats cosmosStats)
    {
        //TODO Hard coded page size - auto generation of count / 36?
        var nodePageSize = (int) cosmosStats.TotalNodes / 36;

        var output = new StringBuilder();
        output.Append(ReadNodes(mappings, out var cosmosIdToCypherImportId, nodePageSize, cosmosStats));

        output.Append(await ReadEdges(mappings, cosmosIdToCypherImportId));
        return output.ToString();
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
        var output = new Dictionary<string, long>();
        var resultSet = await _client.SubmitAsync<dynamic>($"g.{(forNodes ? "V" : "E")}().groupCount().by(label).unfold().project('Label','Count').by(keys).by(values)");
        foreach (var result in resultSet)
        {
            var count = JsonConvert.DeserializeObject<LabelCounts>(JsonConvert.SerializeObject(result));
            output.Add(count.Label, count.Count);
        }

        return output;
    }

    // ReSharper disable once ClassNeverInstantiated.Local
    private class LabelCounts
    {
        public string? Label { get; set; }
        public long Count { get; set; }
    }

    private async Task<IDictionary<string, List<T>>> Read<T>(bool isNode, IList<Map> mappings, Stats stats, int pageSize)
        where T: CosmosEntity
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
        return (int) Math.Min(count / pageSize, 36);
    }

    private async Task<IDictionary<string, List<TRelationship>>> ReadEdges<TRelationship>(Mappings mappings, Stats cosmosStats, int pageSize)
        where TRelationship : CosmosRelationship
    {
        return await Read<TRelationship>(false, mappings.Relationships, cosmosStats, pageSize);
    }
    
    // <summary>
    /// 
    /// </summary>
    /// <param name="mappings"></param>
    /// <param name="pageSize">This is going to be used as a rough guide - we can't actually page, so we're using this to work out the 'number' of pages.</param>
    /// <param name="cosmosStats"></param>
    /// <returns>A cypher statement</returns>
    private async Task<IDictionary<string, List<TNode>>> ReadNodes<TNode>(Mappings mappings, int pageSize, Stats cosmosStats )
     where TNode : CosmosNode
    {
        return await Read<TNode>(true, mappings.Nodes, cosmosStats, pageSize);
    }










    private async Task<string> ReadEdges(Mappings mappings, IDictionary<Guid, int> cosmosIdToCypherImportId)
    {
        var output = new StringBuilder();

        var relationshipCounter = 0;
        foreach (var mapping in mappings.Relationships)
        {
            var relationships = await _client.SubmitAsync<dynamic>($"g.E().hasLabel(\"{mapping.Cosmos}\")");
            foreach (var relationship in relationships)
            {
                CosmosEdgeResponse obj = JsonConvert.DeserializeObject<CosmosEdgeResponse>(JsonConvert.SerializeObject(relationship));

                var startNode = cosmosIdToCypherImportId[obj.InVertexId];
                var endNode = cosmosIdToCypherImportId[obj.OutVertexId];

                var cypher = new StringBuilder($"MERGE (n{startNode})-[r{relationshipCounter}:`{mapping.Neo4j}`]->(n{endNode})");
                if (obj.Properties.Any())
                    cypher.Append(" SET");
                var properties = obj.Properties.Select(property => $" r{relationshipCounter}.{property.Key} = {GetValue(property.Value)}").ToList();
                cypher.Append(string.Join(",", properties));

                output.AppendLine(cypher.ToString());
                relationshipCounter++;
            }
        }

        return output.ToString();
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="mappings"></param>
    /// <param name="cosmosIdToCypherImportId"></param>
    /// <param name="pageSize">This is going to be used as a rough guide - we can't actually page, so we're using this to work out the 'number' of pages.</param>
    /// <param name="cosmosStats"></param>
    /// <returns>A cypher statement</returns>
    private string ReadNodes(Mappings mappings, out IDictionary<Guid, int> cosmosIdToCypherImportId, int pageSize, Stats cosmosStats )
    {
        //I would rather this returned something I can parse properly into statements.
        //IDictionary<string, List<dynamic>> (string = label name)

        var output = new StringBuilder();
        cosmosIdToCypherImportId = new Dictionary<Guid, int>();
        var nodeCounter = 0;
        foreach (var mapping in mappings.Nodes)
        {
            var numPages = (int) Math.Min(cosmosStats.NodeLabelCounts[mapping.Cosmos] / pageSize, 36);
            var gremlinQueries = new GuidIdPaginator().GeneratePagingPairs(numPages).Select(x => x.ToGremlinNode(mapping.Cosmos));
            foreach (var gremlinQuery in gremlinQueries)
            {
                // var readNodesTask = _client.SubmitAsync<dynamic>($"g.V().hasLabel(\"{mapping.Cosmos}\")");
                var readNodesTask = _client.SubmitAsync<dynamic>(gremlinQuery);
                readNodesTask.Wait();
                var nodes = readNodesTask.Result;
                foreach (var node in nodes)
                {
                    CosmosResponse obj = JsonConvert.DeserializeObject<CosmosResponse>(JsonConvert.SerializeObject(node));

                    var cypher = new StringBuilder($"MERGE (n{nodeCounter}:`{mapping.Neo4j}` {{CosmosId:\"{obj.Id}\"}})");
                    if (obj.Properties.Any())
                        cypher.Append(" SET");

                    var properties = obj.Properties.Select(property => $" n{nodeCounter}.{property.Key} = {GetValue(property.Value[0].Value)}").ToList();

                    cypher.Append(string.Join(",", properties));
                    cosmosIdToCypherImportId.Add(obj.Id, nodeCounter);

                    output.AppendLine(cypher.ToString());
                    nodeCounter++;
                }
            }
        }

        return output.ToString();
    }

    private static string? GetValue(object value)
    {
        if (value == null)
            throw new ArgumentNullException(nameof(value));

        return value switch
        {
            int => value.ToString(),
            double => value.ToString(),
            _ => $"\"{value}\""
        };
    }

    #region Cosmos Response Model Classes
    public class CosmosEdgeResponse : CosmosRelationship
    {
        



    }


    #endregion Cosmos Response Model Classes
}