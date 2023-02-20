namespace CosmosToNeo4j.Models;

public class CosmosReadOutput<TNode, TRelationship>
    where TNode : CosmosNode
    where TRelationship : CosmosRelationship
{
    /// <summary>
    ///     A dictionary of the nodes by *Neo4j* label.
    /// </summary>
    public IDictionary<string, List<TNode>>? Nodes { get; set; }

    /// <summary>
    ///     A dictionary of the relationships by *Neo4j* type.
    /// </summary>
    public IDictionary<string, List<TRelationship>>? Relationships { get; set; }
}