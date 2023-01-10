namespace CosmosToNeo4j;

public class Pairing
{
    public string? Start { get; set; }
    public string? End { get; set; }

    public string ToGremlinNode(string? label = null)
    {
        return ToGremlin(true, Start, End, label);
    }

    public string ToGremlinRelationship(string? type = null)
    {
        return ToGremlin(false, Start, End, type);
    }

    private static string ToGremlin(bool isNode, string? start, string? end, string? label = null)
    {
        var identifier = isNode ? "V()" : "E()";
        if (!string.IsNullOrWhiteSpace(label))
            identifier += $".hasLabel(\"{label}\")";

        return string.IsNullOrWhiteSpace(start)
            ? $"g.{identifier}"
            : $"g.{identifier}.has('id',gte('{start}')){(!string.IsNullOrWhiteSpace(end) ? $".has('id', lt('{end}'))" : string.Empty)}";
    }
}