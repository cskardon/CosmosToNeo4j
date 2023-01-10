using System.Text.Json;
using Gremlin.Net.Structure.IO.GraphSON;

namespace CosmosToNeo4j;

/// <summary>
/// This exists to cover for the fact that Cosmos doesn't parse numbers correctly.
/// </summary>
// ReSharper disable once InconsistentNaming
public class CustomGraphSON2Reader : GraphSON2Reader
{
    public override dynamic ToObject(JsonElement graphSon) =>
        graphSon.ValueKind switch
        {
            // numbers
            JsonValueKind.Number when graphSon.TryGetInt32(out var intValue) => intValue,
            JsonValueKind.Number when graphSon.TryGetInt64(out var longValue) => longValue,
            JsonValueKind.Number when graphSon.TryGetDecimal(out var decimalValue) => decimalValue,
            

            _ => base.ToObject(graphSon)
        };
}