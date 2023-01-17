namespace CosmosToNeo4j.Models;

using Newtonsoft.Json;

public abstract class CosmosEntity
{
    [JsonProperty("id")] public string? Id { get; set; }
    [JsonProperty("label")] public string? Label { get; set; }
    [JsonProperty("type")] public string? Type { get; set; }
}