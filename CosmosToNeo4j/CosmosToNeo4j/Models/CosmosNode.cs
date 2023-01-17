namespace CosmosToNeo4j.Models;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

public class CosmosNode : CosmosEntity
{
    private JObject? _properties;
    public JObject? Properties
    {
        get => _properties;
        set
        {
            _properties = value;
            PropertiesAsDictionary = PropertiesToDictionary();
        }
    }

    public IDictionary<string, object>? PropertiesAsDictionary { get; private set; }

    private IDictionary<string, object> PropertiesToDictionary()
    {
        if (Properties == null)
            return new Dictionary<string, object>();

        var output = new Dictionary<string, object>();
        foreach (var element in Properties)
        {
            var prop = (element.Value?.ToObject<List<CosmosProperty>>() ?? new List<CosmosProperty>()).FirstOrDefault();
            if (prop != null && prop.Value != null)
                output.Add(element.Key, prop.Value);
        }

        return output;
    }

    public class CosmosProperty
    {
        [JsonProperty("id")] public string? Id { get; set; }

        [JsonProperty("value")] public object? Value { get; set; }
    }
}
