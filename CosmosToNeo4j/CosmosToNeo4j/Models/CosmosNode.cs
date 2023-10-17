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

    public void RemoveIgnored(IList<PropertyMap>? properties)
    {
        if (properties == null) 
            return;

        foreach (var property in properties.Where(p => p.Ignored))
            if (!string.IsNullOrWhiteSpace(property.Neo4j))
                PropertiesAsDictionary?.Remove(property.Neo4j);
    }

    private IDictionary<string, object> PropertiesToDictionary()
    {
        if (Properties == null)
            return new Dictionary<string, object>();

        var output = new Dictionary<string, object>();
        foreach (var element in Properties)
        {
            var props = (element.Value?.ToObject<List<CosmosProperty>>() ?? new List<CosmosProperty>())
                .Select(p => p.Value)
                .Where(v => v != null);
            
            output.Add(element.Key, props);
        }

        return output;
    }

    public class CosmosProperty
    {
        [JsonProperty("id")] public string? Id { get; set; }

        [JsonProperty("value")] public object? Value { get; set; }
    }
}
