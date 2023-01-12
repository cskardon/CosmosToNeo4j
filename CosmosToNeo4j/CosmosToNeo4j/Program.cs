#define Emulator
// #define Azure
using CosmosToNeo4j;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;

var config = new ConfigurationBuilder()
    .SetBasePath(AppDomain.CurrentDomain.BaseDirectory)
    .AddJsonFile("appsettings.json")
#if Azure
    .AddUserSecrets<Program>()
#endif
    .Build();

string? mappingFile = null;

ParseArgs(args);

void ParseArgs(string[] args)
{
    for (int i = 0; i < args.Length; i++)
    {
        switch (args[i].ToLowerInvariant())
        {
            case "-m":
            case "-map":
            case "-mapping":
                i++;
                mappingFile = args[i];
                break;
            default:
                throw new ArgumentOutOfRangeException("args", args[i], $"Unknown parameter '{args[i]}'.");
        }
    }
}

Mappings? mappings = null;
if (mappingFile != null)
{
    var content = File.ReadAllText(mappingFile);
    mappings = JsonConvert.DeserializeObject<Mappings>(content);
}

mappings ??= new Mappings();

var accountKey = config.GetValue<string>("CosmosDB:AccessKey");
var database = config.GetValue<string>("CosmosDB:Database");
var container = config.GetValue<string>("CosmosDB:Container");
var host = config.GetValue<string>("CosmosDB:Host");
var port = config.GetValue<int>("CosmosDB:Port");
var enableSsl =config.GetValue<bool>("CosmosDB:EnableSsl");

var containerLink = $"/dbs/{database}/colls/{container}";
var cosmos = new CosmosDb(host, port, containerLink, accountKey, enableSsl);
var neo4j = new CosmosToNeo4j.Neo4j(
    $"{config.GetValue<string>(Neo4jSettings.Host)}:{config.GetValue<int>(Neo4jSettings.Port)}",
    config.GetValue<string>(Neo4jSettings.Username)!,
    config.GetValue<string>(Neo4jSettings.Password)!, 
    config.GetValue<string>(Neo4jSettings.Database)!
    );

// Console.WriteLine("Reading from Neo4j");
// neo4j.Read(out var relationships, out var nodes, out var paths);

var cosmosStats = await cosmos.GetStats();
if (!Transferer.AreCosmosMappingsOk(mappings, cosmosStats, out var missingNodeLabels, out var missingRelationshipTypes))
{
    if (missingNodeLabels.Any())
    {
        Console.WriteLine("You have node (vertex!) labels that are in Cosmos, but not in your Mappings:");
        foreach (var missingNodeLabel in missingNodeLabels)
            Console.WriteLine($"\t* {missingNodeLabel}");
        Console.WriteLine("If you want to just transfer these in a 'like for like' manner, please type OK and press ENTER");
        var ok = Console.ReadLine();
        if (ok?.ToLowerInvariant() != "ok")
        {
            Console.WriteLine("Exiting! Nothing has happened yet!");
            return;
        }

        foreach (var missingNodeLabel in missingNodeLabels)
            mappings.Nodes.Add(new Map{Cosmos = missingNodeLabel, Neo4j = missingNodeLabel});
    }

    if (missingRelationshipTypes.Any())
    {
        Console.WriteLine("You have relationship (edge!) types that are in Cosmos, but not in your Mappings:");
        foreach (var missingRelationshipType in missingRelationshipTypes)
            Console.WriteLine($"\t* {missingRelationshipType}");
        Console.WriteLine("If you want to just transfer these in a 'like for like' manner, please type OK and press ENTER");
        var ok = Console.ReadLine();
        if (ok?.ToLowerInvariant() != "ok")
        {
            Console.WriteLine("Exiting! Nothing has happened yet!");
            return;
        }

        foreach (var missingRelationshipType in missingRelationshipTypes)
            mappings.Relationships.Add(new Map{Cosmos = missingRelationshipType, Neo4j = missingRelationshipType });
    }
}


var neo4jStats = await neo4j.GetStats();

if (neo4jStats.ContainsData())
{
    Console.WriteLine($"Your Neo4j Database ({neo4j.Database} on {neo4j.Uri}) is not empty, if you want to continue, please type 'YES' and press ENTER");
    var response = Console.ReadLine();
    if (response?.ToLowerInvariant() != "yes")
        return;
}

// Console.WriteLine("Inserting into CosmosDB");
// await cosmos.Insert(paths, nodes, relationships);

// Console.WriteLine("PAUSE!!!!");
// Console.ReadLine();

Console.WriteLine("Reading from CosmosDB");
var cosmosData = await cosmos.Read<CosmosNode, CosmosRelationship>(mappings, cosmosStats);

Console.WriteLine("Inserting into Neo4j");
await neo4j.InsertIndexes(mappings);
Console.WriteLine("Indexes done...");
await neo4j.Insert(cosmosData);
Console.WriteLine("Data in!");

Console.WriteLine("press enter to exit");
Console.ReadLine();


public class Mappings
{
    public List<Map> Nodes { get; set; }
    public List<Map> Relationships { get; set; }
}

public class Map
{
    public string Cosmos { get; set; }
    public string Neo4j { get; set; }
}

public static class Neo4jSettings
{
    private const string Base = "Neo4j:";

    public const string Host = $"{Base}Host";
    public const string Port = $"{Base}Port";
    public const string Database = $"{Base}Database";
    public const string Username = $"{Base}Username";
    public const string Password = $"{Base}Password";
}