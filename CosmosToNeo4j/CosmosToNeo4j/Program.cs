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

const int maxNumberOfPages = 36;

string? mappingFile = null;
var batchSize = 2000;
var pages = maxNumberOfPages;

ParseArgs(args);

void ParseArgs(string[] args)
{
    for (var i = 0; i < args.Length; i++)
    {
        switch (args[i].ToLowerInvariant())
        {
            case "-m":
            case "-map":
            case "-mapping":
                mappingFile = args[++i];
                break;
            case "-bs":
            case "-batchsize":
                batchSize = int.Parse(args[++i]);
                Console.WriteLine($"Batch Size set to: {batchSize}");
                break;
            case "-p":
            case "-pages":
                pages = int.Parse(args[++i]);
                if (pages > maxNumberOfPages)
                {
                    Console.WriteLine($"** Pages supplied '{pages}' is greater than the maximum allowed at present ({maxNumberOfPages}), so it has been reset **");
                    pages = maxNumberOfPages;
                }
                Console.WriteLine($"Pages set to {pages}");
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

var cosmos = new CosmosDb(config);
var neo4j = new CosmosToNeo4j.Neo4j(config);

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

Console.Write("Reading from CosmosDB...");
var now = DateTime.Now;
var cosmosData = await cosmos.Read<CosmosNode, CosmosRelationship>(mappings, cosmosStats, pages);
Console.WriteLine($" done ({(DateTime.Now - now).TotalMilliseconds} ms)");

Console.WriteLine("Inserting into Neo4j");
Console.Write("Adding indexes...");
now = DateTime.Now;
await neo4j.InsertIndexes(mappings);
Console.WriteLine($" done ({(DateTime.Now - now).TotalMilliseconds} ms)");

Console.Write("Adding nodes and relationships...");
now = DateTime.Now;
var tab = await neo4j.Insert(cosmosData, batchSize);
Console.WriteLine($" done ({(DateTime.Now - now).TotalMilliseconds}ms)");
Console.WriteLine($"\t* Nodes         - {tab.Nodes?.NumberOfBatches} batches ({tab.Nodes?.TimeTaken.TotalMilliseconds} ms)");
Console.WriteLine($"\t* Relationships - {tab.Relationships?.NumberOfBatches} batches ({tab.Relationships?.TimeTaken.TotalMilliseconds} ms)");


var countStats = Transferer.CompareCounts(cosmosStats, await neo4j.GetStats());
if (countStats.ContainsData())
{
    Console.WriteLine("There is a difference between the counts in Cosmos vs Neo4j:");
    Console.WriteLine("\t Nodes");
    Console.WriteLine($"\t\tCosmos: {cosmosStats.TotalNodes}{Environment.NewLine}\t\tNeo4j: {cosmosStats.TotalNodes - countStats.TotalNodes}");
    Console.WriteLine("\t Relationships");
    Console.WriteLine($"\t\tCosmos: {cosmosStats.TotalRelationships}{Environment.NewLine}\t\tNeo4j: {cosmosStats.TotalRelationships - countStats.TotalRelationships}");
}
else
{
    Console.WriteLine($"Imported {cosmosStats.TotalNodes} nodes and {cosmosStats.TotalRelationships} relationships to Neo4j.");
}

Console.WriteLine("Press ENTER to exit");
Console.ReadLine();