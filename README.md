# Cosmos To Neo4j

Taking CosmosDB and migrating to Neo4j

---

Do you have data in CosmosDB? But you want to see that data in Neo4j?
Maybe just to test, to compare etc, but the **effort** oh so much effort, if only there was some way to just get it all and push it in...

Well...

Good news!! This project exists to allow you to connect to a CosmosDB instance, and read the Vertices and Edges and translate them into Nodes and Relationships 
and put them into a Neo4j database.

## Some guidance

There is 1 thing you have to do:

* Update the `appsettings.json` to have the correct config for your Cosmos and Neo4j instances

You can also (optionally) provide a `Mappings` file.

## Parameters

There are some parameters you can use to configure the running of the app.

### Mappings (optional)

`-mapping <FILENAME>` - allows you to specify a Mappings file to map Cosmos Labels to Neo4j Labels.

### BatchSize (optional)

`-batchsize <INT>` - allows you to define the size of the batch that will be ingested into your Neo4j database. You might need to set this to a 
low number for environments with lower memory. The default size is `4000`.

NB This will batch _per label_ or _type_ - so you might have less than 4000 nodes, but see 5 batches, because you have 5 different Labels on your nodes.

## Mappings

The mappings file is a JSON document that tells the app what to map each Label to from Cosmos to Neo4j

The structure is in the format of an Array of Node mappings, and an Array of Relationship mappings (see the example below)

```
{
  "Nodes": [
    {
      "Cosmos": "Actor",
      "Neo4j": "Person"
    }
  ],
  "Relationships": [
    {
      "Cosmos": "Acted",
      "Neo4j": "ACTED_IN"
    },
    {
      "Cosmos": "Watched",
      "Neo4j": "REVIEWED"
    }
  ]
}
```

File name doesn't matter - only the content. You pass it in using the `-m` argument to the app:

`.\CosmosToNeo4j.exe -m .\Mapping.json`

_If_ you don't supply it - you will be asked if you just want to map the labels / types in a 1-to-1 fashion

## Probable issues

* Performance
* Data type support
* Random bugs due to lack of testing

Please try and let me know - raise issues - if at all possible with ways to replicate the data in Cosmos (in the emulator if possible) - so I can test properly.