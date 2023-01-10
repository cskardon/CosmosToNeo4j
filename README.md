# CosmosToNeo4j

Taking CosmosDB and migrating to Neo4j

---

### To use with Gremlin Console


The `cosmos-emulator.yaml` file is in the folder with this. You will want to 

```
:remote connect tinkerpop.server conf/cosmos-emulator.yaml
:remote console
```

Does this work on Cosmos proper? Doesn't seem to on the Emulator

```
gremlin> t = g.V().hasLabel('person');[]
gremlin> t.next(2)
==>v[1]
==>v[2]
gremlin> t.next(2)
==>v[4]
==>v[6]
```

Get a specific node by Neo4j Id.

```
g.V().hasLabel("Person").has("Identifier", eq("1"))

```

Mapping file idea?

`Type::Cosmos::Neo4j`
```
Node::"Person"::"Person"
Node::"Movie"::"Movie"
Rel::"ACTED_IN"::"ACTED_IN"
```

PartitionKey = /PartitionKey

Get labels?

```
g.V().groupCount().by(label).unfold().project('Entity Type','Count').by(keys).by(values)
```