The Wikipedia summary :
-----------------------

    *A graph database is a database that uses graph structures with nodes, edges, and properties to represent and store data.*

    *A graph database is any storage system that provides index-free adjacency. This means that every element contains a direct pointer to its adjacent element and no index lookups are necessary. General graph databases that can store any graph are distinct from specialized graph databases such as triplestores and network databases.*

For further information, you can read this `article on Wikipedia <http://en.wikipedia.org/wiki/Graph_database>`_

Is GrapheekDB a graph database ? :
----------------------------------

Regarding Wikipedia definition :

- GrapheekDB allows to **add nodes and edges**
- **Adjacency does not need index** : GrapheekDB doesn't read an index to find nodes or edges neighbours.
- GrapheekDB is a **general graph database**
- There's **no strict assertions on data modelling**
- It is **schemaless**

There's other taxonomies for graph database, so to answer :

- GrapheekDB is a **multi model document store graph database** : every node or edge can have related data, but this is not mandatory
- It is **persistent** (if and only **if you choose a persistent Key Value Store** such as KyotoCabinet or Symas Lmdb)
- It only represents **Directed Graphs**

And, It has **a lot** of `additionnal features <features.rst>`_
