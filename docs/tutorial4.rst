GrapheekDB Tutorial part 4 : Scaling our app : indexes, performance tips
========================================================================

Previous part : `Part 3 : Path traversal, collecting and aggregating data <tutorial3.rst>`_

....

In this 4th part, you'll find a collection of performance tips

1 - Limit your queries
----------------------

When you build a query which will infer path traversal, try, whenever it is possible to add a .limit(<value>) :

Almost every collection in GrapheekDB core is built with iterators, so by adding a .limit(<value>) clause, you will stop iteration after <value> items. So if value if small, the query will be quite fast.

to summarize :

THIS IS BAD :

.. sourcecode:: python

    nodes = list(g.V())[:10]

THIS IS GOOD :

.. sourcecode:: python

    nodes = g.V().limit(10)

However, remember that GrapheekDB is doing DFS (Depth First Search) not BFS (Breadth First Search), so when using .limit, you will restrict yourself to a sub-sub-sub-....-"tree" which won't be what you expect in some cases

2 - Use indexes
---------------

GrapheekDB has builtin indexes.

So far, **it only has "exact match indexes"**

But, this helps speed up a lot of queries, let's see an example of performance improvements when using indexes (using IPython) :

.. sourcecode:: bash

    In [1]: from grapheekdb.backends.data.localmem import LocalMemoryGraph

    In [2]: g = LocalMemoryGraph()

    In [3]: for i in range(100000):
       ...:     g.add_node(my_id=i)
       ...:

    In [4]: %timeit g.V(my_id=54321).count()
    1 loops, best of 3: 197 ms per loop

    In [5]: g.add_node_index('my_id')

    In [6]: %timeit g.V(my_id=54321).count()
    10000 loops, best of 3: 14 µs per loop

    In [7]: g.remove_node_index('my_id')

    In [8]: %timeit g.V(my_id=54321).count()
    1 loops, best of 3: 194 ms per loop

- We have a graph with 100000 nodes
- Without indexes, it takes 197ms to find a node with a given id
- With indexes, it only takes 14 µs

More information :

- Indexes takes a lot of disk space and slow down writes - use them only when needed (a good start is to only index the property that you would call the primary key in RDBMS world)
- Edges can also be indexed (use : g.add_edge_index(...) to add an edge index & g.remove_edge_index(...) to remove an index)


3 - Know your backends
----------------------

Depending on the backend you use, it is a good idea to explore their related documentation to get performance tips

4 - Promote important properties to nodes
-----------------------------------------

If we look again at our "book store graph database", we can see that the 'thema' property could be "promoted" to node so that we could turn this query (to find every book of a given thema) :

.. sourcecode:: python

    g.V(kind='book', thema='xxxx')

into this query :

.. sourcecode:: python

    g.V(kind='thema', name='xxxx').in_('has_thema')

This will often lead to performance improvements because :

- the first query will need to do a sequential scan to find every book of thema 'xxxx' (yes, an index could definitely helps).
- whereas the thema node 'xxxx' will "already know its neighbours"

In a way, you'll use these promoted nodes as "local indexes"

Moreover, promoting important properties "unlocks" new path traversal, you could, for instance, now write :

.. sourcecode:: python

    person4.out_(action='saw').inV('thema').outV(kind='book').sum()

Which means :

- Find each book that person4 saw
- Find related thema (and remember a given thema could appear multiple time)
- Find thema related books
- Aggregate to find the most valuable books of the same thema (for person4)


5 - The more (hard disk speed, memory size) you give, the more you'll get
-------------------------------------------------------------------------

I think this is not a surprise for you :)

As a rule of thumb :

- Using faster drive will improve writes a lot (by using a SSD, I did a 5 million node and edges insert in 25 min, where previously a slow hard disk (5400 RPM) saved 25% of the entities after 2h)
- Using more memory will help to have bigger database and will speed up traversal.

In the case of persistent backends like KyotoCabinet, you'll often see very impressive performance (almost as fast as memory backend) IF your database file size is smaller than your RAM : persistent backends often use Memory Map Files for persistence feature.

My 5 million entities database was using 3GB of hard disk space - yup, I'm giving disk space to gain performance (by denormalization) and I have more than 4GB of RAM - in this case, performance was really ok (with indexes, it found any node in less than 50 µs)

In the next part, we'll see how to use GrapheekDB in a client/server configuration

....

Next part : `Part 5 : Production use : client/server configuration <tutorial5.rst>`_


