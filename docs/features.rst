GrapheekDB Features :
=====================

GrapheekDB lets you visualize your data :
---------------------------------------

For further information, you can read : `Tutorial part 1 : Adding and removing nodes and edges <tutorial1.rst>`_


GrapheekDB has lookups :
------------------------

(similar to `Django <https://docs.djangoproject.com/en/dev/ref/models/querysets/>`_ lookups)

- *exact*
- *iexact*
- *contains*
- *icontains*
- *in*
- *gt*
- *gte*
- *lt*
- *lte*
- *startswith*
- *istartswith*
- *endswith*
- *iendswith*
- *isnull*

and lookups can be used on **both** nodes and edges

For further information, you can read : `Tutorial part 2 : Lookup nodes and edges <tutorial2.rst>`_

GrapheekDB has methods for path traversal :
-------------------------------------------

(similar to `Gremlin <https://github.com/tinkerpop/gremlin/wiki>`_ path traversal methods)

- *outV, inV, bothV* (for outer, inner or both vertices)
- *outE, inE, bothE* (for outer, inner or both edges)
- *out_, in_, both_* (for the common pattern : "find a edge of a given kind, then find related outter, inner or both vertices")

For further information, you can read : `Tutorial part 3 : Path traversal, collecting and aggregating data <tutorial3.rst>`_

GrapheekDB has aliasing et collecting methods
---------------------------------------------

An example will be better :

.. sourcecode:: python

    from grapheekdb.backends.data.localmem import LocalMemoryGraph
    g = LocalMemoryGraph()

    # Aliasing and collecting methods
    print g.V(kind__exact='document').aka('X').outV().aka('Y').collect('X', 'Y')

- *aka* ("also known as") allows to give a node (or edge) an alias for further use
- *collect* allows to get a collection given aliases

In the previous example, the last line will :

- find every vertex (node) whose property 'kind' is equal to 'document' (the __exact lookup) - *note*, this could be written : g.V(kind='document')
- give the name (alias) 'X' to each starting vertex iteratively
- find every related outer vertex - which will be aliased 'Y'
- then it will create then return a collection (a Python list) of pair of nodes

For further information, you can read : `Tutorial part 3 : Path traversal, collecting and aggregating data <tutorial3.rst>`_

GrapheekDB has aggregation methods
---------------------------------

**Example 1 : count**

.. sourcecode:: python

    g.V(kind='document').outV().outV().outV().count()

This line will :

- find every vertex (node) whose property 'kind' is equal to 'document'
- do 3 consecutive "outer vertex" traversal
- count the number of final vertices


**Example 2 : sum method**

.. sourcecode:: python

    g.V(kind='document').outV().outV().outV().sum()

This line will :

- find every vertex (node) whose property 'kind' is equal to 'document'
- do 3 consecutive "outer vertex" traversal
- create a dictionnary of *vertex -> number of occurence*

Because, as it stands, the same vertex can be found multiple times

For further information, you can read : `Tutorial part 3 : Path traversal, collecting and aggregating data <tutorial3.rst>`_

GrapheekDB has some syntactic sugars
------------------------------------

The previous example can be written :

.. sourcecode:: python

    g.V(kind='document').outV(3).sum()

"outV(3)" means : "traverse the outer vertices, 3 times"

For further information, you can read : `Tutorial part 3 : Path traversal, collecting and aggregating data <tutorial3.rst>`_


GrapheekDB support nodes and edges indexes :
--------------------------------------------

Example of a IPython session :

.. sourcecode:: python

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

Indexes are *currently* only exact match indexes

For further information, you can read : `Tutorial part 4 : Scaling our app : indexes, performance tips <tutorial4.rst>`_


GrapheekDB can be used stand-alone or in client/server configuration
--------------------------------------------------------------------

- stand alone <=> plug directly to a backend such as LocalMemory or KyotoCabinet
- In a client/server way - a server is provided and a client api is available

For further information, you can read : `Tutorial part 5 : Production use : client/server configuration <tutorial5.rst>`_


GrapheekDB server supports concurrency
--------------------------------------

You can see the `benchmarks <benchmarks.rst>`_
