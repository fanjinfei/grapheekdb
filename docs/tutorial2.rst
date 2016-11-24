GrapheekDB Tutorial part 2 : Lookup nodes and edges
===================================================

Previous part : `Part 1 : Adding and removing nodes and edges <tutorial1.rst>`_

....

In this second part, we will learn how to :

- iterate over nodes or edges
- access node or edge properties
- filter nodes
- filter edges

In the previous part, we saw how to vizualise a graph :

.. sourcecode:: python

    %dotobj g.V().dot('name')

But in fact, the most important part of this line is :

.. sourcecode:: python

    g.V()

This gives us an access to **all** nodes (aka vertices) in graph

g.V() returns a Python iterator, let's iterate over it :

.. sourcecode:: python

    for node in g.V():
        print node

It should return :

.. sourcecode:: python

    <node id:0 data:{'kind': 'book', 'name': 'python tutorial', 'thema': 'programming', 'author': 'tim aaaa'}>
    <node id:1 data:{'kind': 'book', 'name': 'cooking for dummies', 'thema': 'cooking', 'author': 'tom bbbb'}>
    <node id:2 data:{'kind': 'book', 'name': 'grapheekdb', 'thema': 'programming', 'author': 'raf cccc'}>
    <node id:3 data:{'kind': 'book', 'name': 'python secrets', 'thema': 'programming', 'author': 'tim aaaa'}>
    <node id:4 data:{'kind': 'book', 'name': 'cooking a python', 'thema': 'cooking', 'author': 'tom bbbb'}>
    <node id:5 data:{'kind': 'book', 'name': 'rst the hard way', 'thema': 'documentation', 'author': 'raf cccc'}>
    <node id:6 data:{'kind': 'person', 'name': 'sam wwww'}>
    <node id:7 data:{'kind': 'person', 'name': 'tim xxxx'}>
    <node id:8 data:{'kind': 'person', 'name': 'luc yyyy'}>
    <node id:9 data:{'kind': 'person', 'name': 'joe zzzz'}>

Let's access nodes properties :
-------------------------------

.. sourcecode:: python

    for node in g.V():
        print node.kind, node.name

It should return :

.. sourcecode:: python

    book python tutorial
    book cooking for dummies
    book grapheekdb
    book python secrets
    book cooking a python
    book rst the hard way
    person sam wwww
    person tim xxxx
    person luc yyyy
    person joe zzzz

Let's explain what we did :

- We iterated over g.V() (ie all vertices)
- We access node properties directly (as if it has always been there) and print them

For information, there's another way to access **all** node (or edge) properties : the .data method :

.. sourcecode:: python

    for node in g.V():
        print node.data()

=> Try it

The following code could even be written in a shorter way :

.. sourcecode:: python

    print g.V().data()  # the .data method can also be used on entity iterators


This lines of code will allow us to write :

A naive node filter
-------------------

Let's say we'd like to find all books in node, we can write :

.. sourcecode:: python

    for node in g.V():
        if node.kind == 'book':
            print node.name

This works and returns :

.. sourcecode:: python

    python tutorial
    cooking for dummies
    grapheekdb
    python secrets
    cooking a python
    rst the hard way

*But* :

- it needs 3 lines to create a very simple filter
- it iterates over every nodes (even those which are not books) to find books

There's a better way to do this

Filtering nodes the right way :
-------------------------

GrapheekDB allows to use lookups (similarly to Django Lookup), you only need to pass keywords argument to .V method.

Here is an example :

.. sourcecode:: python

    for node in g.V(kind='book'):
        print node.name

This also returns :

.. sourcecode:: python

    python tutorial
    cooking for dummies
    grapheekdb
    python secrets
    cooking a python
    rst the hard way

This is **the right way** to filter :

- it's shorter
- it returns an iterator (that can be used for path traversal as we'll see in part 3)
- it benefits from existing indexes (more on this later)

As it is an iterator - like g.V() - we can use .dot method to vizualise the graph :

.. sourcecode:: python

    %dotobj g.V(kind='book').dot('name')

.. image:: https://bitbucket.org/nidusfr/grapheekdb/raw/default/docs/img/graph1.png
   :width: 100%

Let's try with persons :

.. sourcecode:: python

    %dotobj g.V(kind='person').dot('name')


.. image:: https://bitbucket.org/nidusfr/grapheekdb/raw/default/docs/img//graph5.png
   :width: 50%

Let's try to add a filter on another field :

.. sourcecode:: python

    %dotobj g.V(kind='book', thema='programming').dot('name')

.. image:: https://bitbucket.org/nidusfr/grapheekdb/raw/default/docs/img//graph6.png
   :width: 50%

Lookups can be more complex :

.. sourcecode:: python

    %dotobj g.V(name__contains='python').dot('name')

.. image:: https://bitbucket.org/nidusfr/grapheekdb/raw/default/docs/img//graph7.png
   :width: 50%

Let's try something else : we will look for every name that contains 'a', whatever kind it is :

.. sourcecode:: python

    %dotobj g.V(name__contains='a').dot('name')

.. image:: https://bitbucket.org/nidusfr/grapheekdb/raw/default/docs/img//graph8.png
   :width: 60%

Hey, what happens ? Why do we see some edges ? We only wanted nodes...

**-> GrapheekDB, with the .dot method, shows edges that connects 2 nodes if those 2 nodes are in the collection returned by the iterator**

Which implicitely, means that in previous graphs, there was no connection (no edge) between nodes.

Filtering works for edges, too :
--------------------------------

So far, we mainly focused on nodes, but in fact, it is also possible to filter on edges :

.. sourcecode:: python

    %dotobj g.E(action='saw').dot('name', 'action')

.. image:: https://bitbucket.org/nidusfr/grapheekdb/raw/default/docs/img//graph9.png
   :width: 20%

Isn't that cool ?

In one line, we can visualize who saw what ...

With *Path traversal* (next tutorial part), we'll have even more POWER !!!

And now for something completely different
------------------------------------------

The remove method that we saw in previous part can also be used on a entity iterator with lookup.

Example :

.. sourcecode:: python

    %dotobj g.V(kind='something_else').remove()  # This kind of node doesn't exist : I don't want to remove nodes that we'll need in next parts



Part 2 summary :
----------------

- We learned how to access node properties
- We used node properties to implement a simple (naive) filter
- We learned how to filter nodes
- We learned how to filter edges
- We learned how to remove entities in "batch" with .remove() method

If you need more information, you can see a list of lookup in the GrapheekDB `features list <features.rst>`_

BUT, we still didn't implement the recommendation engine !!!

Well, you're lucky, we'll see how to implement it in the next part :)

....

Next part : `Part 3 : Path traversal, collecting and aggregating data <tutorial3.rst>`_
