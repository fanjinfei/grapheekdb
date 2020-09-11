GrapheekDB Tutorial part 1 : Adding and removing nodes and edges
================================================================

`Back to documentation index <index.rst>`_

....

Let’s learn by example
----------------------

Throughout this tutorial, we’ll walk you through the creation of a :

**Basic collaborative filtering recommandation engine for an online book shop**

We’ll assume you have GrapheekDB installed already. You can tell GrapheekDB is installed and which version by running the following command in a shell:

.. sourcecode:: bash

    python -c "import grapheekdb; print(grapheekdb.__version__)"

If GrapheekDB is installed, you should see the version of your installation. If it isn’t, you’ll get an error telling “No module named grapheekdb”.

See `How to install GrapheekDB <install.rst>`_ if it is not installed

Things you should do before starting this tutorial
--------------------------------------------------

(but don't absolutely need)

- You should create a `virtual environment <https://pypi.python.org/pypi/virtualenv>`_ dedicated to this tutorial
- (Strongly recommended) install a `recent IPython <https://pypi.python.org/pypi/ipython>`_ in this virtual environment, this will allow you to run IPython Notebooks and visualize your graphs
- Start a new notebook, type in a shell:

.. sourcecode:: bash

    ipython notebook

- *This will open a browser with a listing of notebooks*
- Create a new notebook
- In the first cell, type:

.. sourcecode:: bash

    %install_ext https://raw.github.com/cjdrake/ipython-magic/master/gvmagic.py

- *This will install Chris Drake Graphviz Magics Ipython Extension*
- *This needs to be done once*
- After that, enable the extension by typing in a new cell:

.. sourcecode:: bash

    %load_ext gvmagic

This will allow you to vizualise graphs

Let's add some books
--------------------

**But first, create a new graph**

.. sourcecode:: python

    from grapheekdb.backends.data.localmem import LocalMemoryGraph
    g = LocalMemoryGraph()

**Add books**

.. sourcecode:: python

    book1 = g.add_node(kind='book', name='python tutorial', author='tim aaaa', thema='programming')
    book2 = g.add_node(kind='book', name='cooking for dummies', author='tom bbbb', thema='cooking')
    book3 = g.add_node(kind='book', name='grapheekdb', author='raf cccc', thema='programming')
    book4 = g.add_node(kind='book', name='python secrets', author='tim aaaa', thema='programming')
    book5 = g.add_node(kind='book', name='cooking a python', author='tom bbbb', thema='cooking')
    book6 = g.add_node(kind='book', name='rst the hard way', author='raf cccc', thema='documentation')

Let's explain what we did so far :

- We add nodes using the graph .add_node method
- Each node has some properties (kind/name/author/thema)
- For your information, some books could have more properties, or less, or even none : GrapheekDB is schemaless.

If, as recommended, you use IPython and GraphViz Magics extension, you can see the result by typing in a cell :

.. sourcecode:: python

    %dotobj g.V().dot('name')

It should look like :

.. image:: /img/graph1.png
   :width: 100%

Let's explain the code line :

- %dotobj is an IPython magic clause (which was "imported" when we loaded gvmagic extension)
- g is the graph
- g.V() represents all the vertices (other name for "nodes")
- (**advanced** : g.V() is a python iterator, I will call it 'entity iterator' in the next parts)
- g.V().dot() returns an object than can be used by %dotobj to draw the graph
- g.V().dot('name') means that we want to use the 'name' property as a label for nodes
- you can try using something else than 'name' such as *nothing* or 'author' or 'thema'
- when no arguments is provided to the .dot method, you will see the node internal ids - those ids are automatically created for you and are used internally by GrapheekDB

Now let's add persons
---------------------

.. sourcecode:: python

    person1 = g.add_node(kind='person', name='sam xxxx')
    person2 = g.add_node(kind='person', name='sam xxxx')
    person3 = g.add_node(kind='person', name='sam xxxx')
    person4 = g.add_node(kind='person', name='sam xxxx')

Now the graph looks like :

.. image:: /img/graph2.png
   :width: 100%

*Note for readers : I know this is bit small, but it will be more readable when we'll start to filter nodes or create edges*

First edges : buyers
--------------------

It appeared that some persons bought some books, let's save this information in the database :

.. sourcecode:: python

    g.add_edge(person1, book1, action='bought')
    g.add_edge(person1, book3, action='bought')
    g.add_edge(person1, book4, action='bought')
    g.add_edge(person2, book2, action='bought')
    g.add_edge(person2, book5, action='bought')
    g.add_edge(person3, book1, action='bought')
    g.add_edge(person3, book3, action='bought')
    g.add_edge(person3, book5, action='bought')
    g.add_edge(person3, book6, action='bought')

Now the graph looks like :

.. image:: /img/graph3.png
   :width: 100%

So far, person4 didn't buy any books but it appears he saw the 1st book, let's add this information :

.. sourcecode:: python

    g.add_edge(person4, book1, action='saw')

Hmmm, I'd like to see which actions persons did

.. sourcecode:: python

    %dotobj g.V().dot('name', 'action')

This gives :

.. image:: /img/graph4.png
   :width: 100%

**That's cool, isn't it ? :)**

Let's explain what g.V().dot('name', 'action') did :

- The .dot method can take up to 3 arguments
- The first argument defines the node property that must be used for nodes label
- The second argument defines the edge property that must be used for edges label
- There's a 3rd argument : limit, it defines how much entities should be displayed. The default is 100.

And now for something completely different
------------------------------------------

So far, we see how to add nodes and edges, but sometimes we need to remove them

This is easy :

.. sourcecode:: python

    # Just adding a node to remove it immediately :
    you_dont_know_it_but_you_re_already_dead_node = g.add_node(foo=1)
    # Remove it :
    you_dont_know_it_but_you_re_already_dead_node.remove()

Part 1 summary :
----------------

- We created a graph
- We learned how to add nodes
- We learned how to add edges
- We learned how to see graph
- We learned how to choose label for nodes and edges vizualisation.
- we learned how to remove a node

But hey ! the goal is to recommend a book to new user !

- Well, now we have a person (person4) who saw a book, we will see in next tutorial parts how we can achieve this <-> recommend him a book

....

Let's go to next part : `Part 2 : Lookup nodes and edges <tutorial2.rst>`_
