GrapheekDB Tutorial part 3 : Path traversal, collecting and aggregating data
============================================================================

Previous part : `Part 2 : Lookup nodes and edges <tutorial2.rst>`_

....

So far, in part 1 & 2, we learned the basics :

- Node/Edge creation & deletion
- Node/Edge filtering
- Graph vizualisation
- Iteration on nodes and edges

In this part, we'll see more advanced features : path traversal.

In other words, we'll learn the  :

**pathology of graph** :p

This can make you feel a bit strange :D

Let's walk on nodes and edges :
-------------------------------

Let's say, I want to know which actions person4 did

With path traversal, this is easy :

.. sourcecode:: python

    for edge in person4.outE():
        print edge.action

This shows :

.. sourcecode:: python

    saw

Let's explain this :

- We start from person4
- We get an edge iterator on outer edges : .outE()
- We iterate on edges and print edge's action

So currently, we start from person4, we are on the road of his actions, but we didn't reach our "destination".

Or, to say it in other words : "ok, he saw something, but what ?"

Let's use another path traversal method to get this information :

.. sourcecode:: python

    for node in person4.outV():
        print node.name, '|', node.thema, '|', node.author

Which shows :

.. sourcecode:: python

    python tutorial | programming | tim aaaa

Ok, now, we have more information about which books could be of interest for person4 :

- books about Python
- books about programming
- books from tim aaaa

Using part2 knowledge, we could use lookups to find every books with 'programming' thema :

.. sourcecode:: python

    for node in g.V(kind='book', thema='programming'):
        print node.name, '|', node.thema, '|', node.author

Which outputs :

.. sourcecode:: python

    python tutorial | programming | tim aaaa
    grapheekdb | programming | raf cccc
    python secrets | programming | tim aaaa

Now, we just need to exclude the book he already saw and here it is : a very simple first recommendation engine (So simple that it wouldn't work "as is" in real book shop where you can have several millions of books)

It uses properties of a "entity" (in our case : a book) to find similar entities

This is a (tiny) **content based recommendation** engine

Path traversal also works when using a entity iterator :
--------------------------------------------------------

In previous section, we started from a node (person4), but we can also traverse path starting from an *entity iterator* (~ a list of nodes or edges).

Example :

.. sourcecode:: python

    %dotobj g.V(kind='person').outV().dot('name')

.. image:: /docs/img/graph1.png
   :width: 100%

Hum, this is not very interesting : we got the very same image as part1/1st image

By the way, let's explain what was done :

- We start from **every** node of kind 'person'
- We traverse paths to go to outer vertices (aka nodes)
- We visualize the graph using 'name' property as node label

Let's try something more complicated :

.. sourcecode:: python

    %dotobj g.V(kind='person', name__contains='m').outE(action='bought').dot('name', 'action')

.. image:: /docs/img/graph10.png
   :width: 80%

There's more to explain for this example :

- We started from *every nodes* of kind 'person' and whose name contains a 'm'
- We walk on outer edges whose action is 'bought' : yes, we can use **filter clauses in path traversal methods**
- g.V(kind='person', name__contains='m').outE(action='bought') is an entity iterator - and in this case an edge iterator
- Thus, we can see the result by using .dot('name', 'action') which shows the edges.
- The nodes are also displayed because seeing edges without source and target nodes wouldn't make sense.

So, so far, we know :

- a_node.outE() is an edge iterator
- g.V(...) is a node iterator
- g.E(...) is an edge iterator
- g.V(...).outV(...) is a node iterator
- g.V(...).outE(...) is a edge iterator

So :

**We can traverse path starting either from a entity (a node or edge) or from an entity iterator**

For information, there are 6 basic path traversals :

- *outV* traverses outer vertices - it can be applied on nodes, edges, nodes iterators & edges iterators
- *inV* traverses inner vertices - it can be applied on nodes, edges, nodes iterators & edges iterators
- *bothV* traverses both vertices - it can be applied on nodes, edges, nodes iterators & edges iterators
- *outE* traverses outer edges - it can be applied on nodes & nodes iterators **only**
- *inE* traverses inner edges - it can be applied on nodes & nodes iterators **only**
- *bothE* traverses both edges - it can be applied on nodes & nodes iterators **only**

=> Don't hesitate to "play" with each path traversal method to "feel" what it does...

For information, there's 3 more "advanced" path traversal :

- out\_
- in\_
- both\_

Those are *"syntactic sugars"* for a common path traversal pattern that we'll see few lines below

We can "chain" path traversals
------------------------------

I guess you already guessed (or even test) it, here is an example :

.. sourcecode:: python

    %dotobj person4.outE(action='saw').outV().inE(action='bought').inV().dot('name')

.. image:: /docs/img/graph11.png
   :width: 30%

This example is far more interesting than the previous ones as it brings us not far away from the "collaborative filtering recommendation engine" :

- it starts from a person that haven't bought any book so far (person4)
- it gets every books he saw (person4.outE(action='saw').outV())
- it gets every person that already bought those books (person4.outE(action='saw').outV().inE(action='bought').inV())

In other words : **we found persons that are similar (in taste) to person4**

Before going to next part (**the** engine), let's show that GrapheekDB allows you to write previous query in a shorter way :

.. sourcecode:: python

    person4.outE(action='saw').outV().inE(action='bought').inV()

can be written :

.. sourcecode:: python

    person4.out_(action='saw').in_(action='bought')

Why this "shortcut" ?

In graph database, this "subtraversal" : .outE(attr1=value1, attr2=value2, ...).outV() is a **very common** pattern, because you'll often find yourself looking for edges with given attributes, then get "post-traversal" nodes.

So, I decided to write "advanced" methods to avoid typing so much text :)

There are 3 advanced methods :

- out_(attr1=value1, attr2=value, ...) is a shortcut for : outE(attr1=value1, attr2=value, ...).outV()
- in_(attr1=value1, attr2=value, ...) is a shortcut for : inE(attr1=value1, attr2=value, ...).inV()
- both_(attr1=value1, attr2=value, ...) **is NOT a shortcut for** : bothE(attr1=value1, attr2=value, ...).bothV(), *it wouldn't give you what you would intuitively expect*... in fact, it is a shortcut for (pseudo code) : outE(attr1=value1, attr2=value, ...).outV() **+** inE(attr1=value1, attr2=value, ...).inV()

Here it is : 1st collaborative filtering recommendation engine
--------------------------------------------------------------

So far, we have similar users, now, it's easy to get recommended books by doing one more traversal :

.. sourcecode:: python

    %dotobj person4.out_(action='saw').in_(action='bought').out_(action='bought').dot('name')

Which gives :

.. image:: /docs/img/graph12.png
   :width: 100%

Previous query gives us "similar users", adding .out_(action='bought') gives us books that were bought by those similar users.

So, it's almost done, one step further would be to iterate over those similar books and show each book that has not been seen by person4.

But, that's not what we will do : each recommended book would have the same "interest", which is not the case in "real world" where some books will be more "valuable" for visitors

2nd try : pimp my recommendation engine
---------------------------------------

Before going further, let's introduce a new entity iterator method : .count()

Here is some examples :

**Example 1 : node count**

.. sourcecode:: python

    print g.V().count()

outputs :

.. sourcecode:: python

    10

**Example 2 : filtered node count**

.. sourcecode:: python

    print g.V(kind='book').count()

outputs :

.. sourcecode:: python

    6

**Let's try this new iterator method on our recommandation query :**

.. sourcecode:: python

    print person4.out_(action='saw').in_(action='bought').out_(action='bought').count()

Which should output :

.. sourcecode:: python

    7

At this step, you may think **"this graph database is totally crappy"** because the previous image showed us :

.. image:: /docs/img/graph12.png
   :width: 100%

And obviously, we can only see 5 nodes and not 7 !!!

Well, in fact, **this is not a bug, this is a feature**, let's see again the complete graph and our query :

**Graph :**

.. image:: /docs/img/graph4.png
   :width: 100%

**Query :**

.. sourcecode:: python

    person4.out_(action='saw').in_(action='bought').out_(action='bought')

I will "manually" traverse some path :

- I start from person4 (joe zzzz)
- I go to "python tutorial"
- I go to "sam wwww"
- I go to "grapheekdb"

another one :

- I start from person4
- I go to "python tutorial"
- I go to "luc yyyy"
- I go to "grapheekdb"

HEY, "grapheekdb" book has been reached two times !

By trying another paths, you'll see that :

- it is also the case for "python tutorial"
- the 3 other nodes are reached only once

Ok, so now, the "7" result can be explained :

- "grapheekdb" is reached 2 times
- "python tutorial" is reached 2 times
- "python secrets" is reached 1 time
- "rst the hard way" is reached 1 time
- "cooking a python" is reached 1 time

And 2 + 2 + 1 + 1 + 1 = 7

To see it even more clearly, instead of vizualising the graph, we can iterate and print book's names :

.. sourcecode:: python

    for book in person4.out_(action='saw').in_(action='bought').out_(action='bought'):
        print book.name

which outputs :

.. sourcecode:: python

    python tutorial
    grapheekdb
    python secrets
    python tutorial
    grapheekdb
    cooking a python
    rst the hard way



This is a **really important result** as it will allow us to sort "final nodes" by using number of occurence for each book, and you know what ? there's a method for that :

.. sourcecode:: python

    for item in person4.out_(action='saw').in_(action='bought').out_(action='bought').sum():
        print item

which should output :

.. sourcecode:: python

    (<node id:0 data:{'kind': 'book', 'name': 'python tutorial', 'thema': 'programming', 'author': 'tim aaaa'}>, 2)
    (<node id:2 data:{'kind': 'book', 'name': 'grapheekdb', 'thema': 'programming', 'author': 'raf cccc'}>, 2)
    (<node id:3 data:{'kind': 'book', 'name': 'python secrets', 'thema': 'programming', 'author': 'tim aaaa'}>, 1)
    (<node id:4 data:{'kind': 'book', 'name': 'cooking a python', 'thema': 'cooking', 'author': 'tom bbbb'}>, 1)
    (<node id:5 data:{'kind': 'book', 'name': 'rst the hard way', 'thema': 'documentation', 'author': 'raf cccc'}>, 1)

Explain this :

- .sum() returns on ordered (descending on occurence number) list of pair
- 1st item of the pair is an entity (in our case : a node)
- 2nd item of the pair is the number of occurences

As is, previous output gave *too much information*, let's write code again to get a simpler output :

.. sourcecode:: python

    for book, occ in person4.out_(action='saw').in_(action='bought').out_(action='bought').sum():
        print book.name, '-->', occ, 'occurence(s)'

which should output :

.. sourcecode:: python

    python tutorial --> 2 occurence(s)
    grapheekdb --> 2 occurence(s)
    python secrets --> 1 occurence(s)
    cooking a python --> 1 occurence(s)
    rst the hard way --> 1 occurence(s)

Let's finalize our recommendation engine
----------------------------------------

Let's write some helper functions :

.. sourcecode:: python

    def get_or_create_person(g, name):
        try:
            person = g.V(name=name, kind='person').next()  # next iterator item
        except StopIteration:
            person = g.add_node(kind='person', name=name)
        return person

    def person_saw_book(g, person, book):
        # I assume person and book are nodes
        g.add_edge(person, book, action='saw')

Implement our algorithm :

.. sourcecode:: python

    def recommend_books(person, count=5):
        return person.out_(action='saw').in_(action='bought').out_(action='bought').sum()[:count]

Now, each time a new user registers our online book shop, we create a node for him :

.. sourcecode:: python

    get_or_create_person(g, name)

Each time, he visits a book page, save this information :

.. sourcecode:: python

    person_saw_book(g, person, book)

And immediately trigger the recommendation algorithm :

.. sourcecode:: python

    recommended_books = recommend_books(person)
    for book, _ in recommended_books
        # display book with your favorite Web Framework [...]

**To go further**

There are some cases where you don't want to get duplicated entities in results, in those cases, just .dedup :

.. sourcecode:: python

    for book in person4.out_(action='saw').in_(action='bought').out_(action='bought').dedup():
        print book

If we just want to get properties, we can write :

.. sourcecode:: python

    print person4.out_(action='saw').in_(action='bought').out_(action='bought').dedup().data()

Try it...

And there are other cases where you are not interested in getting number of occurences : you just want a "probability" (or proportion) of occurence. In this case, you can use .percent method instead of .sum method :

.. sourcecode:: python

    print person4.out_(action='saw').in_(action='bought').out_(action='bought').percent()

And now for something completely different
------------------------------------------

There's something more we can do during path traversal : we can give aliases to some "steps".

I'll use the base query of the recommandation engine :

.. sourcecode:: python

    person4.out_(action='saw').in_(action='bought').out_(action='bought')

And add some **aliases** :

.. sourcecode:: python

    person4.aka('x').out_(action='saw').in_(action='bought').aka('y').out_(action='bought').aka('y')

(aka stands for "also known as")

If we run this code, it seems nothing changed, but let's **add the .collect method** :

.. sourcecode:: python

    person4.aka('x').out_(action='saw').in_(action='bought').aka('y').out_(action='bought').aka('y').collect('x', 'y', 'z')

And let's iterate over the result :

.. sourcecode:: python

    for original_person, similar_person, book in person4.aka('x').out_(action='saw').in_(action='bought').aka('y').out_(action='bought').aka('z').collect('x', 'y', 'z'):
        print original_person.name, '- is similar to -', similar_person.name, '- who bought -', book.name

this outputs :

.. sourcecode:: python

    joe zzzz - is similar to - sam wwww - who bought - python tutorial
    joe zzzz - is similar to - sam wwww - who bought - grapheekdb
    joe zzzz - is similar to - sam wwww - who bought - python secrets
    joe zzzz - is similar to - luc yyyy - who bought - python tutorial
    joe zzzz - is similar to - luc yyyy - who bought - grapheekdb
    joe zzzz - is similar to - luc yyyy - who bought - cooking a python
    joe zzzz - is similar to - luc yyyy - who bought - rst the hard way

I think you'll find those features of 'aliasing' and 'collecting' quite useful:

- It allows us to collect data during our traversals
- The resulting list helps to understand why our recommendation engine is working :)
- And you can also see that GrapheekDB is doing "depth first search" (DFS)

Part 3 summary :
----------------

- We learned graph pathology
- We saw how to implement several recommendation engine by using path traversal, sum and percent methods

The next parts will show :

- How to scale our app by using indexes and performance tips (`part 4 <tutorial4.rst>`_)
- How to use GrapheekDB in a client/server configuration (`part 5 <tutorial5.rst>`_)

....

Next part : `Part 4 : Scaling our app : indexes, performance tips <tutorial4.rst>`_
