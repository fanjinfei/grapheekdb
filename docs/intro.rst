Quick introduction :
--------------------

- Run the server :

.. sourcecode:: bash

    grapheekserve

- In a Python Shell :

.. sourcecode:: python

    # Connect to the server :
    from grapheekdb.client.api import ProxyGraph
    g = ProxyGraph('tcp://127.0.0.1:5555')

    # Print current vertices count
    print g.V().count()

    # Add some nodes
    documents = []
    for i in range(1000):
        # Arbitrary number of attributes can be added
        documents.append(g.add_node(document_id=i, label='Document'))

    persons = []
    for i in range(100):
        # There's no mandatory fields, you could even add nodes without fields...
        persons.append(g.add_node(person_id=i))


    # Print current vertices count
    print g.V().count()

    # Do a simple lookup
    print g.V(document_id=500).count()

    # more complex Lookups - Django style
    print g.V(document_id__gte=500, document_id__lt=510).count()
    print g.V(label__isnull=True).count()

    # Get node map (<-> attributes) :
    node = documents[500]
    print node.data()

    # This also works with lookup :
    print g.V(document_id__gte=500, document_id__lt=510).data()

    # Basic indexing (currently GrapheekDB only supports exact match index) :
    g.add_node_index('document_id')
    print g.V(document_id=500).count()  # this should be faster

    # Basic indexing on multiple fields :
    # this index will speed up queries like :
    # g.V(document_id=500, label='Document')
    # BUT not :
    # g.V(document_id=500) OR g.V(label='Document')
    g.add_node_index('document_id', 'label')

    # Remove indexes :
    g.remove_node_index('document_id')
    g.remove_node_index('document_id', 'label')

    ################ TRAVERSAL ################################

    # First, let's add some edges :
    from random import choice

    for i in range(5000):
        person = choice(persons)
        document = choice(documents)
        # I could also use "label" instead of "action", or foo, or nothing
        # There's no minimal number of fields (well... yes there is : O) or maximum number of fields (if you have enough memory/hard disk space)
        g.add_edge(person, document, action='read')

    # Let's take a random person (hopefully he read some documents) :
    person = choice(persons)

    # 1st traversal : let's answer the question : "how much documents did this person read ?"
    print person.outV().count()

    # Which documents :
    print person.outV().data()

    # The base of a "collaborative" recommandation algorithm :
    # How many persons did read the same documents as this person :
    print person.outV().inV().count()  # you're currently walking forward, then backwards on the graph

    # Get only 20 of them :
    similar_persons = person.outV().inV().limit(20)

    # Who are they ?
    print similar_persons.data()

    # SO FAR, we walked starting from a person, let's start from a document :
    document = choice(documents)

    # Who read this document :
    print document.inV().data()

    ########## MORE EXAMPLES COMING :) #########
    # -> Inference
    # -> Naive page rank
