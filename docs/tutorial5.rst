Part 5 : Production use : client/server configuration :
=======================================================

Previous part : `Part 4 : Getting some performances with indexes <tutorial4.rst>`_

....

So far, we saw how to use a pure in-memory graph database and for information, this graph database was running in the same process as our scripts

**In production, this is not a good choice**, we need a server to :

- separate clients processes (or threads) from server process
- get some multi-tenancy

Well, I have two good news :

- A server is included
- You won't have to learn new way to interact with the graph database

It is quite easy to launch the server, just type in a **Linux Shell** :

.. sourcecode:: bash

    grapheekserve

Which will output :

.. sourcecode:: bash

    GrapheekDB Server version X.Y.Z
    ===============================
    Address              : tcp://127.0.0.1:5555  <--  use the same address in client
    Backend              : grapheekdb.backends.data.localmem.LocalMemoryGraph
    Params               : {}
    Server scripts paths :
    Quit the server with CONTROL-C.

This gives interesting information :

- it runs on TCP, port 5555 (TCP, not HTTP, so don't try to use your browser to navigate to this port, it will probably mess your server)
- it uses the LocalMemory backend which means that **everything** will be removed when you will stop the sever - other backends can be used, cf `backends <backends.rst>`_
- it has no params (params are used to initiate backends - in the case of local memory backend, no additionnal parameter is needed)
- there is no additionnal `server scripts <server_scripts.rst>`_

You can get more info on server by launching :

.. sourcecode:: bash

    $ grapheekserve --help

Which outputs :

.. sourcecode:: bash

    usage: grapheekserve [-h] [-a ADDRESS] [-b BACKEND] [-c CONFIG] [-s SCRIPTS]
                         [params [params ...]]

    Run GrapheekDB server.

    positional arguments:
      params                Data backend parameters. must be of the form :
                            "key1:value1 key2:value ..." (default: )

    optional arguments:
      -h, --help            show this help message and exit
      -a ADDRESS, --address ADDRESS
                            The address string. This has the form
                            'protocol://interface:port', for example
                            'tcp://127.0.0.1:5555'. Protocols supported include
                            tcp, inproc and ipc. (default: tcp://127.0.0.1:5555)
      -b BACKEND, --backend BACKEND
                            Data backend. must be of the form (for instance) :
                            "grapheekdb.backends.data.kyotocab.KyotoCabinetGraph"
                            (default:
                            grapheekdb.backends.data.localmem.LocalMemoryGraph)
      -c CONFIG, --config CONFIG
                            Server configuration file (default: None)
      -s SCRIPTS, --scripts SCRIPTS
                            Server side scripts module path. Must be of the form :
                            path.to.my_module1.scripts:path.to.my_module2.scripts:
                            [.. additional module path ..] (default: )


Then, in a Python shell OR a IPython Notebook, you can establish connection to the server by doing :

.. sourcecode:: bash

    from grapheekdb.client.api import ProxyGraph
    g = ProxyGraph('tcp://127.0.0.1:5555')

And, here it is, you're in !

**Everything** we did in tutorial `part1 <tutorial1.rst>`_, `part2 <tutorial2.rst>`_, `part3 <tutorial3.rst>`_ & `part4 <tutorial4.rst>`_ can be done with this "ProxyGraph" (as ensured by tests which checks "isomorphism" of ProxyGraph and other backend graphs)



Security issues :
-----------------

For information, GrapheekDB uses 0mq (aka ZeroMQ or ZMQ) to handle communications between client and server.

You must know that **there's currently no authentification mechanism in GrapheekDB** between client and server : every client that can access the TCP port can send requests to the server.

**So be sure to setup up your firewall to be sure that only the client can access the server**

In the future, there may be some kind of authentification (or recent ZMQ enhancements could be used for this), but this doesn't remove previous warnings.

Persistent backends :
---------------------

This has not been said or even viewed so far, but **GrapheekDB is mainly targeted** to be used with a **persistent backend** such as KyotoCabinet or Lmdb - "wrappers" are included.

=> You can get more information on backends and how to use them in `backends <backends.rst>`_ page

**Use LocalMemory backend only for prototyping**

You may not be interested in persistence, but for information, GrapheekDB uses backend transactions to ensure consistency of every operations such as node/edge addition/removal, index creation ... **in the case of the LocalMemory backend, there's currently no transactions, so, the backend will eventually get corrupted**.

Moreover, as you'll probably see, **both KyotoCabinet and Lmdb are quite fast** (at least for read operations)

Concurrency :
-------------

You can find a example concurrent load test in `examples/loadtest/locustfile.py <https://bitbucket.org/nidusfr/grapheekdb/src/default/examples/loadtest/locustfile.py>`_

- It uses http://locust.io/ load testing framework
- It only writes (no explicit read) in the database to add or remove nodes - so, it is quite intensive

I achieved to get an average 7000 requests/sec with 1000 concurrent users.

Your experience may vary :)

....

`Back to documentation index <index.rst>`_
