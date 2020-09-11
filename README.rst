==========
GrapheekDB
==========

Getting started :
-----------------

- `Documentation index </docs/index.rst>`_
- `Quick install guide </docs/install.rst>`_
- `GrapheekDB features </docs/features.rst>`_
- `Intro : What is a graph database ? </docs/graph_database.rst>`_

Tutorial :
----------

- `Part 1 : Adding and removing nodes and edges </docs/tutorial1.rst>`_
- `Part 2 : Lookup nodes and edges </docs/tutorial2.rst>`_
- `Part 3 : Path traversal, collecting and aggregating data </docs/tutorial3.rst>`_
- `Part 4 : Scaling our app : indexes, performance tips </docs/tutorial4.rst>`_
- `Part 5 : Production use : client/server configuration </docs/tutorial5.rst>`_

Links:
------

- Home Page : https://bitbucket.org/nidusfr/grapheekdb

Tests and documentation :
-------------------------

code base test coverage is currently : 99 %

**documentation is a work in progress**


Backends :
----------

It can use various KVS (Key/Value Store) backends :

- Local memory (default backend - not transactionnal)
- Kyoto Cabinet : http://fallabs.com/kyotocabinet/
- Symas LMDB : http://symas.com/mdb/
- Sqlite3 - not transactionnal - graph can become inconsistent
- Shelve - not transactionnal - graph can become inconsistent

Known issues :
--------------

There's an issue when running all tests as it opens a lot of files

You may need to raise limits before running tests.

If you want to raise those limits permanently, on Linux, you'll need to edit /etc/security/limits.conf and add those lines :

	username soft nofile 8192
	username hard nofile 8192

- Change username to fit your needs
- 8192 appeared to be enough for tests to run

Then, depending on your linux distribution, you may need to edit /etc/pam.d/login to add :

	session required /lib/security/pam_limits.so

About me :
----------

I'm a freelance working with Python and Django since 2000.
You can find more information on my Website : `Nidus <http://www.nidus.fr/>`_ (In French)

If you have interesting projects (involving GrapheekDB or not), don't hesitate to contact me :)

See you

Contributors :
--------------

- Edwin Cox (sqlite3 and shelve backends)
- Pasdavoine (bug tracking and fixing)