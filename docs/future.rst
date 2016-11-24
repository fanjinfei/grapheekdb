    **Disclaimer : work in progress**

Upcoming releases :
===================

From 0.0.1 to 0.1.0, GrapheekDB will be in alpha stage, which means :

- keys tree structure may be modified
- method signature could change
- new methods and features will be added (depending on my personnal needs for a project of mine)
- documentation will be a work in progress.

From 0.1.0 to 0.4.0, GrapheekDB will also be in alpha stage, which means :

- keys tree structure may be modified
- method signature could change
- BUT, migration from an old version to a new version will be possible by :
	- dumping the old data
	- reloading in new server

From 0.5.0 to 0.9.0, GrapheekDB will be in beta stage, which means :

- keys tree structure should be stable (thus removing the need of migrating data)
- method signature could change
- new clauses and features will be added

0.2.0 :
-------

- unicity constraints (on indexed fields)
- more doc/more examples

0.3.0 :
-------

- adding new backends (?)

0.4.0 :
-------

- new clauses (?)

0.5.0 :
-------
*Beta release*

- Data structure freeze
- Read/Write gml (?)
- Read/Write graphml (?)
- Graphical frontend (so that Grapheek can become graphic :p)


[...]


1.0.0 :
-------
*Stable release*

- API freeze
- Removing server single point of failure by implementing a consensus protocol such as Raft or Paxos (?)
- Range indexes (?)