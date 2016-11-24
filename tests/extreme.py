#!/usr/bin/env
# -*- coding: utf-8 -*-

"""
The tests in this module are "extreme" tests that will :
- give bad time to your hard disk
- takes a long time

They are not targeted to be run in the "normal" test suite
Thus, this file's name does not contain "test" so that nosetests don't detect it as a test module

But, if you want, you can explicitely call it :
nosetests tests/extreme.py --nocapture

If you want to profile :
nosetests tests/extreme.py --with-profile --nocapture
"""

import tempfile

from grapheekdb.backends.data.keys import CHUNK_SIZE


class CommonExtreme(object):

    def __check_with_size(self, count):
        supernode = self.graph.add_node()  # "Supernode" = node with a LOT of edges
        edge_defns = []
        for i in range(count - 1):
            node = self.graph.add_node()
            edge_defns.append((node, supernode, {}))
        self.graph.bulk_add_edge(edge_defns)
        # Checks that nodes and edges creation was ok
        assert(self.graph.V().count() == count)
        assert(self.graph.E().count() == count - 1)
        # Removing supernode and check edges and nodes count :
        supernode.remove()
        assert(self.graph.V().count() == count - 1)
        assert(self.graph.E().count() == 0)

    def test_supernode_below_chunk_size(self):
        self.__check_with_size(CHUNK_SIZE - 1)

    def test_supernode_above_chunk_size(self):
        self.__check_with_size(CHUNK_SIZE + 1)

    def test_extreme_supernode(self):
        self.__check_with_size(25000)


class TestLocalMemoryExtreme(CommonExtreme):

    def setup(self):
        from grapheekdb.backends.data.localmem import LocalMemoryGraph
        self.graph = LocalMemoryGraph()


class TestKyotoCabinetExtreme(CommonExtreme):

    def setup(self):
        from grapheekdb.backends.data.kyotocab import KyotoCabinetGraph
        self.dbpath = tempfile.mktemp() + '.kch'
        self.graph = KyotoCabinetGraph(self.dbpath)

    def teardown(self):
        import os
        self.graph._db_close()
        os.remove(self.dbpath)
