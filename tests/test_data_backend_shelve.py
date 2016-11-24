# -*- coding:utf-8 -*-

import tempfile
import os

from .test_data_backend_localmem import TLocalMemoryGraph


class TestShelveGraph(TLocalMemoryGraph):

    def setup(self):
        from grapheekdb.backends.data.shelvefile import ShelveGraph
        self.dbpath = tempfile.mktemp()
        self.graph = ShelveGraph(self.dbpath)
        self.fill()

    def teardown(self):
        try:
            os.remove(self.dbpath)
        except:
            pass

    def test_close(self):
        # Removing reference to existing vertices and edges, to remove references to graph
        self.n1 = self.n2 = self.n3 = None
        self.e1 = self.e2 = None
        # Just checking that no exception raised :
        del self.graph
