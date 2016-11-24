# -*- coding:utf-8 -*-

import tempfile
import shutil

from .test_data_backend_localmem import TLocalMemoryGraph


class TestlmdbGraph(TLocalMemoryGraph):

    def setup(self):
        from grapheekdb.backends.data.symaslmdb import LmdbGraph
        self.dbpath = tempfile.mktemp()
        self.graph = LmdbGraph(self.dbpath, map_size=10 * 1024 * 1024)
        self.fill()

    def teardown(self):
        try:
            self.graph._db_close()
        except:
            pass
        shutil.rmtree(self.dbpath)

    def test_close(self):
        # Removing reference to existing vertices and edges, to remove references to graph
        self.n1 = self.n2 = self.n3 = None
        self.e1 = self.e2 = None
        # Just checking that no exception raised :
        del self.graph
