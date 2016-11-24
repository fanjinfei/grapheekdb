# -*- coding:utf-8 -*-

from grapheekdb.backends.data.localmem import LocalMemoryGraph


class CommonMethod(object):

    def setup(self):
        graph = self.graph = LocalMemoryGraph()
        self.n1 = graph.add_node()
        self.n2 = graph.add_node()
        self.n3 = graph.add_node()
        self.n4 = graph.add_node()
        self.n5 = graph.add_node()
        self.e1 = graph.add_edge(self.n1, self.n2, time=1)
        self.e2 = graph.add_edge(self.n2, self.n3, time=2)
        self.e3 = graph.add_edge(self.n3, self.n4, time=3)
        self.e4 = graph.add_edge(self.n1, self.n4, time=7)
        self.e5 = graph.add_edge(self.n2, self.n1, time=1)
        self.e6 = graph.add_edge(self.n2, self.n4, time=7)
        self.e7 = graph.add_edge(self.n3, self.n2, time=1)

