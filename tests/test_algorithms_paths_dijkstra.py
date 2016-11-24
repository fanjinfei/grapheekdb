# -*- coding:utf-8 -*-

from grapheekdb.algorithms.paths.dijkstra import single_source_dijkstra, dijkstra_path
from grapheekdb.lib.exceptions import GrapheekUnreachableNode
from .algorithms_paths_common import CommonMethod


class TestDijkstra(CommonMethod):

    def test_single_source_dijkstra(self):
        dists, paths = single_source_dijkstra(self.graph, self.n1, self.n4, weight='time')
        n4_id = self.n4.get_id()
        path = paths[n4_id]
        assert(path == [0, 1, 2, 3])
        assert(dists[n4_id] == 6)

    def test_single_source_dijkstra_no_weight(self):
        dists, paths = single_source_dijkstra(self.graph, self.n1, self.n4)
        n4_id = self.n4.get_id()
        path = paths[n4_id]
        assert(path == [0, 3])
        assert(dists[n4_id] == 1)

    def test_single_source_dijkstra_source_same_as_target(self):
        dists, paths = single_source_dijkstra(self.graph, self.n1, self.n1)
        n1_id = self.n1.get_id()
        path = paths[n1_id]
        assert(path == [0])
        assert(dists[n1_id] == 0)

    def test_dijkstra_path(self):
        dist, path = dijkstra_path(self.graph, self.n1, self.n4, weight='time')
        path_ids = [item.get_id() for item in path]
        assert(path_ids == [0, 1, 2, 3])
        assert(dist == 6)

    def test_dijkstra_path_no_weight(self):
        dist, path = dijkstra_path(self.graph, self.n1, self.n4)
        path_ids = [item.get_id() for item in path]
        assert(path_ids == [0, 3])
        assert(dist == 1)

    def test_dijkstra_path_source_same_as_target(self):
        dist, path = dijkstra_path(self.graph, self.n1, self.n1)
        path_ids = [item.get_id() for item in path]
        assert(path_ids == [0])
        assert(dist == 0)

    def test_dijkstra_path_negative_weight(self):
        self.e7 = self.graph.add_edge(self.n2, self.n1, time=-7)
        exception_raised = False
        try:
            dijkstra_path(self.graph, self.n1, self.n4, weight='time')
        except:
            exception_raised = True
        assert(exception_raised)

    def test_dijkstra_path_unreachable_node(self):
        exception_raised = False
        try:
            dijkstra_path(self.graph, self.n1, self.n5, weight='time')
        except GrapheekUnreachableNode:
            exception_raised = True
        assert(exception_raised)

    def test_dijkstra_path_cutoff(self):
        exception_raised = False
        try:
            dist, path = dijkstra_path(self.graph, self.n1, self.n4, cutoff=2, weight='time')
        except GrapheekUnreachableNode:
            exception_raised = True
        assert(exception_raised)


