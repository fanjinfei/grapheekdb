# -*- coding:utf-8 -*-

from grapheekdb.algorithms.paths.astar import astar_path, astar_path_length
from grapheekdb.lib.exceptions import GrapheekUnreachableNode
from .algorithms_paths_common import CommonMethod


class TestAStar(CommonMethod):

    def test_astar_path(self):
        path = astar_path(self.graph, self.n1, self.n4, weight='time')
        path_ids = [item.get_id() for item in path]
        assert(path_ids == [0, 1, 2, 3])

    def test_astar_path_no_weight(self):
        path = astar_path(self.graph, self.n1, self.n4)
        path_ids = [item.get_id() for item in path]
        assert(path_ids == [0, 3])

    def test_astar_path_source_same_as_target(self):
        path = astar_path(self.graph, self.n1, self.n1)
        path_ids = [item.get_id() for item in path]
        assert(path_ids == [0])

    def test_astar_path_unreachable_node(self):
        exception_raised = False
        try:
            astar_path(self.graph, self.n1, self.n5, weight='time')
        except GrapheekUnreachableNode:
            exception_raised = True
        assert(exception_raised)

    def test_astar_path_length(self):
        length = astar_path_length(self.graph, self.n1, self.n4, weight='time')
        assert(length == 6)

    def test_astar_path_length_no_weight(self):
        length = astar_path_length(self.graph, self.n1, self.n4)
        assert(length == 1)

    def test_astar_path_length_source_same_as_target(self):
        length = astar_path_length(self.graph, self.n1, self.n1)
        assert(length == 0)


