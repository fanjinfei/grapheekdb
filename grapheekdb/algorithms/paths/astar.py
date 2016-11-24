# -*- coding: utf-8 -*-

from heapq import heappop, heappush

from grapheekdb.backends.data.keys import KIND_VERTEX
from grapheekdb.lib.exceptions import GrapheekUnreachableNode


def astar_path(g, src, tgt, heuristic=None, weight=None):
    """Return a list of nodes in a shortest path between source and target
    using the A* ("A-star") algorithm.

    There may be more than one shortest path.  This returns only one.

    :param g:
        GrapheekDB graph

    :param src:
        starting node for path

    :type src:
        node

    :param tgt:
        ending node for path (optionnal)

    :type tgt:
        node

    :param heuristic:
       A function to evaluate the estimate of the distance
       from the a node to the target.  The function takes
       two nodes arguments and must return a number.

    :type heuristic:
        function

    :param weight:
        edge attribute that contains the weight

    :type weight:
        string

    :returns
        distance,path : dictionaries
        Returns a tuple of two dictionaries keyed by node id.
        The first dictionary stores distance from the source.
        The second stores the path from the source to that node.

    """
    if heuristic is None:
        # The default heuristic is h=0 - same as Dijkstra's algorithm
        def heuristic(u, v):
            return 0
    # The queue stores priority, node, cost to reach, and parent.
    # Uses Python heapq to keep in priority order.
    # Add each node's hash to the queue to prevent the underlying heap from
    # attempting to compare the nodes themselves. The hash breaks ties in the
    # priority and is guarenteed unique for all nodes in the graph.
    source_id = src.get_id()
    target_id = tgt.get_id()
    target_data = tgt.data()
    queue = [(0, hash(source_id), source_id, 0, None)]

    # Maps enqueued nodes to distance of discovered paths and the
    # computed heuristics to target. We avoid computing the heuristics
    # more than once and inserting the node into the queue too many times.
    enqueued = {}
    # Maps explored nodes to parent closest to the source.
    explored = {}

    while queue:
        # Pop the smallest item from queue.
        _, __, curnode_id, dist, parent_id = heappop(queue)
        if curnode_id == target_id:
            path = [curnode_id]
            node_id = parent_id
            while node_id is not None:
                path.append(node_id)
                node_id = explored[node_id]
            path.reverse()
            return [g._item_from_id(KIND_VERTEX, idx) for idx in path]

        if curnode_id in explored:
            continue

        explored[curnode_id] = parent_id

        curnode = g._item_from_id(KIND_VERTEX, curnode_id)
        out_edges = curnode.outE()
        out_nodes = curnode.outV()

        for out_edge, out_node in zip(out_edges, out_nodes):
            out_node_id = out_node.get_id()
            if out_node_id in explored:
                continue
            if weight is not None:
                edgedata = out_edge.data()
                ncost = dist + edgedata[weight]
            else:
                ncost = dist + 1
            if out_node_id in enqueued:
                qcost, h = enqueued[out_node_id]
                # if qcost < ncost, a longer path to neighbor remains
                # enqueued. Removing it would need to filter the whole
                # queue, it's better just to leave it there and ignore
                # it when we visit the node a second time.
                if qcost <= ncost:
                    continue
            else:
                h = heuristic(out_node.data(), target_data)
            enqueued[out_node_id] = ncost, h
            heappush(queue, (ncost + h, hash(out_node_id), out_node_id,
                             ncost, curnode_id))

    raise GrapheekUnreachableNode("Node %s not reachable from %s" % (src, tgt))


def astar_path_length(g, src, tgt, heuristic=None, weight=None):
    """
    :param g:
        GrapheekDB graph

    :param src:
        starting node for path

    :type src:
        node

    :param tgt:
        ending node for path (optionnal)

    :type tgt:
        node

    :param heuristic:
       A function to evaluate the estimate of the distance
       from the a node to the target.  The function takes
       two nodes arguments and must return a number.

    :type heuristic:
        function

    :param weight:
        edge attribute that contains the weight

    :type weight:
        string

    :returns
        distance,path : dictionaries
        Return the length of the shortest path between source and target using
        the A* ("A-star") algorithm.
    """
    path = astar_path(g, src, tgt, heuristic, weight)
    result = 0
    # FIXME : Following implementation is quite poor from a performance point of view
    for source, target in zip(path[:-1], path[1:]):
        for out_edge in source.outE():
            if target in list(out_edge.outV()):
                result += out_edge.data().get(weight, 1)
                break
    return result
