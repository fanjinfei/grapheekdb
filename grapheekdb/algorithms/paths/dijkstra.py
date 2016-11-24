# -*- coding: utf-8 -*-
"""
Adapted from NetworkX algorithms to grapheekdb methods
"""

import heapq

from grapheekdb.backends.data.keys import KIND_VERTEX
from grapheekdb.lib.exceptions import GrapheekUnreachableNode


def single_source_dijkstra(g, src, tgt=None, cutoff=None, weight=None):
    """
    Compute shortest paths and lengths in a weighted graph g.
    Uses Dijkstra's algorithm for shortest paths.

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

    :param cutoff:
        Depth to stop the search. Only paths of length <= cutoff are returned.

    :type cutoff:
        integer or float, optional

    :param weight:
        edge attribute that contains the weight

    :type weight:
        string


    :returns
        distance,path : dictionaries
        Returns a tuple of two dictionaries keyed by node id.
        The first dictionary stores distance from the source.
        The second stores the path from the source to that node.

    Notes
    ---------
    Edge weight attributes must be numerical.
    Distances are calculated as sums of weighted edges traversed.

    Based on the Python cookbook recipe (119466) at
    http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/119466

    This algorithm is not guaranteed to work if edge weights
    are negative or are floating point numbers
    (overflows and roundoff errors can cause problems).
    """
    source_id = src.get_id()
    target_id = tgt.get_id() if tgt is not None else None
    if src == tgt:
        return ({source_id: 0}, {source_id: [source_id]})
    dists = {}  # dictionary of final distances
    paths = {source_id: [source_id]}  # dictionary of paths
    seen = {source_id: 0}
    fringe = []  # use heapq with (distance,node_id) tuples
    heapq.heappush(fringe, (0, source_id))
    while fringe:
        (d, node_id) = heapq.heappop(fringe)
        if node_id in dists:
            continue  # already searched this node.
        dists[node_id] = d
        if node_id == target_id:
            break
        node = g._item_from_id(KIND_VERTEX, node_id)
        out_edges = node.outE()
        out_node_ids = node.outV()._iterate()

        for out_edge, out_node_id in zip(out_edges, out_node_ids):
            if weight is not None:
                edgedata = out_edge.data()
                vw_dist = d + edgedata[weight]
            else:
                vw_dist = d + 1
            if cutoff is not None:
                if vw_dist > cutoff:
                    continue
            if out_node_id in dists:
                if vw_dist < dists[out_node_id]:
                    raise ValueError('Contradictory paths found:',
                                     'negative weights?')
            elif out_node_id not in seen or vw_dist < seen[out_node_id]:
                seen[out_node_id] = vw_dist
                heapq.heappush(fringe, (vw_dist, out_node_id))
                paths[out_node_id] = paths[node_id] + [out_node_id]
    return (dists, paths)


def dijkstra_path(g, src, tgt, cutoff=None, weight=None):
    dists, paths = single_source_dijkstra(g, src, tgt, cutoff=cutoff, weight=weight)
    try:
        dist = dists[tgt.get_id()]
    except KeyError:
        raise GrapheekUnreachableNode("Unreachable node")
    path = []
    for node_id in paths[tgt.get_id()]:
        path.append(g._item_from_id(KIND_VERTEX, node_id))
    return (dist, path)


