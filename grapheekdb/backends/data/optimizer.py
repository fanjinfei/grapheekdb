#!/usr/bin/env
# -*- coding: utf-8 -*-

from grapheekdb.lib.undef import UNDEFINED

from grapheekdb.backends.data.keys import METADATA_VERTEX_COUNTER, METADATA_EDGE_COUNTER
from grapheekdb.backends.data.keys import KIND_VERTEX
from grapheekdb.backends.data.keys import METADATA_EDGE_ID_LIST_PREFIX, METADATA_VERTEX_ID_LIST_PREFIX
from grapheekdb.backends.data.keys import CHUNK_SIZE
from grapheekdb.backends.data.keys import build_key


def choose_index_or_scan(seq_count, indexes, filters):
    """
    Helper function to choose between index use or sequential scan

    :param seq_count:
        an estimation of the sequential scan operation count (currently : number of entity_ids)
    :type seq_count:
        integer
    :param indexes:
        indexes that will challenge sequential scan. each index
        will be asked for an estimation of operation count
    :type indexes:
        BaseIndex child class instance

    :returns:
        the best index if one have been found with a probable best execution time than sequential scan
        OR None if no index was found
    """
    best_estimation = seq_count
    best_index = None
    for index in indexes:
        estimation = index.estimate(None, filters)
        if estimation < 0:  # normally : -1 (aka infinite)
            # special case : index is not competent to handle this filter
            continue
        if estimation < best_estimation:
            best_index = index
            best_estimation = estimation
    return best_index


class Optimizer(object):

    def __init__(self, graph):
        self._graph = graph

    def get_kind_ids(self, txn, kind):
        ENTITY_COUNTER = METADATA_VERTEX_COUNTER if kind == KIND_VERTEX else METADATA_EDGE_COUNTER
        METADATA_ID_LIST_PREFIX = METADATA_VERTEX_ID_LIST_PREFIX if kind == KIND_VERTEX else METADATA_EDGE_ID_LIST_PREFIX
        limit = int(self._graph._get(None, ENTITY_COUNTER)) // CHUNK_SIZE
        keys = [build_key(METADATA_ID_LIST_PREFIX, i) for i in range(0, limit + 1)]
        list_entity_ids = self._graph._bulk_get_lst(txn, keys)
        for entity_ids in list_entity_ids:
            if entity_ids != UNDEFINED:
                for entity_id in entity_ids:
                    yield entity_id

    def index_or_seq_scan_iterator(self, _kind, _seq_count, **filters):

        def indexed_entity_generator(entity_ids):
            for entity_id in entity_ids:
                yield entity_id

        iterator = None
        indexes = self._graph._node_indexes if _kind == KIND_VERTEX else self._graph._edge_indexes

        if filters and indexes:
            # There's some filters, so it *may* be useful to use filters
            # In order to know if it is useful, we will ask  every filter
            # to estimate the number of entity ids that they could return
            # we will, then, choose between the best index (given the filters)
            # and sequential iterator
            best_index = choose_index_or_scan(_seq_count, indexes, filters)
            if best_index:
                # Ok, an index will (hopefully :p) returns less ids than the seq scan, let's get ids :
                entity_ids = best_index.ids(None, filters)
                iterator = indexed_entity_generator(entity_ids)

        return iterator
