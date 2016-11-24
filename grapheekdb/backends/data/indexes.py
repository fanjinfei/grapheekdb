#!/usr/bin/env
# -*- coding: utf-8 -*-

try:  # pragma : no cover
    from itertools import izip_longest as zip_longest
except ImportError:  # pragma : no cover
    from itertools import zip_longest

from itertools import product
from collections import defaultdict

import json

from grapheekdb.lib.undef import UNDEFINED

from grapheekdb.backends.data.keys import FORBIDDEN_KEY
from grapheekdb.backends.data.keys import CHUNK_SIZE
from grapheekdb.backends.data.keys import COUNT_SUFFIX
from grapheekdb.backends.data.keys import DATA_SUFFIX
from grapheekdb.backends.data.keys import build_key

from grapheekdb.backends.data.filtertools import get_exact_filters, build_filter_funcs, entity_match

from grapheekdb.lib.exceptions import GrapheekIncompetentIndexException


def normalize_value(obj):
    return json.dumps(obj)


class BaseIndex(object):

    def __init__(self, _graph, _kind, _prefix):
        self._graph = _graph
        self._kind = _kind
        self._prefix = _prefix

    def bulk_add(self, txn, id_iterator):
        raise NotImplementedError

    def add(self, txn, entity_id, data):
        raise NotImplementedError

    def remove(self, txn, entity, data):
        raise NotImplementedError

    def estimate(self, txn, filters):
        raise NotImplementedError

    def ids(self, txn, filters):
        raise NotImplementedError


class ExactIndex(BaseIndex):

    def __init__(self, _graph, _kind, _prefix, *fields, **filters):
        super(ExactIndex, self).__init__(_graph, _kind, _prefix)
        self._fields = list(fields)
        self._set_fields = set(fields)
        self._filters = filters
        self._filter_items = set(filters.items())
        self._set_filter_keys = set(filters.keys())
        self._filter_length = len(filters)
        self._filter_funcs = build_filter_funcs(**filters)
        self._fields.sort()

    def delete(self, txn):
        """
        delete the index
        """
        self._graph._remove_prefix(txn, self._prefix)

    def bulk_add(self, txn, id_iterator):

        def save_lst(txn, chunk_idx, value, lst):
            lst_key = build_key(self._prefix, chunk_idx, value)
            self._graph._set_lst(txn, lst_key, lst)

        kind = self._kind
        values_to_id = defaultdict(list)
        # Getting data for all entity_id in chunk of <CHUNK_SIZE>
        filter_length = self._filter_length
        filter_funcs = self._filter_funcs
        for entity_ids in zip_longest(*([id_iterator] * CHUNK_SIZE), fillvalue=FORBIDDEN_KEY):
            entity_datas = self._graph._bulk_get(txn, [build_key(kind, entity_id, DATA_SUFFIX) for entity_id in entity_ids if entity_id != FORBIDDEN_KEY])
            for entity_key, data in list(entity_datas.items()):
                if filter_length and not entity_match(filter_funcs, data):
                    continue
                entity_id = int(entity_key.replace(kind + '/', '').replace('/' + DATA_SUFFIX, ''))  # not very clean :(
                lst = [data.get(field, None) for field in self._fields]
                # Completing values_to_id (it's here that the reversal value -> entity_id become possible)
                values_to_id[normalize_value(lst)].append(entity_id)
        # This dic will contain the mapping entity id index key -> value chunk id
        entity_dict = {}
        for value, entity_ids in values_to_id.items():
            lst = []
            value_count_key = build_key(self._prefix, COUNT_SUFFIX, value)
            value_count = self._graph._get(txn, value_count_key)
            if value_count == UNDEFINED:
                value_count = 0
            chunk_idx = value_count // CHUNK_SIZE
            for entity_id in entity_ids:
                value_count += 1
                entity_dict[build_key(self._prefix, entity_id)] = build_key(chunk_idx, value)  # Need to keep an info of relation between entity and the chunk id that contains it
                lst.append(entity_id)
                if value_count % CHUNK_SIZE == 0:
                    save_lst(txn, chunk_idx, value, lst)
                    lst = []
                    chunk_idx += 1
            if lst:
                save_lst(txn, chunk_idx, value, lst)
            self._graph._set(txn, value_count_key, value_count)
        self._graph._bulk_set(txn, entity_dict)

    def add(self, txn, entity_id, data):
        if self._filter_length and not entity_match(self._filter_funcs, data):
            return
        value = normalize_value([data.get(field, None) for field in self._fields])
        # Get current chunk idx for this value (NOTE : value is a string representing a list)
        value_count_key = build_key(self._prefix, COUNT_SUFFIX, value)
        value_count = 0
        value_count_current = self._graph._get(txn, value_count_key)
        if value_count_current != UNDEFINED:
            value_count = int(value_count_current)
        # Incrementing entity count for value
        self._graph._set(txn, value_count_key, value_count + 1)
        #  1st adding entity_id to good chunk :
        chunk_idx = value_count // CHUNK_SIZE
        chunk_key = build_key(self._prefix, chunk_idx, value)
        self._graph._append_to_lst(txn, chunk_key, entity_id)
        # Say that entity_id is in a chunk
        # (we need to keep chunk idx, so that entity id can be removed fastly
        # from chunk when entity will be removed from DB)
        self._graph._set(txn, build_key(self._prefix, entity_id), build_key(chunk_idx, value))

    def remove(self, txn, entity_id):
        entity_chunk_sub_key = self._graph._get(txn, build_key(self._prefix, entity_id))
        if entity_chunk_sub_key != UNDEFINED:
            # remove entity_id from chunk
            chunk_key = build_key(self._prefix, entity_chunk_sub_key)
            self._graph._remove_from_lst(txn, chunk_key, entity_id)
            # Finally remove entity_chunk_sub_key
            self._graph._remove(txn, entity_chunk_sub_key)

    def _compatible_filters(self, filters):
        # FIXME : This code works but is hard to understand (for me)
        # And it expects get_exact_filters to build a special value (a list) for __in lookups
        # This method should be as short as :
        # index.supersedes(filters)
        # 1st -a- check that filters is compatible with (partial) index filters
        exact_filters = get_exact_filters(**filters)
        set_exact_filter_keys = set(exact_filters.keys())
        if not(self._set_fields.issubset(set_exact_filter_keys)):
            return False
        if not(self._set_filter_keys.issubset(set(exact_filters.keys()))):
            return False
        common_filters = {}
        if self._filter_length:
            for key, value in list(self._filters.items()):
                out_value = exact_filters[key]
                if value == out_value or (isinstance(out_value, list) and value in out_value):
                    common_filters[key] = value
                else:
                    return False
        exact_filters.update(common_filters)
        # 2nd, check on exact filters
        resulting_filters = []
        for field in self._fields:
            v = exact_filters[field]
            resulting_filters.append(v if isinstance(v, list) else [v])
        return resulting_filters

    def estimate(self, txn, filters):
        resulting_filters = self._compatible_filters(filters)
        if resulting_filters is False:
            return -1  # -1 says that this index shouldn't be used
        total = 0
        for values in product(*resulting_filters):
            value = normalize_value(values)
            value_count_key = build_key(self._prefix, COUNT_SUFFIX, value)
            value_count_current = self._graph._get(txn, value_count_key)
            if value_count_current != UNDEFINED:
                total += int(value_count_current)
        return total  # note : this is an over estimation

    def ids(self, txn, filters):
        resulting_filters = self._compatible_filters(filters)
        if resulting_filters is False:
            raise GrapheekIncompetentIndexException("This index shouldn't have been used")
        for values in product(*resulting_filters):
            value = normalize_value(values)
            value_count_key = build_key(self._prefix, COUNT_SUFFIX, value)
            value_count_current = self._graph._get(txn, value_count_key)
            if value_count_current != UNDEFINED:
                for chunk_idx in range(0, 1 + int(value_count_current) // CHUNK_SIZE):
                    entity_ids_key = build_key(self._prefix, chunk_idx, value)
                    entity_ids = self._graph._get_lst(txn, entity_ids_key)
                    if entity_ids != UNDEFINED:
                        for entity_id in entity_ids:
                            yield entity_id
