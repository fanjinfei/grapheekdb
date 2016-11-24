# -*- coding:utf-8 -*-

from functools import partial
from itertools import tee

from . import lookups

from grapheekdb.lib.undef import UNDEFINED

from grapheekdb.backends.data.keys import DATA_SUFFIX

from grapheekdb.lib.exceptions import GrapheekSubLookupNotImplementedException
from grapheekdb.lib.exceptions import GrapheekInvalidLookupException


def get_exact_filters(**filters):
    exact_filters = {}
    for key, value in list(filters.items()):
        # Note : not only looking for exact lookups
        lst = key.split('__')
        len_lst = len(lst)
        if len_lst == 1:
            field = lst[0]
            clause = 'exact'
            exact_filters[field] = value
        elif len_lst == 2:
            field, clause = lst
            if clause in ['exact', 'in']:
                exact_filters[field] = value
        else:
            raise GrapheekSubLookupNotImplementedException
    return exact_filters


def build_filter_funcs(**filters):
    field = clause = None
    filter_funcs = []
    for key, value in list(filters.items()):
        lst = key.split('__')
        len_lst = len(lst)
        if len_lst == 1:
            field = lst[0]
            clause = 'exact'
            lookup_func = lookups.lookup_exact
        elif len_lst == 2:
            field, clause = lst
            try:
                lookup_func = getattr(lookups, 'lookup_' + lst[1])
            except AttributeError:
                raise GrapheekInvalidLookupException
        else:
            raise GrapheekSubLookupNotImplementedException
        filter_funcs.append((field, clause, partial(lookup_func, value)))
    return filter_funcs


def entity_match(filter_funcs, data, internals=None):
    if data == UNDEFINED:  # pragma : no cover
        # This could happen because of concurrent modifications (my different process or different threads)
        # This won't normally occur when load testing with greenlets -> thus no cover
        # (or worst : direct access to backend by another "process")
        return False
    for field, clause, filter_func in filter_funcs:
        try:
            field_value = data[field]
        except KeyError:
            try:
                field_value = internals[field]
            except (KeyError, TypeError):
                field_value = None
        if field_value is None and clause != 'isnull':
            return False
        try:
            match = filter_func(field_value)
        except:
            # Whatever happens, it shouldn't
            # So, let's consider that data wasn't ok for the filter
            return False
        if not(match):
            return False
    return True


def filter_entities(graph, kind, iterator, filter_funcs, on_item=False):
    _, cloned_iterator = tee(iterator)
    if filter_funcs:
        if on_item:
            for entity_id in cloned_iterator:
                data = graph._get(None, kind + '/' + str(entity_id) + '/' + DATA_SUFFIX)
                match = entity_match(filter_funcs, data, {'_id': entity_id})
                if match:
                    yield graph._item_from_id(kind, entity_id)
        else:
            for entity_id in cloned_iterator:
                data = graph._get(None, kind + '/' + str(entity_id) + '/' + DATA_SUFFIX)
                match = entity_match(filter_funcs, data, {'_id': entity_id})
                if match:
                    yield entity_id
    else:
        # I don't do : yield item if on_item else entity_id
        # Because item is not computed (when on_item is False, this is not useful to instantiate an Entity)
        if on_item:
            for entity_id in cloned_iterator:
                yield graph._item_from_id(kind, entity_id)
        else:
            for entity_id in cloned_iterator:
                yield entity_id
