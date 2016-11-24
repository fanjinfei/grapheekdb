# -*- coding:utf-8 -*-

import sys
import random
from collections import defaultdict
from functools import reduce, cmp_to_key
from itertools import chain, tee

from grapheekdb.lib.undef import UNDEFINED
from grapheekdb.lib.readwrite import GraphReadWrite
from grapheekdb.lib.nx import GraphNx
from grapheekdb.lib.validations import check_valid_data
from grapheekdb.lib.exceptions import GrapheekDataException, GrapheekDataPreparationFailedException, GrapheekIndexAlreadyExistsException
from grapheekdb.lib.exceptions import GrapheekIndexCreationFailedException, GrapheekIndexRemovalFailedException, GrapheekNoSuchTraversalException
from grapheekdb.lib.exceptions import GrapheekMissingKeyException, GrapheekInvalidDataTypeException
from grapheekdb.lib.exceptions import GrapheekUnknownScriptException, GrapheekUnknownAlias
from grapheekdb.lib.exceptions import GrapheekUnknownMethod, GrapheekMixedKindException
from grapheekdb.lib.expressions import eval_expr

from grapheekdb.backends.data.keys import build_key, PREPARED
from grapheekdb.backends.data.keys import METADATA_EDGE_COUNTER, METADATA_EDGE_INDEX_COUNTER, METADATA_EDGE_INDEX_LIST, METADATA_EDGE_INDEX_FIELDS_PREFIX, METADATA_EDGE_INDEX_PREFIX
from grapheekdb.backends.data.keys import METADATA_VERTEX_COUNTER, METADATA_VERTEX_INDEX_COUNTER, METADATA_VERTEX_INDEX_LIST, METADATA_VERTEX_INDEX_FIELDS_PREFIX, METADATA_VERTEX_INDEX_PREFIX
from grapheekdb.backends.data.keys import METADATA_VERTEX_REMOVED_COUNTER, METADATA_EDGE_REMOVED_COUNTER
from grapheekdb.backends.data.keys import DATA_SUFFIX, IN_EDGES_SUFFIX, IN_VERTICES_SUFFIX, OUT_EDGES_SUFFIX, OUT_VERTICES_SUFFIX, BOTH_EDGES_SUFFIX, BOTH_VERTICES_SUFFIX
from grapheekdb.backends.data.keys import COUNT_SUFFIX, KIND_VERTEX, KIND_EDGE, KIND_INDEX
from grapheekdb.backends.data.keys import METADATA_EDGE_ID_LIST_PREFIX, METADATA_VERTEX_ID_LIST_PREFIX, CHUNK_SIZE
from grapheekdb.backends.data.filtertools import build_filter_funcs, filter_entities
from grapheekdb.backends.data.ordertools import build_order_func
from grapheekdb.backends.data.indexes import ExactIndex, normalize_value
from grapheekdb.backends.data.operations import Addition, Removal
from grapheekdb.backends.data.optimizer import Optimizer

PYTHON3 = sys.version_info.major == 3


# misc iterators :
def _jump_iterator(_graph, _kind, _entity_id_iterator, _traversal, _neighbors_cache, **filters):
    for entity_id in _entity_id_iterator:
        for neighbor_id in iter(_graph._neighbors(_kind, entity_id, _traversal, _neighbors_cache, False, **filters)):
            yield neighbor_id


def _random_iterator(_graph, _kind, _entity_id_iterator, _traversal, _neighbors_cache, **filters):
    for entity_id in _entity_id_iterator:
        for neighbor_id in iter(_graph._neighbors(_kind, entity_id, _traversal, _neighbors_cache, True, **filters)):
            yield neighbor_id


def check_base_node(entity):
    if not(isinstance(entity, Node)):
        raise GrapheekInvalidDataTypeException('%s is not a node' % (entity,))


def check_and_get_count(args):
    count_args = len(args)
    assert(count_args < 2)
    # how many recursive jumps ? :
    count = 1
    if count_args == 1:
        count = int(args[0])
        assert(count >= 0)
    return count


class DotExport(object):
    """
    This class is mainly intended to be used with an IPython Graphviz Magics extension
    You can start IPython by launching in a shell
    ipython notebook

    Then create a new notebook and install extension :
    %install_ext https://raw.github.com/cjdrake/ipython-magic/master/gvmagic.py

    Load extension :
    %load_ext gvmagic

    Then you can see an entity iterator by doing, for instance :
    %dotobj g.V(...).dot()

    The EntityIterator.dot method instantiates a DotExport which itself has a .to_dot method as expected by gvmagic...
    """

    def __init__(self, entity_iterator, node_label=None, edge_label=None, limit=100):
        self._entity_iterator = entity_iterator
        self._graph = entity_iterator._graph
        self.node_label = node_label
        self.edge_label = edge_label
        self.limit = int(limit)
        assert(self.limit > 0)

    def __str__(self):
        return self.to_dot()

    def _clean_label(self, label):
        import re
        s = re.sub('[^0-9a-zA-Z_\s]', '', str(label))  # Needs sth less intrusive
        return s

    def _node_label(self, node_id):
        node = Node(node_id, self._graph)
        label = str(node_id)  # Default value
        if self.node_label is not None:
            label = node.data().get(self.node_label, label)
        return self._clean_label(label)

    def _edge_struct(self, edge_id):
        edge = Edge(edge_id, self._graph)
        label = ''  # Default value
        if self.edge_label is not None:
            label = edge.data().get(self.edge_label, label)
        # get src and tgt node ids :
        src_id = edge._src_id()
        tgt_id = edge._tgt_id()
        return (self._clean_label(label), src_id, tgt_id)

    def to_dot(self):
        entity_iterator = self._entity_iterator
        kind = entity_iterator._src_kind
        assert(kind in [KIND_VERTEX, KIND_EDGE])
        node_collections = []
        edge_collections = []
        if kind == KIND_EDGE:
            # Collecting label, ids and related node ids :
            edge_node_ids = set()
            for edge_id in set(entity_iterator.limit(self.limit)._iterate(on_item=False)):
                label, src_id, tgt_id = self._edge_struct(edge_id)
                edge_collections.append((label, src_id, tgt_id))
                edge_node_ids.add(src_id)
                edge_node_ids.add(tgt_id)
            # Collecting nodes :
            for node_id in edge_node_ids:
                label = self._node_label(node_id)
                node_collections.append((node_id, label))
        else:  # KIND_VERTEX
            node_edge_ids = set()
            node_ids = set(entity_iterator.limit(self.limit)._iterate(on_item=False))
            # Collecting label, ids and related edge ids :
            for node_id in node_ids:
                label = self._node_label(node_id)
                node_collections.append((node_id, label))
                for edge_id in self._graph._neighbors(KIND_VERTEX, node_id, BOTH_EDGES_SUFFIX):
                    if edge_id not in node_edge_ids:
                        edge_label, edge_src_id, edge_tgt_id = self._edge_struct(edge_id)
                        if (edge_src_id in node_ids) and (edge_tgt_id in node_ids):
                            edge_collections.append((edge_label, edge_src_id, edge_tgt_id))
                            node_edge_ids.add(edge_id)
        # Finally build the dot string :
        lines = []
        lines.append('digraph G {')
        for node_id, label in node_collections:
            lines.append('"n%s" [' % node_id)
            lines.append('label ="%s"' % label)
            lines.append('];')
        for label, src_id, tgt_id in edge_collections:
            lines.append('"n%s" -> "n%s" [label="%s"]' % (src_id, tgt_id, label))
        lines.append('}')
        return '\n'.join(lines)


class EntityAggregate(object):

    def __init__(self, graph, entity_kind, entity_iterator, pair_iterator=None, agg='+', expr=None, asc=False):
        self._graph = graph
        self._entity_kind = entity_kind
        self._entity_iterator = entity_iterator
        self._pair_iterator = pair_iterator
        assert(agg in (None, '+', '%'))
        self._agg = agg
        self._expr = expr
        self._mult = 1 if asc else -1
        # -------------------------------
        self._current_iteration = None

    def _get_results(self):
        # Special case : if self._agg is None, then it means that current EntityAggregate
        # has been created by another EntityAggregate (for instance, by method limit)
        # so we'll just iterate on _pair_iterator
        if self._agg is None:
            return list(self._pair_iterator)
        if self._agg == '+':
            counters = defaultdict(int)
        else:
            counters = defaultdict(float)
        total = 0
        entity_data_cache = {}
        for entity_id in self._entity_iterator._iterate(on_item=False):
            if self._expr is None:
                value = 1
            else:
                # Safe eval of expression :
                entity_data = entity_data_cache.get(entity_id, None)
                if entity_data is None:
                    entity_data = self._graph._item_from_id(self._entity_kind, entity_id).data()
                    entity_data_cache[entity_id] = entity_data
                context = entity_data
                context['_'] = self._entity_iterator._context
                try:
                    value = eval_expr(context, self._expr)
                finally:
                    del context['_']
            counters[entity_id] += value
            total += value
        if total and (self._agg == '%'):
            for entity_id, value in list(counters.items()):
                counters[entity_id] = float(counters[entity_id]) / float(total)  # This may raise an exception, it's up to the user to handle it...
        ordered_counters = sorted(list(counters.items()), key=lambda t: t[1] * self._mult)
        results = []
        for entity_id, count in ordered_counters:
            entity = self._graph._item_from_id(self._entity_kind, entity_id)
            results.append((entity, count))
        return results

    def __iter__(self):
        return self

    def next(self):  # pragma : no cover
        # Python 2 compatibility
        return self.__next__()

    def __next__(self):
        if self._current_iteration is None:
            self._current_iteration = iter(self._get_results())
        try:
            return next(self._current_iteration)
        except StopIteration:
            self._current_iteration = None
        raise StopIteration

    def entities(self):
        def entity_iterator(iterator):
            for entity, _ in iterator:
                yield entity.get_id()
        return EntityIterator(self._graph, self._entity_kind, {}, entity_iterator(self), parent=self._entity_iterator)

    def limit(self, number):
        def limit_iterator(iterator, number):
            counter = 0
            for entity_id, count in iterator:
                if counter < number:
                    yield (entity_id, count)
                else:
                    break  # This line is REALLY important (it stops outer iteration)
                counter += 1
        return EntityAggregate(self._graph, self._entity_kind, self._entity_iterator, limit_iterator(self, number), agg=None, asc=None)


class EntityIterator(object):

    def __init__(self, graph, src_kind, filters, id_iterator, parent=None, neighbors_cache=None, context=None):
        assert(id_iterator is not None)
        self._graph = graph
        self._src_kind = src_kind
        self._filters = filters
        self._filter_funcs = build_filter_funcs(**filters)
        self._initial_id_iterator, self._id_iterator = tee(id_iterator)
        self._parent = parent
        self._neighbors_cache = neighbors_cache if neighbors_cache is not None else {}
        self._release_neighbors_cache = neighbors_cache is None
        if context is None:
            self._context = {} if parent is None else parent._context
        else:
            self._context = context
        self._current_iteration = None

    def __next__(self):
        if self._current_iteration is None:
            self._current_iteration = self._clone_initial()._iterate(on_item=True)
        try:
            return next(self._current_iteration)
        except StopIteration:
            self._current_iteration = None
        raise StopIteration

    def __iter__(self):
        return self

    def __contains__(self, entity):
        try:
            if entity._kind != self._src_kind:
                return False
        except:
            return False
        entity_id = entity.get_id()
        for it_entity_id in self._clone_initial()._iterate(on_item=False):
            if it_entity_id == entity_id:
                return True
        return False

    def __le__(self, it2):
        return self._graph.issubset(self, it2)

    def __ge__(self, it2):
        return self._graph.issuperset(self, it2)

    def __or__(self, it2):
        # | => union
        return self._graph.union(self, it2)

    def __and__(self, it2):
        # & => intersection
        return self._graph.intersection(self, it2)

    def __sub__(self, it2):
        # - => difference
        return self._graph.difference(self, it2)

    def __xor__(self, it2):
        # ^ => symmetric_difference
        return self._graph.symmetric_difference(self, it2)

    def _finalize(self):
        if self._release_neighbors_cache:
            # Implicit cache removal
            self._neighbors_cache = {}

    def _iterate(self, on_item=False):
        for item in filter_entities(self._graph, self._src_kind, self._id_iterator, self._filter_funcs, on_item=on_item):
            yield item
        self._finalize()

    def _clone_initial(self):
        self._initial_id_iterator, id_iterator = tee(self._initial_id_iterator)
        return EntityIterator(self._graph, self._src_kind, self._filters, id_iterator, self._parent)

    def _jump(self, _kind, _traversal, _random, *args, **filters):
        jumping_iterator = _random_iterator if _random else _jump_iterator
        # TODO : Refactor this function : it can certainly be rewritten in a far easier way
        count = check_and_get_count(args)
        current_iterator = self
        parent_kind = self._src_kind
        current_kind = _kind
        current_parent = self
        # (potentially) recursive jumping
        #_jump_iterator(graph, kind, entity_id_iterator, traversal, neighbors_cache, filters)
        for _ in range(count):
            current_iterator = EntityIterator(
                self._graph,
                current_kind,
                filters,
                jumping_iterator(
                    self._graph,
                    parent_kind,
                    current_iterator._iterate(),
                    _traversal,
                    self._neighbors_cache,
                    **filters
                ),
                parent=current_parent
            )
            parent_kind = current_kind
            current_kind = KIND_VERTEX
            current_parent = current_iterator
        return current_iterator

    def next(self):
        # Python 2 compatibility
        return self.__next__()

    def inV(self, *args, **filters):
        return self._jump(KIND_VERTEX, IN_VERTICES_SUFFIX, False, *args, **filters)

    def outV(self, *args, **filters):
        return self._jump(KIND_VERTEX, OUT_VERTICES_SUFFIX, False, *args, **filters)

    def bothV(self, *args, **filters):
        return self._jump(KIND_VERTEX, BOTH_VERTICES_SUFFIX, False, *args, **filters)

    def in_(self, *args, **filters):
        assert(self._src_kind == KIND_VERTEX)
        count = check_and_get_count(args)
        if count == 1:
            return self.inE(**filters).inV()
        return self.in_(count - 1, **filters).inE(*args, **filters).inV()

    def out_(self, *args, **filters):
        assert(self._src_kind == KIND_VERTEX)
        count = check_and_get_count(args)
        if count == 1:
            return self.outE(**filters).outV()
        return self.out_(count - 1, **filters).outE(*args, **filters).outV()

    def both_(self, *args, **filters):
        assert(self._src_kind == KIND_VERTEX)
        count = check_and_get_count(args)

        def neighbor_iterator(graph, it, filters, neighbors_cache, filter_funcs):
            entity_ids = list(it)
            for entity_id in entity_ids:
                neighbor_edge_id_iterator = graph._neighbors(KIND_VERTEX, entity_id, OUT_EDGES_SUFFIX, neighbors_cache, **filters)
                for neighbor_edge_id in filter_entities(self._graph, KIND_EDGE, neighbor_edge_id_iterator, filter_funcs):
                    for neighbor_node_id in graph._neighbors(KIND_EDGE, neighbor_edge_id, OUT_VERTICES_SUFFIX, neighbors_cache):
                        if int(entity_id) != int(neighbor_node_id):
                            yield neighbor_node_id
            for entity_id in entity_ids:
                neighbor_edge_id_iterator = graph._neighbors(KIND_VERTEX, entity_id, IN_EDGES_SUFFIX, neighbors_cache, **filters)
                for neighbor_edge_id in filter_entities(self._graph, KIND_EDGE, neighbor_edge_id_iterator, filter_funcs):
                    for neighbor_node_id in graph._neighbors(KIND_EDGE, neighbor_edge_id, IN_VERTICES_SUFFIX, neighbors_cache):
                        if int(entity_id) != int(neighbor_node_id):
                            yield neighbor_node_id

        filter_funcs = build_filter_funcs(**filters)
        if count == 1:
            return EntityIterator(
                self._graph,
                KIND_VERTEX,
                {},
                neighbor_iterator(self._graph, self._iterate(), filters, self._neighbors_cache, filter_funcs),
                parent=self,
                neighbors_cache=self._neighbors_cache
            )
        entity_iterator = self.both_(count - 1, **filters)
        return EntityIterator(
            self._graph,
            KIND_VERTEX,
            {},
            neighbor_iterator(self._graph, entity_iterator._iterate(), filters, self._neighbors_cache, filter_funcs),
            parent=entity_iterator,
            neighbors_cache=self._neighbors_cache
        )

    # NOTE : for xxE methods, below, there's no *args :
    # currently *args is aimed to contain only number of jumps
    # so only 1st item of args is used
    # if args is empty, only one jump is done
    # So this is "recursive" jumping
    # But chaining xxE methods wouldn't make sense as Edge outer entities are always Nodes

    def inE(self, *args, **filters):
        if self._src_kind == KIND_EDGE:
            raise GrapheekNoSuchTraversalException
        return self._jump(KIND_EDGE, IN_EDGES_SUFFIX, False, **filters)  # no *args for inE ()

    def outE(self, *args, **filters):
        if self._src_kind == KIND_EDGE:
            raise GrapheekNoSuchTraversalException
        return self._jump(KIND_EDGE, OUT_EDGES_SUFFIX, False, **filters)

    def bothE(self, *args, **filters):
        if self._src_kind == KIND_EDGE:
            raise GrapheekNoSuchTraversalException
        return self._jump(KIND_EDGE, BOTH_EDGES_SUFFIX, False, **filters)

    def random(self, _method, *args, **filters):
        if self._src_kind == KIND_VERTEX:
            try:
                kind, traversal = {
                    'inV': (KIND_VERTEX, IN_VERTICES_SUFFIX),
                    'outV': (KIND_VERTEX, OUT_VERTICES_SUFFIX),
                    'bothV': (KIND_VERTEX, BOTH_VERTICES_SUFFIX),
                    'inE': (KIND_EDGE, IN_EDGES_SUFFIX),
                    'outE': (KIND_EDGE, OUT_EDGES_SUFFIX),
                    'bothE': (KIND_EDGE, BOTH_EDGES_SUFFIX)
                }[_method]
            except KeyError:
                raise GrapheekUnknownMethod(_method)
        elif self._src_kind == KIND_EDGE:
            try:
                kind, traversal = {
                    'inV': (KIND_VERTEX, IN_VERTICES_SUFFIX),
                    'outV': (KIND_VERTEX, OUT_VERTICES_SUFFIX),
                    'bothV': (KIND_VERTEX, BOTH_VERTICES_SUFFIX)
                }[_method]
            except KeyError:
                raise GrapheekUnknownMethod(_method)
        return self._jump(KIND_EDGE, BOTH_EDGES_SUFFIX, True, **filters)

    def dedup(self, *aliases):
        def dedup_iterator(entity_iterator, aliases):
            if aliases:
                alias_known = defaultdict(set)
            else:
                known = set()
            for entity_id in entity_iterator._iterate():
                if aliases:
                    keys = []
                    for alias in aliases:
                        ctx_entity = entity_iterator._context.get(alias, None)
                        if ctx_entity is None:
                            raise GrapheekUnknownAlias("'%s' alias is not defined" % (alias,))
                        keys.append((ctx_entity._kind, ctx_entity.get_id()))
                    known = alias_known[tuple(keys)]
                if entity_id in known:
                    continue
                known.add(entity_id)
                yield entity_id
        return EntityIterator(self._graph, self._src_kind, {}, dedup_iterator(self, aliases), parent=self)

    def without(self, *aliases):
        def without_iterator(entity_iterator, aliases):
            clone_entity_iterator = entity_iterator
            for entity_id in clone_entity_iterator._iterate(on_item=False):  # Iterating updates the context ...
                member = False
                for alias in aliases:
                    excluded_entity = entity_iterator._context.get(alias, UNDEFINED)
                    if excluded_entity._kind == self._src_kind and excluded_entity.get_id() == entity_id:
                        member = True
                        break
                if not member:
                    yield entity_id
        return EntityIterator(self._graph, self._src_kind, {}, without_iterator(self, aliases), parent=self)

    def idata(self, *args, **kwargs):
        for item in self:
            yield item.data(*args, **kwargs)

    def data(self, *args, **kwargs):
        return list(self.idata(*args, **kwargs))

    def limit(self, count):
        def limit_iterator(entity_id_iterator, count):
            counter = 0
            for entity_id in entity_id_iterator:
                if counter < count:
                    yield entity_id
                else:
                    break  # This line is REALLY important (it stops outer iteration)
                counter += 1
        return EntityIterator(self._graph, self._src_kind, {}, limit_iterator(self._iterate(), count), parent=self)

    def aka(self, alias):
        def alias_iterator(entity_id_iterator, alias):
            graph = self._graph
            for entity_id in entity_id_iterator:
                self._context[alias] = graph._item_from_id(self._src_kind, entity_id)
                yield entity_id
        return EntityIterator(self._graph, self._src_kind, {}, alias_iterator(self._iterate(), alias), parent=self)

    # Following methods don't return iterator :

    def count(self):
        return sum(1 for _ in self._iterate())  # space efficient counter

    def remove(self):
        remove_func = self._graph._remove_node if self._src_kind == KIND_VERTEX else self._graph._remove_edge
        for entity_id in self._iterate():
            remove_func(entity_id)

    def update(self, **updates):
        # Do the update (not done while iterating because *every* entity must be updated before starting to yield)
        txn = self._graph._transaction_begin()
        try:
            for entity_id in self._iterate(on_item=False):
                self._graph._bulk_update_data(txn, self._src_kind, entity_id, **updates)
            self._graph._transaction_commit(txn)
        except Exception as e:
            self._graph._transaction_rollback(txn)
            raise GrapheekDataException(repr(e))

    def all(self):
        return list(self)

    def ids(self):
        return list(self._iterate(on_item=False))

    def collect(self, *aliases):
        result = []
        for entity in self:  # Iterating updates the context ...
            result.append([self._context.get(alias, None) for alias in aliases])
        return result

    def sum(self, expr=None, asc=False):
        return EntityAggregate(self._graph, self._src_kind, self, pair_iterator=None, agg='+', expr=expr, asc=asc)

    def percent(self, expr=None, asc=False):
        return EntityAggregate(self._graph, self._src_kind, self, pair_iterator=None, agg='%', expr=expr, asc=asc)

    def order_by(self, *clauses):

        def order_iterator(entity_id_iterator, cmp_func):
            lst = []
            for item in self:
                dic = item.data().copy()
                dic['_id'] = item.get_id()
                lst.append(dic)
            if PYTHON3:
                lst.sort(key=cmp_to_key(cmp_func))
            else:
                lst.sort(cmp_func)
            for dic in lst:
                yield dic['_id']
        cmp_func = build_order_func(*clauses)
        return EntityIterator(self._graph, self._src_kind, {}, order_iterator(order_iterator, cmp_func), parent=self)

    def _context_iterator(self):
        for entity_id in self._iterate():  # Iterating updates the context ...
            yield self._context.copy()

    def call(self, _fname, *aliases, **params):
        # Same as request but ignore return
        self.request(_fname, *aliases, **params)

    def request(self, _fname, *aliases, **params):
        fn = self._graph._get_server_script(_fname)
        return fn(self._graph, self._context_iterator(), *aliases, **params)

    def dot(self, node_label=None, edge_label=None, limit=100):
        return DotExport(self, node_label, edge_label, limit)

    def _dot_str(self, node_label=None, edge_label=None, limit=100):  # pragma : no cover
        return str(DotExport(self, node_label, edge_label, limit))


class Entity(object):

    def __init__(self, entity_id, graph):
        self._entity_id = int(entity_id)  # Depending on backend, sometimes, id are stored as string
        self._graph = graph

    def __repr__(self):
        kind = 'node' if self._kind == KIND_VERTEX else 'edge'
        return '<%s id:%s data:%s>' % (kind, self._entity_id, repr(self.data()))

    def __eq__(self, other):
        return other.__class__ == self.__class__ and other.get_id() == self.get_id() and other._graph == self._graph

    def __ne__(self, other):
        # Cf Python Doc : http://docs.python.org/2/reference/datamodel.html#object.__ne__
        # "When defining __eq__(), one should also define __ne__() so that the operators will behave as expected"
        return not self.__eq__(other)

    def __getattr__(self, attr):
        try:
            return self._graph._get_data(None, self._kind, self._entity_id)[attr]
        except KeyError:
            raise AttributeError("%r instance has no attribute %r" % (self.__class__, attr))

    def __setattr__(self, attr, value):
        if attr.startswith('_'):
            self.__dict__.update({attr: value})
        else:
            self._graph._update_data(self._kind, self._entity_id, attr, value)

    def _neighbors(self, _traversal, _random, **filters):
        return self._graph._neighbors(self._kind, self._entity_id, _traversal, _cache=None, _random=_random, **filters)

    def _jump(self, _tgt_kind, _traversal, _random, *args, **filters):
        jumping_iterator = _random_iterator if _random else _jump_iterator
        # TODO : Factor code with EntityIterator._jump
        count_args = len(args)
        assert(count_args < 2)
        # how many recursive jumps ? :
        count = 1
        if count_args == 1:
            count = args[0]
        current_iterator = self._neighbors(_traversal, _random, **filters)
        parent_kind = self._kind
        current_kind = _tgt_kind
        current_parent = None
        # (potentially) recursive jumping
        for i in range(count):
            IT = current_iterator if i == 0 else jumping_iterator(self._graph, parent_kind, current_iterator._iterate(), _traversal, {}, **filters)
            current_iterator = EntityIterator(
                self._graph,
                current_kind,
                filters,
                IT,
                parent=current_parent
            )
            parent_kind = current_kind
            current_kind = KIND_VERTEX
            current_parent = current_iterator
        return current_iterator

    def get(self, field, default=None):
        try:
            return self.__getattr__(field)
        except AttributeError:
            return default

    def inV(self, *args, **filters):
        return self._jump(KIND_VERTEX, IN_VERTICES_SUFFIX, False, *args, **filters)

    def outV(self, *args, **filters):
        return self._jump(KIND_VERTEX, OUT_VERTICES_SUFFIX, False, *args, **filters)

    def bothV(self, *args, **filters):
        return self._jump(KIND_VERTEX, BOTH_VERTICES_SUFFIX, False, *args, **filters)

    def random(self, _method, **filters):
        try:
            kind, traversal = {
                'inV': (KIND_VERTEX, IN_VERTICES_SUFFIX),
                'outV': (KIND_VERTEX, OUT_VERTICES_SUFFIX),
                'bothV': (KIND_VERTEX, BOTH_VERTICES_SUFFIX)
            }[_method]
        except KeyError:
            raise GrapheekUnknownMethod(_method)
        return self._jump(kind, traversal, True, **filters)

    def data(self, *args, **kwargs):
        return self._graph._get_data(None, self._kind, self._entity_id, *args, **kwargs)

    def get_id(self):
        return self._entity_id

    def update(self, **updates):
        self._graph._bulk_update_data(None, self._kind, self._entity_id, **updates)

    def aka(self, alias):
        def self_iterator(entity_id):
            yield entity_id
        return EntityIterator(self._graph, self._kind, {}, self_iterator(self.get_id()), context={alias: self})


class Node(Entity):

    def __init__(self, entity_id, graph):
        super(Node, self).__init__(entity_id, graph)
        self._kind = KIND_VERTEX

    def __change_denorm_data(self, lst_method, counter_method, *counter_args):
        root_key = KIND_VERTEX + '/' + str(self._entity_id) + '/'
        # traversal denormalized lists
        lst_method(root_key + IN_EDGES_SUFFIX)
        lst_method(root_key + IN_VERTICES_SUFFIX)
        lst_method(root_key + OUT_EDGES_SUFFIX)
        lst_method(root_key + OUT_VERTICES_SUFFIX)
        lst_method(root_key + BOTH_EDGES_SUFFIX)
        lst_method(root_key + BOTH_VERTICES_SUFFIX)
        # traversal denormalized counters
        counter_method(root_key + IN_EDGES_SUFFIX + '/' + COUNT_SUFFIX, *counter_args)
        counter_method(root_key + IN_VERTICES_SUFFIX + '/' + COUNT_SUFFIX, *counter_args)
        counter_method(root_key + OUT_EDGES_SUFFIX + '/' + COUNT_SUFFIX, *counter_args)
        counter_method(root_key + OUT_VERTICES_SUFFIX + '/' + COUNT_SUFFIX, *counter_args)
        counter_method(root_key + BOTH_EDGES_SUFFIX + '/' + COUNT_SUFFIX, *counter_args)
        counter_method(root_key + BOTH_VERTICES_SUFFIX + '/' + COUNT_SUFFIX, *counter_args)

    def _add_denorm_data(self):
        operation = Addition()
        self.__change_denorm_data(operation.init_lst, operation.set, 0)
        return operation

    def _remove_denorm_data(self):
        operation = Removal()
        self.__change_denorm_data(operation.remove, operation.remove)
        return operation

    def _add_denorm_src_data(self, target, edge):
        source_id = self._entity_id
        target_id = target.get_id()
        edge_id = edge.get_id()
        root_key = KIND_VERTEX + '/' + str(source_id) + '/'
        s_edge_id = str(edge_id)
        s_target_id = str(target_id)
        # building operation :
        operation = Addition()
        # lists
        operation.append_to_lst(root_key + OUT_EDGES_SUFFIX, edge_id)
        operation.append_to_lst(root_key + BOTH_EDGES_SUFFIX, edge_id)
        operation.append_to_lst(root_key + OUT_VERTICES_SUFFIX, target_id)
        operation.append_to_lst(root_key + BOTH_VERTICES_SUFFIX, target_id)
        # counters
        operation.update_inc(root_key + OUT_EDGES_SUFFIX + '/' + COUNT_SUFFIX)
        operation.update_inc(root_key + BOTH_EDGES_SUFFIX + '/' + COUNT_SUFFIX)
        operation.update_inc(root_key + OUT_VERTICES_SUFFIX + '/' + COUNT_SUFFIX)
        operation.update_inc(root_key + BOTH_VERTICES_SUFFIX + '/' + COUNT_SUFFIX)
        # create a key/value for links between source and target (aimed to be used with indexes)
        operation.set(root_key + OUT_EDGES_SUFFIX + '/' + s_edge_id, 1)
        operation.set(root_key + BOTH_EDGES_SUFFIX + '/' + s_edge_id, 1)
        operation.set(root_key + OUT_VERTICES_SUFFIX + '/' + s_target_id, 1)
        operation.set(root_key + BOTH_VERTICES_SUFFIX + '/' + s_target_id, 1)
        # ---
        return operation

    def _remove_denorm_src_data(self, target, edge):
        source_id = self._entity_id
        target_id = target.get_id()
        edge_id = edge.get_id()
        root_key = KIND_VERTEX + '/' + str(source_id) + '/'
        s_edge_id = str(edge_id)
        s_target_id = str(target_id)
        # building operation :
        operation = Removal()
        # lists
        operation.remove_from_lst(root_key + OUT_EDGES_SUFFIX, edge_id)
        operation.remove_from_lst(root_key + BOTH_EDGES_SUFFIX, edge_id)
        operation.remove_from_lst(root_key + OUT_VERTICES_SUFFIX, target_id)
        operation.remove_from_lst(root_key + BOTH_VERTICES_SUFFIX, target_id)
        # counters
        operation.update_dec(root_key + OUT_EDGES_SUFFIX + '/' + COUNT_SUFFIX)
        operation.update_dec(root_key + BOTH_EDGES_SUFFIX + '/' + COUNT_SUFFIX)
        operation.update_dec(root_key + OUT_VERTICES_SUFFIX + '/' + COUNT_SUFFIX)
        operation.update_dec(root_key + BOTH_VERTICES_SUFFIX + '/' + COUNT_SUFFIX)
        # create a key/value for links between source and target (aimed to be used with indexes)
        operation.remove(root_key + OUT_EDGES_SUFFIX + '/' + s_edge_id)
        operation.remove(root_key + BOTH_EDGES_SUFFIX + '/' + s_edge_id)
        operation.remove(root_key + OUT_VERTICES_SUFFIX + '/' + s_target_id)
        operation.remove(root_key + BOTH_VERTICES_SUFFIX + '/' + s_target_id)
        # ---
        return operation

    def _add_denorm_tgt_data(self, source, edge):
        target_id = self._entity_id
        source_id = source.get_id()
        edge_id = edge.get_id()
        root_key = KIND_VERTEX + '/' + str(target_id) + '/'
        s_edge_id = str(edge_id)
        s_source_id = str(source_id)
        # building operation :
        operation = Addition()
        # lists
        operation.append_to_lst(root_key + IN_EDGES_SUFFIX, edge_id)
        operation.append_to_lst(root_key + BOTH_EDGES_SUFFIX, edge_id)
        operation.append_to_lst(root_key + IN_VERTICES_SUFFIX, source_id)
        operation.append_to_lst(root_key + BOTH_VERTICES_SUFFIX, source_id)
        # counters
        operation.update_inc(root_key + IN_EDGES_SUFFIX + '/' + COUNT_SUFFIX)
        operation.update_inc(root_key + BOTH_EDGES_SUFFIX + '/' + COUNT_SUFFIX)
        operation.update_inc(root_key + IN_VERTICES_SUFFIX + '/' + COUNT_SUFFIX)
        operation.update_inc(root_key + BOTH_VERTICES_SUFFIX + '/' + COUNT_SUFFIX)
        # create a key/value for links between source and target (aimed to be used with indexes)
        operation.set(root_key + IN_EDGES_SUFFIX + '/' + s_edge_id, 1)
        operation.set(root_key + BOTH_EDGES_SUFFIX + '/' + s_edge_id, 1)
        operation.set(root_key + IN_VERTICES_SUFFIX + '/' + s_source_id, 1)
        operation.set(root_key + BOTH_VERTICES_SUFFIX + '/' + s_source_id, 1)
        # ---
        return operation

    def _remove_denorm_tgt_data(self, source, edge):
        target_id = self._entity_id
        source_id = source.get_id()
        edge_id = edge.get_id()
        root_key = KIND_VERTEX + '/' + str(target_id) + '/'
        s_edge_id = str(edge_id)
        s_source_id = str(source_id)
        # building operation :
        operation = Removal()
        # lists
        operation.remove_from_lst(root_key + IN_EDGES_SUFFIX, edge_id)
        operation.remove_from_lst(root_key + BOTH_EDGES_SUFFIX, edge_id)
        operation.remove_from_lst(root_key + IN_VERTICES_SUFFIX, source_id)
        operation.remove_from_lst(root_key + BOTH_VERTICES_SUFFIX, source_id)
        # counters
        operation.update_dec(root_key + IN_EDGES_SUFFIX + '/' + COUNT_SUFFIX)
        operation.update_dec(root_key + BOTH_EDGES_SUFFIX + '/' + COUNT_SUFFIX)
        operation.update_dec(root_key + IN_VERTICES_SUFFIX + '/' + COUNT_SUFFIX)
        operation.update_dec(root_key + BOTH_VERTICES_SUFFIX + '/' + COUNT_SUFFIX)
        # create a key/value for links between source and target (aimed to be used with indexes)
        operation.remove(root_key + IN_EDGES_SUFFIX + '/' + s_edge_id)
        operation.remove(root_key + BOTH_EDGES_SUFFIX + '/' + s_edge_id)
        operation.remove(root_key + IN_VERTICES_SUFFIX + '/' + s_source_id)
        operation.remove(root_key + BOTH_VERTICES_SUFFIX + '/' + s_source_id)
        # ---
        return operation

    def in_(self, *args, **filters):
        def self_iterator(entity_id):
            yield entity_id
        entity_iterator = EntityIterator(self._graph, KIND_VERTEX, {}, self_iterator(self.get_id()), context={})
        return entity_iterator.in_(*args, **filters)

    def out_(self, *args, **filters):
        def self_iterator(entity_id):
            yield entity_id
        entity_iterator = EntityIterator(self._graph, KIND_VERTEX, {}, self_iterator(self.get_id()), context={})
        return entity_iterator.out_(*args, **filters)

    def both_(self, *args, **filters):
        def self_iterator(entity_id):
            yield entity_id
        entity_iterator = EntityIterator(self._graph, KIND_VERTEX, {}, self_iterator(self.get_id()), context={})
        return entity_iterator.both_(*args, **filters)

    def inE(self, **filters):
        return self._jump(KIND_EDGE, IN_EDGES_SUFFIX, False, **filters)

    def outE(self, **filters):
        return self._jump(KIND_EDGE, OUT_EDGES_SUFFIX, False, **filters)

    def bothE(self, **filters):
        return self._jump(KIND_EDGE, BOTH_EDGES_SUFFIX, False, **filters)

    def random(self, _method, **filters):
        try:
            kind, traversal = {
                'inV': (KIND_VERTEX, IN_VERTICES_SUFFIX),
                'outV': (KIND_VERTEX, OUT_VERTICES_SUFFIX),
                'bothV': (KIND_VERTEX, BOTH_VERTICES_SUFFIX),
                'inE': (KIND_EDGE, IN_EDGES_SUFFIX),
                'outE': (KIND_EDGE, OUT_EDGES_SUFFIX),
                'bothE': (KIND_EDGE, BOTH_EDGES_SUFFIX)
            }[_method]
        except KeyError:
            raise GrapheekUnknownMethod(_method)
        return self._jump(kind, traversal, True, **filters)

    def remove(self):
        self._graph._remove_node(self._entity_id)


class Edge(Entity):

    def __init__(self, entity_id, graph):
        super(Edge, self).__init__(entity_id, graph)
        self._kind = KIND_EDGE

    def _src_id(self):
        return self._graph._get_lst(None, build_key(KIND_EDGE, self._entity_id, IN_VERTICES_SUFFIX))[0]

    def _tgt_id(self):
        return self._graph._get_lst(None, build_key(KIND_EDGE, self._entity_id, OUT_VERTICES_SUFFIX))[0]

    def _add_denorm_data(self, source, target):
        edge_id = self._entity_id
        source_id = source.get_id()
        target_id = target.get_id()
        root_key = KIND_EDGE + '/' + str(edge_id) + '/'
        s_source_id = str(source_id)
        s_target_id = str(target_id)
        # building operation :
        operation = Addition()
        # traversal denormalized lists
        operation.append_to_lst(root_key + IN_VERTICES_SUFFIX, source_id)
        operation.append_to_lst(root_key + OUT_VERTICES_SUFFIX, target_id)
        operation.append_to_lst(root_key + BOTH_VERTICES_SUFFIX, source_id)
        operation.append_to_lst(root_key + BOTH_VERTICES_SUFFIX, target_id)
        # create a key/value for links between source, target and current edge (aimed to be used with indexes)
        operation.set(root_key + IN_VERTICES_SUFFIX + '/' + s_source_id, 1)
        operation.set(root_key + OUT_VERTICES_SUFFIX + '/' + s_target_id, 1)
        operation.set(root_key + BOTH_VERTICES_SUFFIX + '/' + s_source_id, 1)
        operation.set(root_key + BOTH_VERTICES_SUFFIX + '/' + s_target_id, 1)
        # ---
        return operation

    def _remove_denorm_data(self, source, target):
        edge_id = self._entity_id
        source_id = source.get_id()
        target_id = target.get_id()
        root_key = KIND_EDGE + '/' + str(edge_id) + '/'
        s_source_id = str(source_id)
        s_target_id = str(target_id)
        # building operation
        operation = Removal()
        # traversal denormalized lists
        operation.remove(root_key + IN_VERTICES_SUFFIX)
        operation.remove(root_key + OUT_VERTICES_SUFFIX)
        operation.remove(root_key + BOTH_VERTICES_SUFFIX)
        operation.remove(root_key + BOTH_VERTICES_SUFFIX)
        # remove key/value for links between source, target and current edge (aimed to be used with indexes)
        operation.remove(root_key + IN_VERTICES_SUFFIX + '/' + s_source_id)
        operation.remove(root_key + OUT_VERTICES_SUFFIX + '/' + s_target_id)
        operation.remove(root_key + BOTH_VERTICES_SUFFIX + '/' + s_source_id)
        operation.remove(root_key + BOTH_VERTICES_SUFFIX + '/' + s_target_id)
        # ---
        return operation

    def remove(self):
        self._graph._remove_edge(self._entity_id)


class BaseGraph(GraphReadWrite, GraphNx):

    def close(self):
        if not self._closed:
            self._db_close()
            self._closed = True

    def __initialize__(self):
        # "Rebuilding" indexes :
        txn = self._transaction_begin()
        try:
            # Get vertex and edge count (useful for queries where we need to compare index perf and seq scan) :
            self._node_count = self._get(txn, METADATA_VERTEX_COUNTER) - self._get(txn, METADATA_VERTEX_REMOVED_COUNTER)
            self._edge_count = self._get(txn, METADATA_EDGE_COUNTER) - self._get(txn, METADATA_EDGE_REMOVED_COUNTER)
            # Instantiating vertex and edge indexes
            self._node_indexes = []
            self._edge_indexes = []
            #   1st vertex indexes :
            index_count = self._get(txn, METADATA_VERTEX_INDEX_COUNTER)
            for index_id in range(0, index_count):
                fields, filter_tuples = self._get(txn, build_key(METADATA_VERTEX_INDEX_FIELDS_PREFIX, index_id))
                filters = dict(filter_tuples)
                if fields != UNDEFINED:
                    prefix = build_key(KIND_INDEX, KIND_VERTEX, index_id)
                    self._node_indexes.append(ExactIndex(self, KIND_VERTEX, prefix, *fields, **filters))
            #   2nd edge indexes (~ same code, but I'm not sure it makes sense to factor...)
            index_count = self._get(txn, METADATA_EDGE_INDEX_COUNTER)
            for index_id in range(0, index_count):
                fields, filter_tuples = self._get(txn, build_key(METADATA_EDGE_INDEX_FIELDS_PREFIX, index_id))
                filters = dict(filter_tuples)
                if fields != UNDEFINED:
                    prefix = build_key(KIND_INDEX, KIND_EDGE, index_id)
                    self._edge_indexes.append(ExactIndex(self, KIND_EDGE, prefix, *fields, **filters))
            self._transaction_commit(txn)
        except Exception as e:  # pragma : no cover
            self._transaction_rollback(txn)
            raise GrapheekDataException(repr(e))
        # Instantiate an optimizer (which will choose between scan and sequential scan AND (in the future) cache best index for most runned queries)
        self._optimizer = Optimizer(self)

    def _prepare_database(self, txn):
        self._set(txn, PREPARED, 1)
        self._set(txn, METADATA_VERTEX_COUNTER, 0)
        self._set(txn, METADATA_VERTEX_INDEX_COUNTER, 0)
        self._set(txn, METADATA_VERTEX_REMOVED_COUNTER, 0)
        self._set(txn, METADATA_EDGE_COUNTER, 0)
        self._set(txn, METADATA_EDGE_REMOVED_COUNTER, 0)
        self._set(txn, METADATA_EDGE_INDEX_COUNTER, 0)

    def _ensure_prepared(self):
        if not(self._has_key(PREPARED)):
            txn = self._transaction_begin()
            try:
                self._prepare_database(txn)
                self._transaction_commit(txn)
            except Exception as e:
                self._transaction_rollback(txn)
                raise GrapheekDataPreparationFailedException(repr(e))
        self.__initialize__()

    # Following methods MUST be overriden by child classes :

    def _db_close(self):
        raise NotImplementedError

    def _transaction_begin(self):
        raise NotImplementedError

    def _transaction_commit(self, handle):
        raise NotImplementedError

    def _transaction_rollback(self, handle):
        raise NotImplementedError

    def _has_key(self, key):
        raise NotImplementedError

    def _get(self, txn, key):
        raise NotImplementedError

    def _bulk_get(self, txn, keys):
        raise NotImplementedError

    def _set(self, key, value):
        raise NotImplementedError

    def _bulk_set(self, key, value):
        raise NotImplementedError

    def _remove(self, *key):
        raise NotImplementedError

    def _bulk_remove(self, txn, keys):
        raise NotImplementedError

    def _remove_prefix(self, prefix):
        raise NotImplementedError

    # For next methods, default implementation MAY suffice :
    # BUT overriding them in child classes is a path to performance :)
    # See for instance, kyotocab.KyotoCabinetGraph _init_lst, _get_lst & _append_to_lst overriding

    def _init_lst(self, txn, key):
        # Create an empty list
        self._set(txn, key, [])

    def _get_lst(self, txn, key):
        res = self._get(txn, key)  # if _get returns UNDEFINED, that's also ok, I return it...
        return res

    def _set_lst(self, txn, key, values):
        self._set(txn, key, values)

    def _bulk_get_lst(self, txn, keys):
        return [self._get(txn, key) for key in keys]

    def _append_to_lst(self, txn, key, value):
        lst = self._get(txn, key)
        if lst == UNDEFINED:
            lst = []
        self._set(txn, key, lst + [value])

    def _bulk_append_to_lst(self, txn, key, values):
        lst = self._get(txn, key)
        if lst == UNDEFINED:
            lst = []
        self._set(txn, key, lst + values)

    def _remove_from_lst(self, txn, key, value):
        old = self._get(txn, key)
        # Caution : we are only removing ONE occurence
        # This is voluntary
        # For instance, it lst contains neighbour node, we need to remove only one occurence
        # cause current entity and neighbour node can be linked multiple time
        new = old[:]
        new.remove(value)
        self._set(txn, key, new)

    def _bulk_remove_from_lst(self, txn, key, values):
        old = self._get(txn, key)
        new = old[:]
        for value in values:
            new.remove(value)
        self._set(txn, key, new)

    def _update_inc(self, txn, key, value=1):
        oldval_ = self._get(txn, key)
        if oldval_ == UNDEFINED:
            raise GrapheekMissingKeyException
        oldval = int(oldval_)
        newval = oldval + value
        self._set(txn, key, newval)
        return newval

    def _update_dec(self, txn, key, value=1):
        return self._update_inc(txn, key, value * -1)

    def _new_id_for_key(self, txn, key):
        return self._update_inc(txn, key) - 1  # -1 so that entity idx starts at 0

    # Sub data update (no need to override, IMHO)
    # Remark that we are using kind, id instead of key (key is a *key* of the Key Value Store, id is an entity id (node id or edge id for instance))

    def _get_data(self, txn, kind, entity_id, *args, **kwargs):
        key = build_key(kind, entity_id, DATA_SUFFIX)
        result = self._get(txn, key)  # if _get returns UNDEFINED, that's also ok, I return it...
        if args:
            tmpres = dict((k, v) for k, v in result.items() if k in args)
            result = tmpres
            if kwargs:
                flat = kwargs.get('flat', False)
                if flat:
                    tmpres = [result[k] for k in args]
                    result = tmpres
        return result

    def _bulk_update_data(self, _txn, _kind, _entity_id, **updates):
        check_valid_data(updates)
        release_txn = False
        if _txn is None:
            _txn = self._transaction_begin()
            release_txn = True
        try:
            data = self._get_data(_txn, _kind, _entity_id)
            # Remove entity from indexes before updating data (we'll re-add it just after with new values)
            self._remove_from_all_entity_indexes(_txn, _kind, _entity_id)
            # Do the update :
            data.update(**updates)
            key = build_key(_kind, _entity_id, DATA_SUFFIX)
            self._set(_txn, key, data)
            # Readding entity to indexes :
            self._add_to_all_entity_indexes(_txn, _kind, _entity_id, data)
            if release_txn:
                self._transaction_commit(_txn)
        except Exception as e:
            if release_txn:
                self._transaction_rollback(_txn)
            raise GrapheekDataException(repr(e))

    def _update_data(self, kind, entity_id, subkey, value):
        self._bulk_update_data(None, kind, entity_id, **{subkey: value})  # Don't create a txn, it will be created by _bulk_update_data

    # DON'T OVERRIDE THOSE METHODS :

    def _get_server_script(self, _fname):
        # 1st, get function from cache :
        func = self._script_cache.get(_fname, None)
        if func is None:
            for module in self._script_module_cache:
                try:
                    func = getattr(module, _fname)
                except AttributeError:
                    pass  # it may not exist in module but may exist in exist...
                if func is not None:
                    # Updating cache and stop iteration
                    self._script_cache[_fname] = func
                    break
        if func is None:
            # not found -> raise an exception to warn user
            raise GrapheekUnknownScriptException('%s method cannot be found' % (_fname,))
        return func

    def _add_node(self, txn, data, node_id=None):
        assert(isinstance(data, dict))
        release_txn = False
        if txn is None:
            release_txn = True
            txn = self._transaction_begin()
        try:
            operation = Addition()
            if node_id is None:
                node_id = self._new_id_for_key(txn, METADATA_VERTEX_COUNTER)
            node = Node(node_id, self)
            operation.set(build_key(KIND_VERTEX, node_id, DATA_SUFFIX), data.copy())
            operation.merge(node._add_denorm_data())
            # Adding node id to proper node ids list :
            operation.append_to_lst(build_key(METADATA_VERTEX_ID_LIST_PREFIX, node_id // CHUNK_SIZE), node_id)
            if release_txn:
                # Applying operation (before updating indexes as indexes needs key to be updated)
                operation.apply(txn, self)
                # Updating vertex indexes :
                self._add_to_all_entity_indexes(txn, KIND_VERTEX, node_id, data)
                # Updating denorm node count
                self._node_count += 1
                self._transaction_commit(txn)
                return node
            else:
                return operation
        except Exception as e:
            if release_txn:
                self._transaction_rollback(txn)
            raise GrapheekDataException(repr(e))

    def _bulk_add_node(self, node_defns, silent=False):
        txn = self._transaction_begin()
        try:
            operation = Addition()
            node_count = len(node_defns)
            last_node_id = self._update_inc(txn, METADATA_VERTEX_COUNTER, node_count) - 1
            first_node_id = last_node_id - node_count + 1
            range_ids = list(range(first_node_id, last_node_id + 1))
            for node_id, data in zip(range_ids, node_defns):
                operation.merge(self._add_node(txn, data, node_id))
            operation.apply(txn, self)
            # now updating indexes :
            self._bulk_add_to_all_entity_indexes(txn, KIND_VERTEX, range_ids)
            # update denormalized node count
            self._node_count += node_count
            # all is ok, commit :)
            self._transaction_commit(txn)
            if not(silent):
                res = [Node(node_id, self) for node_id in range_ids]
                return res
        except Exception as e:
            self._transaction_rollback(txn)
            raise GrapheekDataException(repr(e))

    def _remove_node(self, node_id):
        txn = self._transaction_begin()
        try:
            operation = Removal()
            node = Node(node_id, self)
            # edges attached to this node must be removed before removing any node data
            # (this will infer some denorm data modifications on related outer and inner nodes)
            out_edge_ids = self._get_lst(txn, build_key(KIND_VERTEX, node_id, OUT_EDGES_SUFFIX))
            for out_edge_id in out_edge_ids:
                operation.merge(self._remove_edge(out_edge_id, txn=txn))
            in_edge_ids = self._get_lst(txn, build_key(KIND_VERTEX, node_id, IN_EDGES_SUFFIX))
            for in_edge_id in in_edge_ids[:]:
                operation.merge(self._remove_edge(in_edge_id, txn=txn))
            # now, removing node related data and denorm data :
            operation.merge(node._remove_denorm_data())
            # Removing node id from proper node ids list :
            operation.remove_from_lst(build_key(METADATA_VERTEX_ID_LIST_PREFIX, int(node_id) // CHUNK_SIZE), node_id)
            # Updating vertex indexes before removing data
            self._remove_from_all_entity_indexes(txn, KIND_VERTEX, node_id)
            # Now removing data
            operation.remove(build_key(KIND_VERTEX, node_id, DATA_SUFFIX))
            # Incrementing removed vertex counter :
            operation.update_inc(METADATA_VERTEX_REMOVED_COUNTER)
            # Applying operation
            operation.apply(txn, self)
            # Update denorm counter :
            self._node_count -= 1
            self._transaction_commit(txn)
        except Exception as e:
            self._transaction_rollback(txn)
            raise GrapheekDataException(repr(e))

    def _add_edge(self, txn, source, target, data=None, edge_id=None):
        assert(isinstance(data, dict))
        release_txn = False
        if txn is None:
            release_txn = True
            txn = self._transaction_begin()
        try:
            operation = Addition()
            if edge_id is None:
                edge_id = self._new_id_for_key(txn, METADATA_EDGE_COUNTER)
            data = data or {}
            # Initializing
            operation.init_lst(build_key(KIND_EDGE, edge_id, IN_VERTICES_SUFFIX))
            operation.init_lst(build_key(KIND_EDGE, edge_id, OUT_VERTICES_SUFFIX))
            operation.init_lst(build_key(KIND_EDGE, edge_id, BOTH_VERTICES_SUFFIX))
            # Persistence and denormalization - START
            operation.set(build_key(KIND_EDGE, edge_id, DATA_SUFFIX), data)
            # Adding edge id to proper edge ids list :
            operation.append_to_lst(build_key(METADATA_EDGE_ID_LIST_PREFIX, int(edge_id) // CHUNK_SIZE), edge_id)
            # Edge data denormalization
            edge = Edge(edge_id, self)
            operation.merge(edge._add_denorm_data(source, target))
            # source data denormalization
            operation.merge(source._add_denorm_src_data(target, edge))
            # target data denormalization
            operation.merge(target._add_denorm_tgt_data(source, edge))
            if release_txn:
                # Applying operation (before updating indexes as indexes needs key to be updated)
                operation.apply(txn, self)
                # Updating edge indexes :
                self._add_to_all_entity_indexes(txn, KIND_EDGE, edge_id, data)
                # Updating edge count denorm :
                self._edge_count += 1
                self._transaction_commit(txn)
                # Persistence and denormalization - END
                return edge
            else:
                return operation
        except Exception as e:
            if release_txn:
                self._transaction_rollback(txn)
            raise GrapheekDataException(repr(e))

    def _bulk_add_edge(self, edge_defns, silent=False):
        txn = self._transaction_begin()
        try:
            operation = Addition()
            edge_count = len(edge_defns)
            last_edge_id = self._update_inc(txn, METADATA_EDGE_COUNTER, edge_count) - 1
            first_edge_id = last_edge_id - edge_count + 1
            range_ids = list(range(first_edge_id, last_edge_id + 1))
            for edge_id, edge_defn in zip(range_ids, edge_defns):
                source, target, data = edge_defn
                operation.merge(self._add_edge(txn, source, target, data, edge_id=edge_id))
            operation.apply(txn, self)
            # now updating indexes :
            self._bulk_add_to_all_entity_indexes(txn, KIND_EDGE, range_ids)
            # update denormalized node count
            self._edge_count += edge_count
            # all is ok, commit :)
            self._transaction_commit(txn)
            if not(silent):
                return [Edge(edge_id, self) for edge_id in range_ids]
        except Exception as e:
            self._transaction_rollback(txn)
            raise GrapheekDataException(repr(e))

    def _bulk_add_edge_by_ids(self, edge_id_defns, silent=False):
        for _, _, data in edge_id_defns:
            check_valid_data(data)
        edge_defns = [(self.v(source_id), self.v(target_id), data) for source_id, target_id, data in edge_id_defns]
        return self._bulk_add_edge(edge_defns, silent)

    def _add_edge_by_ids(self, source_id, target_id, data=None):
        source = Node(source_id, self)
        target = Node(target_id, self)
        return self._add_edge(None, source, target, data)

    def _remove_edge(self, edge_id, txn=None):
        def remove_vertex_denorm_data(node, other, node_suffix):
            rm_denorm_method = node._remove_denorm_src_data if node_suffix == OUT_VERTICES_SUFFIX else node._remove_denorm_tgt_data
            return rm_denorm_method(other, edge)

        release_txn = False
        if txn is None:
            release_txn = True
            txn = self._transaction_begin()
        try:
            operation = Removal()
            edge = Edge(edge_id, self)
            source = target = None
            source_id = self._get_lst(txn, build_key(KIND_EDGE, edge_id, IN_VERTICES_SUFFIX))[0]
            source = Node(source_id, self)
            target_id = self._get_lst(txn, build_key(KIND_EDGE, edge_id, OUT_VERTICES_SUFFIX))[0]
            target = Node(target_id, self)
            operation.merge(remove_vertex_denorm_data(source, target, OUT_VERTICES_SUFFIX))
            operation.merge(remove_vertex_denorm_data(target, source, IN_VERTICES_SUFFIX))
            # Removing edge id from proper edge ids list :
            operation.remove_from_lst(build_key(METADATA_EDGE_ID_LIST_PREFIX, edge_id // CHUNK_SIZE), edge_id)
            # Removing edge related lists
            operation.remove(build_key(KIND_EDGE, edge_id, IN_VERTICES_SUFFIX))
            operation.remove(build_key(KIND_EDGE, edge_id, OUT_VERTICES_SUFFIX))
            operation.remove(build_key(KIND_EDGE, edge_id, BOTH_VERTICES_SUFFIX))
            # Updating edge indexes before removing data
            self._remove_from_all_entity_indexes(txn, KIND_EDGE, edge_id)
            # Now removing edge data
            operation.remove(build_key(KIND_EDGE, edge_id, DATA_SUFFIX))
            # Removing edge denormalized data
            assert((source or target) is not None)  # Both source and target must NOT be None
            operation.merge(edge._remove_denorm_data(source, target))
            # Incrementing removed edge counter :
            operation.update_inc(METADATA_EDGE_REMOVED_COUNTER)
            # Updating edge counter denorm :
            self._edge_count -= 1
            if release_txn:
                operation.apply(txn, self)
                self._transaction_commit(txn)
            else:
                return operation
        except Exception as e:
            if release_txn:
                self._transaction_rollback(txn)
            raise GrapheekDataException(repr(e))

    # Item id -> Item helper

    def _item_from_id(self, kind, entity_id):
        Klass = Node if kind == KIND_VERTEX else Edge
        return Klass(entity_id, self)

    # Traversals given entity ids :

    def _neighbors(self, _kind, _entity_id, _traversal, _cache=None, _random=False, **filters):
        """
        _neighbors is using a cache (transmitted by iterator) to speed up lookup
        This is done to asymptotically improve performance at the expense of local memory
        Why asymptotically ? because for short path traversal, cache has few probability to be hitted
        On the contrary, for "long path", probability is greater

        WARNING : filter is only used to check if index can be used
        It is not used to really filter neighbors (which is the EntityIterator responsibily)
        So don't be confused by this function signature :)
        """
        # seq scan on neighbours :
        #   potentially using cache
        #   building neighbour_ids
        has_cache = _cache is not None
        neighbour_ids = None
        if has_cache:
            cache_key = (_kind, _entity_id, _traversal)
            neighbour_ids = _cache.get(cache_key, None)
        if neighbour_ids is None:
            # no cache..
            key = build_key(_kind, _entity_id, _traversal)
            neighbour_ids = self._get_lst(None, key)
            if has_cache:
                _cache[cache_key] = neighbour_ids
        # Special case when filters is empty or kind is edge (no need to look in index in the case of edge)
        if (_kind == KIND_EDGE) or not filters:
            if not(_random):
                for entity_id in iter(neighbour_ids):
                    yield entity_id
            else:
                yield random.choice(neighbour_ids)
        else:
            # Preparing metrics that will allow to choose between sequential access and index access :
            iterator = None
            index_count = len(self._node_indexes) if _kind == KIND_VERTEX else len(self._edge_indexes)
            seq_count = self._get(None, build_key(_kind, _entity_id, _traversal, COUNT_SUFFIX))
            # querying index needs <index_count> key access
            # challenging seq_count
            if seq_count > 2 * index_count:
                # ---
                # Why "2" in next line ?
                # Because if an index use appears to be better than index scan, we will have
                # to challenge every indexed ids to check that it is among current entity neighbours
                # so it will lead to
                # which can cost a lot (<index_count> disk access for persistent backends)
                _traversal_kind = {
                    IN_VERTICES_SUFFIX: KIND_VERTEX,
                    OUT_VERTICES_SUFFIX: KIND_VERTEX,
                    BOTH_VERTICES_SUFFIX: KIND_VERTEX,
                    IN_EDGES_SUFFIX: KIND_EDGE,
                    OUT_EDGES_SUFFIX: KIND_EDGE,
                    BOTH_EDGES_SUFFIX: KIND_EDGE,
                }[_traversal]
                iterator = self._optimizer.index_or_seq_scan_iterator(_traversal_kind, seq_count * 2, **filters)
            if iterator is None:
                if not(_random):
                    for entity_id in iter(neighbour_ids):
                        yield entity_id
                else:
                    yield random.choice(neighbour_ids)
            else:
                indexed_ids = set(iterator)
                if not(_random):
                    for entity_id in indexed_ids.intersection(neighbour_ids):
                        yield entity_id
                else:  # pragma : no cover
                    yield random.choice(indexed_ids.intersection(neighbour_ids))

    # Don't override next methods : it's the public interface
    # (or some "private" helper methods for public methods)

    def __X(self, _kind, **filters):

        if self._closed:
            raise GrapheekDataException('Graph is closed')
        if _kind == KIND_VERTEX:
            seq_count = self._node_count
        else:
            seq_count = self._edge_count
        iterator = self._optimizer.index_or_seq_scan_iterator(_kind, seq_count, **filters)
        if iterator is None:  # no index is better than sequential scan, so choose it
            iterator = self._optimizer.get_kind_ids(None, _kind)

        return EntityIterator(self, _kind, filters, iterator)

    def V(self, **filters):
        return self.__X(KIND_VERTEX, **filters)

    def E(self, **filters):
        return self.__X(KIND_EDGE, **filters)

    def v(self, id):
        if self._closed:  # pragma : no cover
            raise GrapheekDataException('Graph is closed')
        return Node(id, self)

    def e(self, id):
        if self._closed:  # pragma : no cover
            raise GrapheekDataException('Graph is closed')
        return Edge(id, self)

    def add_node(self, **kwargs):
        check_valid_data(kwargs)
        return self._add_node(None, kwargs.copy())

    def bulk_add_node(self, node_defns, silent=False):
        for data in node_defns:
            check_valid_data(data)
        node_defns_copy = [node_defn.copy() for node_defn in node_defns]
        return self._bulk_add_node(node_defns_copy, silent)

    def add_edge(self, _source, _target, **kwargs):
        check_base_node(_source)
        check_base_node(_target)
        check_valid_data(kwargs)
        return self._add_edge(None, _source, _target, kwargs.copy())

    def bulk_add_edge(self, edge_defns, silent=False):
        try:
            for source, target, data in edge_defns:
                check_base_node(source)
                check_base_node(target)
                check_valid_data(data)
        except (TypeError, ValueError) as e:
            raise GrapheekDataException(repr(e))
        edge_defns_copy = [(source, target, data.copy()) for source, target, data in edge_defns]
        return self._bulk_add_edge(edge_defns_copy, silent)

    def bulk_add_edge_by_ids(self, edge_id_defns, silent=False):
        return self._bulk_add_edge_by_ids(edge_id_defns, silent)

    def add_edge_by_ids(self, _source_id, _target_id, **kwargs):
        return self._add_edge_by_ids(_source_id, _target_id, kwargs)

    def update_data(self, kind, entity_id, attr, value):
        self._update_data(kind, entity_id, attr, value)

    def _add_to_all_entity_indexes(self, txn, kind, entity_id, data):
        indexes = self._node_indexes if kind == KIND_VERTEX else self._edge_indexes
        for index in indexes:
            index.add(txn, entity_id, data)

    def _bulk_add_to_all_entity_indexes(self, txn, kind, entity_ids):
        indexes = self._node_indexes if kind == KIND_VERTEX else self._edge_indexes
        for index in indexes:
            index.bulk_add(txn, iter(entity_ids))

    def _remove_from_all_entity_indexes(self, txn, kind, entity_id):
        indexes = self._node_indexes if kind == KIND_VERTEX else self._edge_indexes
        for index in indexes:
            index.remove(txn, entity_id)

    def _add_entity_index(self, _kind, *args, **filters):
        fields = list(args)
        fields.sort()
        filter_tuples = list(filters.items())
        filter_tuples.sort()
        index_signature = [fields, filter_tuples]
        index_signature_string = normalize_value(index_signature)
        assert(_kind in (KIND_VERTEX, KIND_EDGE))
        if _kind == KIND_VERTEX:
            METADATA_INDEX_PREFIX = METADATA_VERTEX_INDEX_PREFIX
            METADATA_INDEX_COUNTER = METADATA_VERTEX_INDEX_COUNTER
            METADATA_COUNTER = METADATA_VERTEX_COUNTER
            METADATA_INDEX_LIST = METADATA_VERTEX_INDEX_LIST
            METADATA_INDEX_FIELDS_PREFIX = METADATA_VERTEX_INDEX_FIELDS_PREFIX
            entity_indexes = self._node_indexes
        else:
            METADATA_INDEX_PREFIX = METADATA_EDGE_INDEX_PREFIX
            METADATA_INDEX_COUNTER = METADATA_EDGE_INDEX_COUNTER
            METADATA_COUNTER = METADATA_EDGE_COUNTER
            METADATA_INDEX_LIST = METADATA_EDGE_INDEX_LIST
            METADATA_INDEX_FIELDS_PREFIX = METADATA_EDGE_INDEX_FIELDS_PREFIX
            entity_indexes = self._edge_indexes
        # Checking if index already exists (won't create if it is the case) :
        index_key = '/'.join([METADATA_INDEX_PREFIX] + [index_signature_string])
        index_id = self._get(None, index_key)
        if index_id != UNDEFINED:
            # Index already exists
            raise GrapheekIndexAlreadyExistsException
        txn = self._transaction_begin()
        try:
            index_id = self._new_id_for_key(txn, METADATA_INDEX_COUNTER)
            self._set(txn, index_key, index_id)
            self._append_to_lst(txn, METADATA_INDEX_LIST, index_id)  # "Registering" index in index list
            self._set(txn, build_key(METADATA_INDEX_FIELDS_PREFIX, index_id), index_signature)
            entity_count = self._get(txn, METADATA_COUNTER)
            if entity_count == UNDEFINED:
                raise GrapheekDataException
            prefix = build_key(KIND_INDEX, _kind, index_id)
            # NOTE : So far, only ExactIndex is supported :
            index = ExactIndex(self, _kind, prefix, *fields, **filters)
            # Entity iterator : index will iterate on it to create denormalized data
            # that will (should ;)) speedup query
            id_iterator = self._optimizer.get_kind_ids(txn, _kind)
            # Building index :
            index.bulk_add(txn, id_iterator)
            # Everything seems to be ok, adding index to proper list
            entity_indexes.append(index)
            self._transaction_commit(txn)
        except Exception as e:
            self._transaction_rollback(txn)
            raise GrapheekIndexCreationFailedException(repr(e))

    def add_node_index(self, *fields, **filters):
        self._add_entity_index(KIND_VERTEX, *fields, **filters)

    def add_edge_index(self, *fields, **filters):
        self._add_entity_index(KIND_EDGE, *fields, **filters)

    def get_node_indexes(self):
        return [[index._fields, dict(index._filter_items)] for index in self._node_indexes]

    def get_edge_indexes(self):
        return [[index._fields, dict(index._filter_items)] for index in self._edge_indexes]

    def _remove_entity_index(self, _kind, *args, **kwargs):
        assert(_kind in (KIND_VERTEX, KIND_EDGE))
        fields = list(args)
        fields.sort()
        filter_tuples = list(kwargs.items())
        filter_tuples.sort()
        index_signature = [fields, filter_tuples]
        index_signature_string = normalize_value(index_signature)
        if _kind == KIND_VERTEX:
            indexes = self._node_indexes
            METADATA_INDEX_PREFIX = METADATA_VERTEX_INDEX_PREFIX
            METADATA_INDEX_LIST = METADATA_VERTEX_INDEX_LIST
            METADATA_INDEX_FIELDS_PREFIX = METADATA_VERTEX_INDEX_FIELDS_PREFIX
            METADATA_INDEX_COUNTER = METADATA_VERTEX_INDEX_COUNTER
        else:
            indexes = self._edge_indexes
            METADATA_INDEX_PREFIX = METADATA_EDGE_INDEX_PREFIX
            METADATA_INDEX_LIST = METADATA_EDGE_INDEX_LIST
            METADATA_INDEX_FIELDS_PREFIX = METADATA_EDGE_INDEX_FIELDS_PREFIX
            METADATA_INDEX_COUNTER = METADATA_EDGE_INDEX_COUNTER
        index_to_remove = None
        for index in indexes:
            if index._fields == fields and index._filter_items == set(kwargs.items()):
                index_to_remove = index
                break
        if index_to_remove is None:
            raise GrapheekIndexRemovalFailedException
        txn = self._transaction_begin()
        try:
            # Removing index
            index.delete(txn)  # it is the responsability of the index to remove everything it created
            # Removing index from index list :
            indexes.remove(index)
            # update counter
            self._update_dec(txn, METADATA_INDEX_COUNTER)
            # Removing "management key"
            index_key = '/'.join([METADATA_INDEX_PREFIX] + [index_signature_string])
            index_id = self._get(txn, index_key)
            # Remove index key :
            self._remove(txn, index_key)
            # Removing index fields :
            self._remove(txn, build_key(METADATA_INDEX_FIELDS_PREFIX, index_id))
            # Removing index from index list :
            self._remove_from_lst(txn, METADATA_INDEX_LIST, index_id)
            self._transaction_commit(txn)
        except Exception as e:
            self._transaction_rollback(txn)
            raise GrapheekIndexRemovalFailedException(repr(e))

    def remove_node_index(self, *fields, **filters):
        self._remove_entity_index(KIND_VERTEX, *fields, **filters)

    def remove_edge_index(self, *fields, **filters):
        self._remove_entity_index(KIND_EDGE, *fields, **filters)

    def _operator_kind_and_checks(self, *entity_iterators):
        # ensure entity iterators are of the same kind :
        kinds = set([it._src_kind for it in entity_iterators])
        if len(set(kinds)) > 1:
            raise GrapheekMixedKindException
        kind = kinds.pop()
        return kind

    def union(self, *entity_iterators):
        kind = self._operator_kind_and_checks(*entity_iterators)

        def id_iterator(*entity_iterators):
            known_ids = set()
            clone_entity_iterators = [entity_iterator._clone_initial() for entity_iterator in entity_iterators]
            for entity in chain(*clone_entity_iterators):
                entity_id = entity._entity_id
                if entity_id not in known_ids:
                    known_ids.add(entity_id)
                    yield entity_id
        return EntityIterator(self, kind, {}, id_iterator(*entity_iterators))

    def _marshalled_operator_helper(self, commands):  # pragma : no cover
        entity_iterators = []
        for command in commands:
            obj = self
            for method_name, args, kwargs in command:
                method = getattr(obj, method_name)
                obj = method(*args, **kwargs)
            entity_iterators.append(obj)
        return entity_iterators

    def _marshalled_union(self, *commands, **kwargs):  # pragma : no cover
        entity_iterators = self._marshalled_operator_helper(commands)
        return self.union(*entity_iterators)

    def _set_operation(self, func, *entity_iterators):
        kind = self._operator_kind_and_checks(*entity_iterators)
        id_sets = [set(it._clone_initial().ids()) for it in entity_iterators]
        ids = reduce(func, id_sets)
        return EntityIterator(self, kind, {}, iter(ids))

    def issubset(self, *entity_iterators):
        self._operator_kind_and_checks(*entity_iterators)
        id_sets = [set(it._clone_initial().ids()) for it in entity_iterators]
        func = lambda x, y: x.issubset(y)
        value = reduce(func, id_sets)
        return value

    def _marshalled_issubset(self, *commands, **kwargs):  # pragma : no cover
        entity_iterators = self._marshalled_operator_helper(commands)
        return self.issubset(*entity_iterators)

    def issuperset(self, *entity_iterators):
        self._operator_kind_and_checks(*entity_iterators)
        id_sets = [set(it._clone_initial().ids()) for it in entity_iterators]
        func = lambda x, y: x.issuperset(y)
        value = reduce(func, id_sets)
        return value

    def _marshalled_issuperset(self, *commands, **kwargs):  # pragma : no cover
        entity_iterators = self._marshalled_operator_helper(commands)
        return self.issuperset(*entity_iterators)

    def intersection(self, *entity_iterators):
        func = lambda x, y: x.intersection(y)
        return self._set_operation(func, *entity_iterators)

    def _marshalled_intersection(self, *commands, **kwargs):  # pragma : no cover
        entity_iterators = self._marshalled_operator_helper(commands)
        return self.intersection(*entity_iterators)

    def difference(self, *entity_iterators):
        func = lambda x, y: x.difference(y)
        return self._set_operation(func, *entity_iterators)

    def _marshalled_difference(self, *commands, **kwargs):  # pragma : no cover
        entity_iterators = self._marshalled_operator_helper(commands)
        return self.difference(*entity_iterators)

    def symmetric_difference(self, *entity_iterators):
        func = lambda x, y: x.symmetric_difference(y)
        return self._set_operation(func, *entity_iterators)

    def _marshalled_symmetric_difference(self, *commands, **kwargs):  # pragma : no cover
        entity_iterators = self._marshalled_operator_helper(commands)
        return self.symmetric_difference(*entity_iterators)

    def setup_server_scripts(self, *module_paths):
        import sys
        # Following lines may raise an exception if data module cannot be imported
        # I'm letting exception propagate so that the user can fix path (or other potential errors)
        self._script_module_cache = []
        for module_path in list(module_paths) + ['grapheekdb.server.scripts']:
            if module_path:
                __import__(module_path)
                class_module = sys.modules[module_path]
                self._script_module_cache.append(class_module)
        self._script_cache = {}
