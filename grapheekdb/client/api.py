# -*- coding:utf-8 -*-

import zmq
import msgpack

from grapheekdb.backends.data.keys import KIND_VERTEX, KIND_EDGE

from grapheekdb.lib.validations import check_valid_data

from grapheekdb.lib.readwrite import GraphReadWrite
from grapheekdb.lib.nx import GraphNx

from grapheekdb.lib.exceptions import GrapheekException
from grapheekdb.lib.exceptions import GrapheekDataException
from grapheekdb.lib.exceptions import GrapheekUnmarshallingException
from grapheekdb.lib.exceptions import GrapheekInvalidDataTypeException

try:  # pragma : no cover
    LONG = long
except NameError:  # pragma : no cover
    LONG = int
NONETYPE = type(None)
try:  # pragma : no cover
    UNICODE = unicode
except NameError:  # pragma : no cover
    UNICODE = str


def unmarshall(graph, item):
    if isinstance(item, (bool, float, int, str, LONG, NONETYPE, UNICODE)):
        return item
    if isinstance(item, bytes):  # pragma : no cover
        return str(item, encoding='utf8')
    if isinstance(item, (ProxyNode, ProxyEdge)):
        return item
    elif isinstance(item, dict):
        # Check if item is in fact a "special" dict (<-> handling an instance serialization)
        kind = item.get('__', None)
        if kind is not None:
            # If msg is an exception, raise it rapidly (do no count an caller to raise sth)
            # (But this doesn't forbid caller to catch exception, btw)
            if kind == 'x':
                raise GrapheekException(
                    '\n____________________________________\n*** SERVER *** exception traceback :\n%s'
                    % (item['d'],)
                )
            elif kind == 'n':
                return ProxyNode(graph, item['_i'], **unmarshall(graph, item['d']))
            elif kind == 'e':
                return ProxyEdge(graph, item['_i'], **unmarshall(graph, item['d']))
            elif kind == 'd':
                return dict((unmarshall(graph, key), unmarshall(graph, value)) for key, value in item['d'])
        else:
            items = [(unmarshall(graph, key), unmarshall(graph, value)) for key, value in list(item.items())]
            return dict(items)
    elif isinstance(item, (list, tuple)):
        return [unmarshall(graph, x) for x in item]
    raise GrapheekUnmarshallingException('Unknown type %s' % type(item))


def sanitize(item):
    if isinstance(item, (bool, float, int, str, LONG, NONETYPE, UNICODE)):
        return item
    if isinstance(item, bytes):  # pragma : no cover
        return str(item, encoding='utf8')
    if isinstance(item, dict):
        items = [(sanitize(key), sanitize(value)) for key, value in list(item.items())]
        return dict(items)
    elif isinstance(item, (list, tuple, set)):
        return [sanitize(x) for x in item]


def check_proxy_node(entity):
    if not(isinstance(entity, ProxyNode)):
        raise GrapheekInvalidDataTypeException('%s is not a node')


class ProxyDotExport(object):

    def __init__(self, dotstring):
        self._dotstring = dotstring

    def __repr__(self):
        return self._dotstring

    def to_dot(self):
        return self._dotstring


class ProxyIteratorCommon(object):

    def _request(self):
        return self._graph._request(self._commands)

    def __iter__(self):
        return self

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

    def next(self):
        # Python 2 compatibility
        return self.__next__()

    def __next__(self):
        if self._current_iteration is None:
            response = self._request()
            self._current_iteration = iter(response)
        try:
            return next(self._current_iteration)
        except StopIteration:
            self._current_iteration = None
        raise StopIteration

    def _do(self, _method, *args, **kwargs):
        command = [_method, args, kwargs]
        return self.__class__(self._graph, self._commands + [command])

    def _raw(self, _method, *args, **kwargs):
        command = [_method, args, kwargs]
        response = self._graph._request(self._commands + [command])
        return unmarshall(self._graph, response)


class ProxyEntityAggregate(ProxyIteratorCommon):

    def __init__(self, graph, commands):
        self._graph = graph
        self._commands = commands
        self._current_iteration = None

    def entities(self):
        command = ['entities', [], {}]
        return ProxyEntityIterator(self._graph, self._commands + [command])

    def limit(self, number):
        return self._do('limit', number)


class ProxyEntityIterator(ProxyIteratorCommon):

    def __init__(self, graph, commands):
        self._graph = graph
        self._commands = commands
        self._current_iteration = None

    def _jump(self, _traversal, *args, **kwargs):
        return self._do(_traversal, *args, **kwargs)

    def inV(self, *args, **kwargs):
        return self._jump('inV', *args, **kwargs)

    def outV(self, *args, **kwargs):
        return self._jump('outV', *args, **kwargs)

    def bothV(self, *args, **kwargs):
        return self._jump('bothV', *args, **kwargs)

    def in_(self, *args, **kwargs):
        return self._jump('in_', *args, **kwargs)

    def out_(self, *args, **kwargs):
        return self._jump('out_', *args, **kwargs)

    def both_(self, *args, **kwargs):
        return self._jump('both_', *args, **kwargs)

    def inE(self, **kwargs):
        # TODO : Check that the "ring" on which we are is composed of nodes (NOT edges)
        return self._jump('inE', **kwargs)

    def outE(self, **kwargs):
        # TODO : Check that the "ring" on which we are is composed of nodes (NOT edges)
        return self._jump('outE', **kwargs)

    def bothE(self, **kwargs):
        # TODO : Check that the "ring" on which we are is composed of nodes (NOT edges)
        return self._jump('bothE', **kwargs)

    def random(self, *args, **kwargs):
        return self._jump('random', *args, **kwargs)

    def dedup(self, *aliases):
        return self._do('dedup', *aliases)

    def without(self, *aliases):
        return self._do('without', *aliases)

    def data(self, *args, **kwargs):
        return self._raw('data', *args, **kwargs)

    def limit(self, count):
        return self._do('limit', count)

    def aka(self, alias):
        return self._do('aka', alias)

    def count(self):
        return int(self._raw('count'))

    def remove(self):
        self._raw('remove')

    def update(self, **updates):
        self._raw('update', **updates)

    def all(self):
        return self._raw('all')

    def ids(self):
        return self._raw('ids')

    def collect(self, *aliases):
        return self._raw('collect', *aliases)

    def sum(self, expr=None, asc=False):
        command = ('sum', [expr], dict(asc=asc))
        return ProxyEntityAggregate(self._graph, self._commands + [command])

    def percent(self, expr=None, asc=False):
        command = ('percent', [expr], dict(asc=asc))
        return ProxyEntityAggregate(self._graph, self._commands + [command])

    def order_by(self, *clauses):
        return self._do('order_by', *clauses)

    def call(self, _fname, *aliases, **params):
        return self._raw('call', _fname, *aliases, **params)

    def request(self, _fname, *aliases, **params):
        return self._raw('request', _fname, *aliases, **params)

    def dot(self, node_label=None, edge_label=None, limit=100):
        dotstring = self._raw('_dot_str', node_label, edge_label, limit)
        return ProxyDotExport(dotstring)


class ProxyEntity(object):

    def __init__(self, _graph, _entity_id, **data):
        check_valid_data(data)
        self._entity_id = _entity_id
        self._graph = _graph
        self._commands = [[self._kind, [_entity_id], {}]]
        self._data = data

    def __repr__(self):
        kind = 'node' if self._kind == KIND_VERTEX else 'edge'
        return '<%s id:%s data:%s>' % (kind, self._entity_id, repr(self._data))

    def __eq__(self, other):
        return other.__class__ == self.__class__ and other.get_id() == self.get_id() and other._graph == self._graph

    def __ne__(self, other):
        # Cf Python Doc : http://docs.python.org/2/reference/datamodel.html#object.__ne__
        # "When defining __eq__(), one should also define __ne__() so that the operators will behave as expected"
        return not self.__eq__(other)

    def __getattr__(self, attr):
        # Get value form local data
        if attr in self._data:
            return self._data[attr]
        # Try to update data :
        data = self._data = self.data()
        try:
            return data[attr]
        except KeyError:
            raise AttributeError("%r instance has no attribute %r" % (self.__class__, attr))

    def __setattr__(self, attr, value):
        if attr.startswith('_'):
            self.__dict__.update({attr: value})
        else:
            command = ['update_data', [self._kind, self._entity_id, attr, value], {}]
            self._graph._request([command])

    def _do(self, _method, *args, **kwargs):
        command = [_method, args, kwargs]
        return ProxyEntityIterator(self._graph, self._commands + [command])

    def _raw(self, _method, *args, **kwargs):
        command = [_method, args, kwargs]
        response = self._graph._request(self._commands + [command])
        return unmarshall(self._graph, response)

    def get(self, field, default=None):
        try:
            return self.__getattr__(field)
        except AttributeError:
            return default

    def inV(self, *args, **kwargs):
        return self._do('inV', *args, **kwargs)

    def outV(self, *args, **kwargs):
        return self._do('outV', *args, **kwargs)

    def bothV(self, *args, **kwargs):
        return self._do('bothV', *args, **kwargs)

    def in_(self, *args, **kwargs):
        return self._do('in_', *args, **kwargs)

    def out_(self, *args, **kwargs):
        return self._do('out_', *args, **kwargs)

    def both_(self, *args, **kwargs):
        return self._do('both_', *args, **kwargs)

    def random(self, *args, **kwargs):
        return self._do('random', *args, **kwargs)

    def data(self, *args, **kwargs):
        return self._raw('data', *args, **kwargs)

    def get_id(self):
        return int(self._entity_id)

    def remove(self):
        return self._raw('remove')

    def update(self, **updates):
        self._raw('update', **updates)
        # No exception raised -> Apply updates on self.__dict__
        self.__dict__.update(updates)

    def aka(self, alias):
        return self._do('aka', alias)


class ProxyNode(ProxyEntity):

    def __init__(self, _graph, _node_id, **data):
        self._kind = KIND_VERTEX
        super(ProxyNode, self).__init__(_graph, _node_id, **data)

    def inE(self, **kwargs):
        return self._do('inE', **kwargs)

    def outE(self, **kwargs):
        return self._do('outE', **kwargs)

    def bothE(self, **kwargs):
        return self._do('bothE', **kwargs)


class ProxyEdge(ProxyEntity):

    def __init__(self, _graph, _edge_id, **data):
        self._kind = KIND_EDGE
        super(ProxyEdge, self).__init__(_graph, _edge_id, **data)


class ProxyGraph(GraphReadWrite, GraphNx):

    def __init__(self, address):  # pragma : no cover
        self._address = address
        #self._zmq_context = zmq.Context()
        #self._zmq_socket = self._zmq_context.socket(zmq.REQ)
        #self._zmq_socket.connect(address)

    def _request(self, commands):
        data = msgpack.dumps(sanitize(commands), encoding='utf8')
        zmq_context = zmq.Context()
        zmq_socket = zmq_context.socket(zmq.REQ)
        zmq_socket.connect(self._address)
        zmq_socket.send(data)
        raw = zmq_socket.recv()
        msg = msgpack.loads(raw, encoding='utf8')
        return unmarshall(self, msg)

    def v(self, entity_id):
        return ProxyNode(self, entity_id)

    def V(self, **kwargs):
        command = [KIND_VERTEX.upper(), [], kwargs]
        return ProxyEntityIterator(self, [command])

    def e(self, entity_id):
        return ProxyEdge(self, entity_id)

    def E(self, **kwargs):
        command = [KIND_EDGE.upper(), [], kwargs]
        return ProxyEntityIterator(self, [command])

    def add_node(self, **kwargs):
        check_valid_data(kwargs)
        command = ['add_node', [], kwargs]
        res = self._request([command])
        return res

    def bulk_add_node(self, node_defns, silent=False):
        for data in node_defns:
            check_valid_data(data)
        command = ['bulk_add_node', [node_defns], {'silent': silent}]
        return self._request([command])

    def add_edge(self, _source, _target, **kwargs):
        check_valid_data(kwargs)
        check_proxy_node(_source)
        check_proxy_node(_target)
        source_id = _source.get_id()
        target_id = _target.get_id()
        command = ['add_edge_by_ids', [source_id, target_id], kwargs]
        return self._request([command])

    def bulk_add_edge(self, edge_defns, silent=False):
        try:
            for source, target, data in edge_defns:
                check_proxy_node(source)
                check_proxy_node(target)
                check_valid_data(data)
        except (TypeError, ValueError) as e:
            raise GrapheekDataException(repr(e))
        # Don't be suprised by [[ ... ]] instead of [ ... ]
        # Server function is expecting only one argument :
        args = [[(source.get_id(), target.get_id(), data) for source, target, data in edge_defns]]
        command = ['bulk_add_edge_by_ids', args, {'silent': silent}]
        return self._request([command])

    def _add_entity_index(self, _kind, *fields, **filters):
        assert(_kind in (KIND_EDGE, KIND_VERTEX))
        command = ['add_node_index' if _kind == KIND_VERTEX else 'add_edge_index', fields, filters]
        return self._request([command])

    def add_node_index(self, *fields, **filters):
        return self._add_entity_index(KIND_VERTEX, *fields, **filters)

    def add_edge_index(self, *fields, **filters):
        return self._add_entity_index(KIND_EDGE, *fields, **filters)

    def get_node_indexes(self):
        command = ['get_node_indexes', [], {}]
        return self._request([command])

    def get_edge_indexes(self):
        command = ['get_edge_indexes', [], {}]
        return self._request([command])

    def _remove_entity_index(self, _kind, *fields, **filters):
        assert(_kind in (KIND_EDGE, KIND_VERTEX))
        command = ['remove_node_index' if _kind == KIND_VERTEX else 'remove_edge_index', fields, filters]
        return self._request([command])

    def remove_node_index(self, *fields, **filters):
        return self._remove_entity_index(KIND_VERTEX, *fields, **filters)

    def remove_edge_index(self, *fields, **filters):
        return self._remove_entity_index(KIND_EDGE, *fields, **filters)

    def _operation_helper(self, operation, *entity_iterators):
        command = [operation, [entity_iterator._commands for entity_iterator in entity_iterators], {}]
        return ProxyEntityIterator(self, [command])

    def issubset(self, *entity_iterators):
        command = ['_marshalled_issubset', [entity_iterator._commands for entity_iterator in entity_iterators], {}]
        return self._request([command])

    def issuperset(self, *entity_iterators):
        command = ['_marshalled_issuperset', [entity_iterator._commands for entity_iterator in entity_iterators], {}]
        return self._request([command])

    def union(self, *entity_iterators):
        return self._operation_helper('_marshalled_union', *entity_iterators)

    def intersection(self, *entity_iterators):
        return self._operation_helper('_marshalled_intersection', *entity_iterators)

    def difference(self, *entity_iterators):
        return self._operation_helper('_marshalled_difference', *entity_iterators)

    def symmetric_difference(self, *entity_iterators):
        return self._operation_helper('_marshalled_symmetric_difference', *entity_iterators)
