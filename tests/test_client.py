# -*- coding:utf-8 -*-

#import zmq.green as zmq
import zmq

import time
from multiprocessing import Process, Queue
from uuid import uuid4
from grapheekdb.client.api import ProxyGraph
from grapheekdb.server.serve import runserver
from grapheekdb.lib.exceptions import GrapheekException
from grapheekdb.lib.exceptions import GrapheekUnmarshallingException

from .data_backend_common import FillMethod, CommonMethods


class WaitingProxyGraph(ProxyGraph):

    def __init__(self, address):
        self._address = address
        self._zmq_context = zmq.Context()
        self._zmq_socket = self._zmq_context.socket(zmq.REQ)
        counter = 0
        while True:
            counter += 1
            try:
                self._zmq_socket.connect(address)
                break
            except:
                time.sleep(0.1)
            if counter > 50:
                break


def serve(_address, _backend, _scripts, **params):
    context = zmq.Context()
    runserver(_address, _backend, _scripts, context, **params)


def client(address, queue):
    from random import randint
    graph = WaitingProxyGraph(address)
    try:
        for i in range(100):
            remove = randint(1, 5) == 0
            if remove:
                graph.V(foo=randint(1, 10)).remove()
            else:
                graph.add_node(foo=randint(1, 10))
    except:
        queue.put(False)
    queue.put(True)


class TestClient(FillMethod, CommonMethods):
    """
    This test is very IMPORTANT :
    it checks that client API is isomorphic to direct backend API.

    This class (TestClient) inherits from CommonMethods which holds the "real" tests
    """

    def setup(self):
        self.address = "ipc:///tmp/%s" % (uuid4().int,)
        self.backend = "grapheekdb.backends.data.localmem.LocalMemoryGraph"
        self.params = {}
        self.scripts = ''
        self.server_process = Process(target=serve, args=(self.address, self.backend, self.scripts), kwargs=self.params)
        self.server_process.start()
        self.graph = WaitingProxyGraph(self.address)
        self.fill()

    def teardown(self):
        self.graph._zmq_socket.send_string('stop')
        self.graph._zmq_socket.close()
        self.server_process.terminate()

    def test_client_exception(self):
        exception_raised = False
        try:
            self.graph.add_node_index('foo')
            # adding the same index must raised an exception
            self.graph.add_node_index('foo')
        except GrapheekException:
            exception_raised = True
        assert(exception_raised)

    def test_unmarshalling_bad_data(self):
        from grapheekdb.client.api import unmarshall

        class Foo:
            pass

        foo = Foo()
        exception_raised = False
        try:
            unmarshall(self.graph, foo)
        except GrapheekUnmarshallingException:
            exception_raised = True
        assert(exception_raised)

    def test_double_close(self):
        pass  # disable inherited method

    def test_partial_indexes_4(self):
        pass  # disable inherited method

    def test_iterator_agg_with_reference_to_context_forbidden_expression_1(self):
        pass  # disable inherited method

    def test_iterator_agg_with_reference_to_context_forbidden_expression_2(self):
        pass  # disable inherited method

    def test_iterator_agg_with_reference_to_context_forbidden_expression_3(self):
        pass  # disable inherited method

    def test_entity_random_method_unknown_method(self):
        pass  # disable inherited method

    def test_edge_random_method_unknown_method(self):
        pass  # disable inherited method

    def test_iterator_random_method_unknown_method(self):
        pass  # disable inherited method

    def test_iterator_random_method_unknown_method_2(self):
        pass  # disable inherited method

    def test_iterator_random_method_unknown_method_3(self):
        pass  # disable inherited method


class TestConcurrentAccess(object):
    """
    This is not a deterministic test <-> anti-pattern
    But it definitily helps to find potential concurrency or load issues
    """

    def setup(self):
        self.address = "ipc:///tmp/%s" % (uuid4().int,)
        self.backend = "grapheekdb.backends.data.localmem.LocalMemoryGraph"
        self.params = {}
        self.scripts = ''
        self.graph = WaitingProxyGraph(self.address)
        self.server_process = Process(target=serve, args=(self.address, self.backend, self.scripts), kwargs=self.params)
        self.server_process.start()

    def teardown(self):
        self.graph._zmq_socket.send_string('stop')
        self.graph._zmq_socket.close()
        self.server_process.terminate()

    def _concurrency(self, count):
        queues = [Queue() for _ in range(count)]
        processes = [Process(target=client, args=(self.address, queue)) for queue in queues]
        for process in processes:
            process.start()
        results = [queue.get() for queue in queues]
        for process in processes:
            process.terminate()
        return results

    def test_concurrency_10_noindex(self):
        results = self._concurrency(10)
        assert(all(results))

    def test_concurrency_10_index(self):
        graph = WaitingProxyGraph(self.address)
        graph.add_node_index('foo')
        results = self._concurrency(10)
        assert(all(results))


class TestServerSideScriptCallByClient(object):

    def setup(self):
        self.address = "ipc:///tmp/%s" % (uuid4().int,)
        self.backend = "grapheekdb.backends.data.localmem.LocalMemoryGraph"
        self.params = {}
        self.scripts = 'grapheekdb.server.dummy_scripts'
        self.graph = WaitingProxyGraph(self.address)
        self.server_process = Process(target=serve, args=(self.address, self.backend, self.scripts), kwargs=self.params)
        self.server_process.start()
        self.n1 = self.graph.add_node(nid=1)
        self.n2 = self.graph.add_node(nid=2)
        self.n3 = self.graph.add_node(nid=3)
        self.graph.add_edge(self.n1, self.n2)
        self.graph.add_edge(self.n2, self.n3)

    def teardown(self):
        self.graph._zmq_socket.send_string('stop')
        self.graph._zmq_socket.close()
        self.server_process.terminate()

    def test_server_script_add_edge(self):
        edge_count_before = self.graph.E().count()
        self.graph.V(nid=1).aka('x').outV().outV().aka('y').call('add_edge', 'x', 'y', foo=1)
        edge_count_after = self.graph.E().count()
        assert(edge_count_after == edge_count_before + 1)

    def test_server_script_add_edge_multiple(self):
        # see https://bitbucket.org/nidusfr/grapheekdb/issue/11
        self.graph.V(nid__in=[1,2]).aka('x').out_().aka('y').call('add_edge', 'x', 'y', foo=1)
        created = set(tuple(v.nid for v in e.bothV()) for e in self.graph.E(foo=1))
        assert created, {(1,2), (2, 3)}

    def test_server_check_call_is_silent(self):
        res = self.graph.V(nid=1).aka('x').outV().outV().aka('y').call('add_edge', 'x', 'y', foo=1)
        assert(res is None)

    def test_server_check_request_is_verbose(self):
        res = self.graph.V(nid=1).aka('x').outV().outV().aka('y').request('echo', msg="hello")
        assert res == [{'args': [], 'kwargs': {"msg": "hello"}}]

    def test_server_script_unknown_function(self):
        exception_raised = False
        try:
            self.graph.V(nid=1).aka('x').outV().outV().aka('y').call('this_function_doesnt_exist', 'x', 'y', foo=1)
        except GrapheekException:
            exception_raised = True
        assert(exception_raised)

    # Test dot generation - to_dot method

    def test_dot_to_dot_generation_basic(self):
        s = str(self.graph.V().dot().to_dot())
        assert('digraph' in s)

    def test_dot_to_dot_generation_basic_2(self):
        s = str(self.graph.V().dot())
        assert('digraph' in s)

    def test_dot_to_dot_generation_basic_3(self):
        s = repr(self.graph.E().dot().to_dot())
        assert('digraph' in s)

    def test_dot_to_dot_generation_with_filter(self):
        str(self.graph.V(foo=1).dot().to_dot())

    def test_dot_to_dot_generation_boolean_label_1(self):
        # Adding a node with a boolean property
        self.graph.add_node(visible=True)
        # Get the representation
        s = repr(self.graph.V().dot('visible').to_dot())
        assert('digraph' in s)
