# -*- coding:utf-8 -*-

import tempfile
import msgpack
from uuid import uuid4
import time
from multiprocessing import Process, Queue

import zmq

from grapheekdb.lib.exceptions import GrapheekMarshallingException
from grapheekdb.lib.exceptions import GrapheekInvalidDataTypeException
from grapheekdb.server.serve import marshall
from grapheekdb.client.api import unmarshall


class TestMarshalling:

    def test_marshall_string(self):
        assert(marshall('foo') == 'foo')

    def test_marshall_list(self):
        assert(marshall(list(range(10))) == list(range(10)))

    def test_marshall_dict(self):
        from grapheekdb.backends.data.localmem import LocalMemoryGraph
        graph = LocalMemoryGraph()
        assert(unmarshall(graph, marshall(dict(foo=1, bar=2))) == dict(foo=1, bar=2))

    def test_marshall_node(self):
        from grapheekdb.backends.data.localmem import LocalMemoryGraph
        from grapheekdb.client.api import ProxyNode
        graph = LocalMemoryGraph()
        node = graph.add_node(foo=1)
        entity = unmarshall(graph, marshall(node))
        assert(isinstance(entity, ProxyNode))
        assert(entity.foo == 1)

    def test_marshall_edge(self):
        from grapheekdb.backends.data.localmem import LocalMemoryGraph
        from grapheekdb.client.api import ProxyEdge
        graph = LocalMemoryGraph()
        node1 = graph.add_node(foo=1)
        node2 = graph.add_node(foo=2)
        edge = graph.add_edge(node1, node2, foo=3)
        entity = unmarshall(graph, marshall(edge))
        assert(isinstance(entity, ProxyEdge))
        assert(entity.foo == 3)

    def test_marshall_entity_iterator(self):
        from grapheekdb.backends.data.localmem import LocalMemoryGraph
        graph = LocalMemoryGraph()
        assert(isinstance(marshall(graph.V()), list))

    def test_marshalling_bad_data(self):

        class Foo:
            pass

        foo = Foo()
        exception_raised = False
        try:
            marshall(foo)
        except GrapheekMarshallingException:
            exception_raised = True
        assert(exception_raised)


class TestServerConfigParsing:

    def setup(self):
        self.path = tempfile.mktemp()

    def test_config_parsing(self):
        from grapheekdb.server.serve import read_config_file
        content = """[server]
address=foo1
backend=foo2
[backend]
param1=foo3
param2=foo4
[scripts]
import=path1:path2:path3
"""
        f = open(self.path, 'w')
        f.write(content)
        f.close()
        address, backend, params, script_import = read_config_file(self.path)
        assert(address == 'foo1')
        assert(backend == 'foo2')
        assert(params == {'param1': 'foo3', 'param2': 'foo4'})
        assert(script_import == 'path1:path2:path3')


class TestCheckValidValue:

    def test_check_valid_value_raise_an_exception_for_invalid_embedded_dictionnary(self):
        from grapheekdb.lib.validations import check_valid_value

        class A:
            pass

        value = {
            'foo': {
                'bar': 1,
                'bar': A()
            }
        }

        exception_raised = False
        try:
            check_valid_value(value)
        except GrapheekInvalidDataTypeException:
            exception_raised = True
        assert(exception_raised)


class TestServerParseParams:

    def test_parse(self):
        from grapheekdb.server.serve import parse_params
        assert(parse_params(['key1:value1', 'key2:value2', 'key3:value3']) == {'key1': 'value1', 'key2': 'value2', 'key3': 'value3'})


class TestServer:

    def test_running_server(self):
        pass
        from grapheekdb.server.serve import main as main_serve

        address = "ipc:///tmp/%s" % (uuid4().int,)
        # hack test : running a process to stop running server after tests are done

        def fetch_data_and_stop_server(address, queue):
            context = zmq.Context()
            res = {
                'v_count': None,
                'e_count': None,
            }
            counter = 0
            while True:
                counter += 1
                try:
                    socket = context.socket(zmq.REQ)
                    socket.connect(address)
                    break
                except:
                    time.sleep(0.1)
                if counter > 50:
                    return  # res won't be updated -> test will fail
            # Fetching vertices count
            command = msgpack.dumps([
                ['V', [], {}],
                ['count', [], {}]
            ], encoding='utf8')
            socket.send(command)
            res['v_count'] = msgpack.loads(socket.recv(), encoding='utf8')
            # Fetching edges count
            command = msgpack.dumps([
                ['E', [], {}],
                ['count', [], {}]
            ], encoding='utf8')
            socket.send(command)
            res['e_count'] = msgpack.loads(socket.recv(), encoding='utf8')
            # Stopping server imperatively :
            socket.send_string('stop')
            res['ack'] = socket.recv()
            queue.put(res)
        # Starting process BEFORE starting server (else test would block)
        queue = Queue()
        process = Process(target=fetch_data_and_stop_server, args=(address, queue))
        process.start()
        # Now starting server (will block but external process will unblock it)
        context = zmq.Context()
        main_serve(cmds=('-a %s -b grapheekdb.backends.data.localmem.LocalMemoryGraph' % (address,)).split(), verbose=False, context=context)
        results = queue.get()
        process.terminate()
        # Checking results :
        assert(results['v_count'] == 0)
        assert(results['e_count'] == 0)
