#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import traceback
import msgpack
import argparse
from collections import OrderedDict

try:  # pragma : no cover
    from ConfigParser import RawConfigParser  # Python 2
except:  # pragma : no cover
    from configparser import RawConfigParser  # Python 3

try:  # pragma : no cover
    LONG = long
except NameError:  # pragma : no cover
    LONG = int
NONETYPE = type(None)
try:  # pragma : no cover
    UNICODE = unicode
except NameError:  # pragma : no cover
    UNICODE = str

import zmq

from grapheekdb.backends.data.base import EntityAggregate, EntityIterator, Node, Edge
from grapheekdb import __version__
from grapheekdb.lib.exceptions import GrapheekMarshallingException


def marshall(item):
    if isinstance(item, (bool, float, int, str, LONG, NONETYPE, UNICODE)):
        return item
    if isinstance(item, bytes):  # pragma : no cover
        # this should only occur in Python3
        return str(item, encoding='utf8')
    elif isinstance(item, (list, tuple)):
        return [marshall(x) for x in item]
    elif isinstance(item, (dict, OrderedDict)):
        items = [(marshall(key), marshall(value)) for key, value in list(item.items())]
        # Not returning : dict(items)
        # Because marshall(key) could be an unhashable type (such as dict)
        # contrary to key...
        return {
            '__': 'd',
            'd': items
        }
    elif isinstance(item, Node):
        return {
            '__': 'n',
            '_i': item.get_id(),
            'd': marshall(item.data())
        }
    elif isinstance(item, Edge):
        return {
            '__': 'e',
            '_i': item.get_id(),
            'd': marshall(item.data())
        }
    elif isinstance(item, (EntityAggregate, EntityIterator)):
        return [marshall(x) for x in item]
    raise GrapheekMarshallingException('Unknown type or instance : %s - %s' % (type(item), item))

def fixbytes(item):
    if isinstance(item, (bool, float, int, str, LONG, NONETYPE, UNICODE)):
        return item
    if isinstance(item, bytes):  # pragma : no cover
        # this should only occur in Python3
        return str(item, encoding='utf8')
    elif isinstance(item, (list, tuple)):
        return [fixbytes(x) for x in item]
    elif isinstance(item, dict):
        return dict([(fixbytes(key), fixbytes(value)) for key, value in list(item.items())])
    raise GrapheekMarshallingException('Unknown type or instance : %s - %s' % (type(item), item))  # pragma : no cover


def runserver(_address, _backend, _scripts, _context, **params):
    class_module_name, class_name = _backend.rsplit('.', 1)
    # START -- Following lines may raise an exception if data module cannot be imported
    # I'm letting exception propagate so that the user can fix path (or other potential errors)
    __import__(class_module_name)
    class_module = sys.modules[class_module_name]
    # -- END
    GraphClass = getattr(class_module, class_name)
    g = GraphClass(**params)
    try:
        # Setting script module paths :
        g.setup_server_scripts(*_scripts.split(':'))
        # --
        if _context is None:  # pragma : no cover
            _context = zmq.Context()
        socket = _context.socket(zmq.REP)
        socket.bind(_address)
        # -----------------------------------------
        stop = False
        ipc = _address.startswith('ipc://')
        while not(stop):  # pragma : no cover
            try:
                # Wait for next request from client
                raw = socket.recv()
                # if address is ipc, it means that the "server" process had been start from another process
                # -> the parent needs a way to stop imperatively the "server" process, thus the following line :
                if ipc:
                    if raw in ['stop', b'stop']:
                        socket.send_string('ok')
                        break
                # Handling request :
                data = fixbytes(msgpack.loads(raw, encoding='utf8'))
                obj = g
                for item in data:
                    method_name, args, kwargs = item
                    method = getattr(obj, method_name)
                    obj = method(*args, **kwargs)
                result = marshall(obj)
                socket.send(msgpack.dumps(result, encoding='utf8'))
            except KeyboardInterrupt:
                stop = True
            except:
                result = {
                    '__': 'x',
                    'd': traceback.format_exc()
                }
                try:
                    socket.send(msgpack.dumps(result, encoding='utf8'))
                except:
                    pass  # The server must not fail
                    # TODO : At least log an info
    finally:
        g.close()


def read_config_file(path):
    # defaults
    address = backend = script_import = None
    params = {}
    # parsing config file
    config = RawConfigParser()
    config.read(path)
    if config.has_section('server'):
        items = dict(config.items('server'))
        address = items.get('address', None)
        backend = items.get('backend', None)
    if config.has_section('scripts'):
        items = dict(config.items('scripts'))
        script_import = items.get('import', None)
    if config.has_section('backend'):
        params = dict(config.items('backend'))
    return address, backend, params, script_import


def parse_params(params):
    result = {}
    for param in params:
        key, value = param.split(':')
        result[key] = value
    return result


def main(cmds=None, verbose=True, context=None):

    # default config :
    default_address = "tcp://127.0.0.1:5555"
    default_backend = "grapheekdb.backends.data.localmem.LocalMemoryGraph"
    default_scripts = ''
    default_params  = {}

    # parsing command line :
    parser = argparse.ArgumentParser(description='Run GrapheekDB server.', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '-a',
        '--address',
        dest='address',
        required=False,
        help="""The address string. This has the form 'protocol://interface:port',
        for example 'tcp://127.0.0.1:5555'. Protocols supported include
        tcp, inproc and ipc.
        """
    )
    parser.add_argument(
        "-b",
        "--backend",
        dest="backend",
        required=False,
        help='Data backend. must be of the form (for instance) : "grapheekdb.backends.data.kyotocab.KyotoCabinetGraph"',
    )
    parser.add_argument(
        "-c",
        "--config",
        dest="config",
        required=False,
        help='Server configuration file',
    )
    parser.add_argument(
        "-s",
        "--scripts",
        dest="scripts",
        required=False,
        default=default_scripts,
        help='Server side scripts module path. Must be of the form : path.to.my_module1.scripts:path.to.my_module2.scripts:[.. additional module path ..]',
    )
    parser.add_argument(
        "params",
        nargs="*",
        default="",
        help='Data backend parameters. must be of the form : "key1:value1 key2:value ..."'
    )
    if cmds is not None:
        args = parser.parse_args(cmds)
    else:  # pragma : no cover
        args = parser.parse_args()
    # Building runserver arguments & keyword arguments
    # There's 3 steps to build them :
    # 1st step : use default values (done in first lines of main )
    # 2nd step : Read value from config file path passed as argument (if there's one). Override existing previous values
    # 3rd step :  Read value from command line options. Override existing previous values
    #
    # let's do the 2nd step : configuration file path
    config_address = config_backend = config_scripts = None
    config_params = {}
    line_config = args.config
    if line_config:  # pragma : no cover
        config_address, config_backend, config_params, config_scripts = read_config_file(line_config)
    # 3nd step : command line options :
    line_address = args.address
    line_backend = args.backend
    line_scripts = parse_params(args.scripts)
    line_params = parse_params(args.params)
    # Synthesis :
    address = line_address or config_address or default_address
    backend = line_backend or config_backend or default_backend
    scripts = line_scripts or config_scripts or default_scripts
    default_params.update(config_params)
    default_params.update(line_params)
    params = default_params
    config = {
        'version': __version__,
        'address': address,
        'backend': backend,
        'params': params,
        'scripts': scripts
    }
    label = "GrapheekDB Server version %(version)s" % config
    if verbose:  # pragma : no cover
        print(label)
        print('=' * len(label))
        print("Address              : %(address)s  <--  use the same address in client" % config)
        print("Backend              : %(backend)s" % config)
        print("Params               : %(params)s" % config)
        print("Server scripts paths : %(scripts)s" % config)
        print("Quit the server with CONTROL-C.")
    runserver(address, backend, scripts, context, **params)


if __name__ == '__main__':  # pragma : no cover
    main()
