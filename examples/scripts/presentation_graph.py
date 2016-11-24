#!/usr/bin/env
# -*- coding: utf-8 -*-

from grapheekdb.backends.data.localmem import LocalMemoryGraph
g = LocalMemoryGraph()

raf = g.add_node(name='Raphael')
grapheekdb = g.add_node(name='Grapheekdb')
python = g.add_node(name='Python')
localmem = g.add_node(name='Local Memory')
kyoto = g.add_node(name='Kyoto Cabinet')
lmdb = g.add_node(name='Symas LMDB')
gpl = g.add_node(name='GPL V3')
fr = g.add_node(name='french')
persistent = g.add_node(name='persistent')
kvs = g.add_node(name='Key Value Store')

g.add_edge(raf, grapheekdb, action='created')
g.add_edge(raf, python, action='loves')
g.add_edge(raf, fr, action='is')

g.add_edge(grapheekdb, python, action='is implemented in')
g.add_edge(grapheekdb, gpl, action='is')
g.add_edge(grapheekdb, localmem, action='can use')
g.add_edge(grapheekdb, lmdb, action='can use')
g.add_edge(grapheekdb, kyoto, action='can use')

g.add_edge(localmem, kvs, action='is a')
g.add_edge(kyoto, kvs, action='is a')
g.add_edge(lmdb, kvs, action='is a')

g.add_edge(kyoto, persistent, action='is')
g.add_edge(lmdb, persistent, action='is')

# %dotobj g.V().dot('name', 'action')
