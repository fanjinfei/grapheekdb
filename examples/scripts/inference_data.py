#!/usr/bin/env python
# -*- coding: utf-8 -*-

from grapheekdb.backends.data.localmem import LocalMemoryGraph
g = LocalMemoryGraph()

#######################################
#  Adding persons of the same family  #
#######################################

martine = g.add_node(name='martine', gender='f')
gerard = g.add_node(name='gerard', gender='m')
laou = g.add_node(name='laou', gender='f')
daniel = g.add_node(name='daniel', gender='m')

flo = g.add_node(name='flo', gender='f')
raf = g.add_node(name='raf', gender='m')
theo = g.add_node(name='theo', gender='m')
nathan = g.add_node(name='nathan', gender='m')
noe = g.add_node(name='noe', gender='m')

nathalie_g = g.add_node(name='nathalie_g', gender='f')
pierre_yves = g.add_node(name='pierre_yves', gender='m')
gabriel = g.add_node(name='gabriel', gender='m')

nathalie_c = g.add_node(name='nathalie_c', gender='f')
nicolas = g.add_node(name='nicolas', gender='m')
hanae = g.add_node(name='hanae', gender='f')
come = g.add_node(name='come', gender='m')

clarisse = g.add_node(name='clarisse', gender='f')
thomas = g.add_node(name='thomas', gender='m')
noam = g.add_node(name='noam', gender='m')

# Creating 'is_parent' relationship

# grandparents -> parents

g.add_edge(martine, raf, rel='is_parent')
g.add_edge(gerard, raf, rel='is_parent')

g.add_edge(martine, pierre_yves, rel='is_parent')
g.add_edge(gerard, pierre_yves, rel='is_parent')

g.add_edge(laou, flo, rel='is_parent')
g.add_edge(daniel, flo, rel='is_parent')

g.add_edge(laou, nicolas, rel='is_parent')
g.add_edge(daniel, nicolas, rel='is_parent')

g.add_edge(laou, clarisse, rel='is_parent')
g.add_edge(daniel, clarisse, rel='is_parent')

# parents -> children

# flo & raf family

g.add_edge(flo, theo, rel='is_parent')
g.add_edge(raf, theo, rel='is_parent')

g.add_edge(flo, nathan, rel='is_parent')
g.add_edge(raf, nathan, rel='is_parent')

g.add_edge(flo, noe, rel='is_parent')
g.add_edge(raf, noe, rel='is_parent')

# nathalie_g & pierre_yves family

g.add_edge(pierre_yves, gabriel, rel='is_parent')
g.add_edge(nathalie_g, gabriel, rel='is_parent')

# nathalie_c & nicolas family

g.add_edge(nathalie_c, hanae, rel='is_parent')
g.add_edge(nicolas, hanae, rel='is_parent')

g.add_edge(nathalie_c, come, rel='is_parent')
g.add_edge(nicolas, come, rel='is_parent')

# clarisse & thomas family

g.add_edge(clarisse, noam, rel='is_parent')
g.add_edge(thomas, noam, rel='is_parent')
