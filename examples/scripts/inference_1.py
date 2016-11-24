#!/usr/bin/env python
# -*- coding: utf-8 -*-

from .inference_data import g

#######################################
#         Inference examples          #
#######################################

# Who are the grand parents ? of who ?

for grandparent, grandchild in g.V().aka('x').outE(rel='is_parent').outV().outE(rel='is_parent').outV().aka('y').collect('x', 'y'):
    print(grandparent.name, 'is grandparent of', grandchild.name)

# Who is the brother of who ?

for man, other in g.V(gender='m').aka('x').inE(rel='is_parent').inV().outE(rel='is_parent').outV().dedup().aka('y').collect('x', 'y'):
    if man != other:
        print(man.name, 'is the brother of', other.name)

# Who are cousin ?

cousins = set()

for cousin1, grandparent, cousin2 in g.V().aka('x').inE(rel='is_parent').inV().inE(rel='is_parent').inV().aka('grandparent').outE(rel='is_parent').outV().outE(rel='is_parent').outV().aka('y').collect('x', 'grandparent', 'y'):
    if not(cousin2 in list(cousin1.inE(rel='is_parent').inV().outE(rel='is_parent').outV())):
        cousins.add((cousin1.name, cousin2.name, grandparent.name))

for name1, name2, gp_name in sorted(list(cousins)):
    print(name1, 'is cousin of', name2, '(by %s)' % gp_name)




