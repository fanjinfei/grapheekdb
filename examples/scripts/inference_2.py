#!/usr/bin/env python
# -*- coding: utf-8 -*-

from .inference_data import g

########################################################
#  Same examples as inference_1.py using "shortcuts"   #
########################################################

# Who are the grand parents ? of who ?

for grandparent, grandchild in g.V().aka('x').out_(2, rel='is_parent').aka('y').collect('x', 'y'):
    print(grandparent.name, 'is grandparent of', grandchild.name)

# Who is the brother of who ?

for man, other in g.V(gender='m').aka('x').in_(rel='is_parent').out_(rel='is_parent').dedup().aka('y').collect('x', 'y'):
    if man != other:
        print(man.name, 'is the brother of', other.name)

# Who are cousin ?

cousins = set()

for cousin1, grandparent, cousin2 in g.V().aka('x').in_(rel='is_parent').in_(rel='is_parent').aka('grandparent').out_(2, rel='is_parent').aka('y').collect('x', 'grandparent', 'y'):
    if not(cousin2 in list(cousin1.in_(rel='is_parent').out_(rel='is_parent'))):
        cousins.add((cousin1.name, cousin2.name, grandparent.name))

for name1, name2, gp_name in sorted(list(cousins)):
    print(name1, 'is cousin of', name2, '(by %s)' % gp_name)




