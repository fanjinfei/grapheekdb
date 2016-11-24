#!/usr/bin/env
# -*- coding: utf-8 -*-

from collections import defaultdict


class Addition(object):

    def __init__(self):
        self._append_to_lst_registry = defaultdict(list)
        self._update_inc_registry = defaultdict(int)
        self._init_lst_registry = set()
        self._set_registry = {}
        self._applied = False

    def merge(self, other):
        for key, entity_ids in other._append_to_lst_registry.items():
            self._append_to_lst_registry[key].extend(entity_ids)
        for key, value in other._update_inc_registry.items():
            self._update_inc_registry[key] += value
        self._init_lst_registry.update(other._init_lst_registry)
        self._set_registry.update(other._set_registry)

    def append_to_lst(self, key, entity_id):
        self._append_to_lst_registry[key].append(entity_id)

    def update_inc(self, key):
        self._update_inc_registry[key] += 1

    def init_lst(self, key):
        self._init_lst_registry.add(key)

    def set(self, key, value):
        self._set_registry[key] = value

    def apply(self, txn, graph):
        assert(not(self._applied))
        # Init lst :
        for key in self._init_lst_registry:
            graph._init_lst(txn, key)
        # Add entity_ids to lists
        for key, entity_ids in self._append_to_lst_registry.items():
            graph._bulk_append_to_lst(txn, key, entity_ids)
        # Adding key value :
        graph._bulk_set(txn, self._set_registry)
        # ---
        self._applied = True
        # Increasing value
        for key, value in self._update_inc_registry.items():
            graph._update_inc(txn, key, value)


class Removal(object):

    def __init__(self):
        self._remove_from_lst_registry = defaultdict(list)
        self._update_inc_registry = defaultdict(int)
        self._update_dec_registry = defaultdict(int)
        self._remove_registry = set()
        self._applied = False

    def merge(self, other):
        for key, entity_ids in other._remove_from_lst_registry.items():
            self._remove_from_lst_registry[key].extend(entity_ids)
        for key, value in other._update_inc_registry.items():
            self._update_inc_registry[key] += value
        for key, value in other._update_dec_registry.items():
            self._update_dec_registry[key] += value
        self._remove_registry.update(other._remove_registry)

    def remove_from_lst(self, key, entity_id):
        self._remove_from_lst_registry[key].append(entity_id)

    def update_inc(self, key):
        self._update_inc_registry[key] += 1

    def update_dec(self, key):
        self._update_dec_registry[key] += 1

    def remove(self, key):
        self._remove_registry.add(key)

    def apply(self, txn, graph):
        assert(not(self._applied))
        # Removing entity_ids from lists
        for key, entity_ids in self._remove_from_lst_registry.items():
            graph._bulk_remove_from_lst(txn, key, entity_ids)
        # Increasing value
        for key, value in self._update_inc_registry.items():
            graph._update_inc(txn, key, value)
        # Decreasing value
        for key, value in self._update_dec_registry.items():
            graph._update_dec(txn, key, value)
        # Removing keys :
        graph._bulk_remove(txn, self._remove_registry)
        # ---
        self._applied = True
