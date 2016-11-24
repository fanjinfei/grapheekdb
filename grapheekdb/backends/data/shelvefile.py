# -*- coding:utf-8 -*-

"""
Initially contributed by Edwin Cox
CAUTION : shelve is missing transactions, so graph can progressively become inconsistent
"""

import shelve
from grapheekdb.backends.data.base import BaseGraph
from grapheekdb.lib.undef import UNDEFINED


class ShelveGraph(BaseGraph):

    def __init__(self, filename):
        # create the database object
        self._filename = filename
        # open the database
        self._db = shelve.open(filename)
        super(ShelveGraph, self).__init__()
        self._ensure_prepared()
        self._closed = False

    # Start method overriding :

    def _db_close(self):
        if not self._closed:
            self._db.close()

    def _transaction_begin(self):
        return True

    def _transaction_commit(self, txn):
        pass

    def _transaction_rollback(self, txn):
        pass

    def _has_key(self, key):
        return key in self._db

    def _get(self, txn, key):
        try:
            return self._db[key]
        except KeyError:
            return UNDEFINED  # Not returning None, as None is a valid value

    def _bulk_get(self, txn, keys):
        result = {}
        for key in keys:
            value = self._get(txn, key)
            if value != UNDEFINED:
                result[key] = value
        return result

    def _set(self, txn, key, value):
        self._db[key] = value

    def _bulk_set(self, txn, updates):
        self._db.update(updates)

    def _remove(self, txn, key):
        try:
            del self._db[key]
        except KeyError:
            pass

    def _bulk_remove(self, txn, keys):
        for key in keys:
            try:
                del self._db[key]
            except KeyError:
                pass

    def _remove_prefix(self, txn, prefix):
        remove_keys = [key for key in self._db.keys() if key.startswith(prefix)]
        for key in remove_keys:
            self._remove(txn, key)

    def _append_to_lst(self, txn, key, value):
        lst = self._get(txn, key)
        if lst == UNDEFINED:
            lst = [value]
            self._set(txn, key, lst)
        else:
            lst.append(value)
            self._set(txn, key, lst)

    def _bulk_append_to_lst(self, txn, key, value):
        lst = self._get(txn, key)
        if lst == UNDEFINED:
            lst = []
            lst.extend(value)
            self._set(txn, key, lst)
        else:
            lst.extend(value)
            self._set(txn, key, lst)
