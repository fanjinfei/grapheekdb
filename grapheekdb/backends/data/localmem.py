# -*- coding:utf-8 -*-

from grapheekdb.backends.data.base import BaseGraph

from grapheekdb.lib.undef import UNDEFINED


class LocalMemoryGraph(BaseGraph):

    def __init__(self):
        self._dic = {}
        super(LocalMemoryGraph, self).__init__()
        self._ensure_prepared()
        self._closed = False

    # Start method overriding :

    def _db_close(self):  # pragma : no cover
        pass

    def _transaction_begin(self):
        return True

    def _transaction_commit(self, txn):
        pass

    def _transaction_rollback(self, txn):
        pass

    def _has_key(self, key):
        return key in self._dic

    def _get(self, txn, key):
        return self._dic.get(key, UNDEFINED)  # Not returning None, as None is a valid value

    def _bulk_get(self, txn, keys):
        dic = {}
        for key in keys:
            value = self._get(txn, key)
            if value != UNDEFINED:
                dic[key] = value
        return dic

    def _set(self, txn, key, value):
        self._dic[key] = value

    def _bulk_set(self, txn, updates):
        self._dic.update(updates)

    def _remove(self, txn, key):
        try:
            del self._dic[key]
        except KeyError:
            pass  # I want _remove method to be idempotent

    def _bulk_remove(self, txn, keys):
        for key in keys:
            try:
                del self._dic[key]
            except KeyError:  # pragma : no cover
                pass  # I want _remove method to be idempotent

    def _remove_prefix(self, txn, prefix):
        remove_keys = [key for key in self._dic if key.startswith(prefix)]
        for key in remove_keys:
            self._remove(txn, key)

    def _append_to_lst(self, txn, key, value):
        lst = self._get(txn, key)
        if lst == UNDEFINED:
            lst = []
            lst.append(value)
            self._set(txn, key, lst)
        else:
            lst.append(value)

    def _bulk_append_to_lst(self, txn, key, value):
        lst = self._get(txn, key)
        if lst == UNDEFINED:
            lst = []
            lst.extend(value)
            self._set(txn, key, lst)
        else:
            lst.extend(value)

