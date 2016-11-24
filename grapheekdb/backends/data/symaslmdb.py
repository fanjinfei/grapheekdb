# -*- coding:utf-8 -*-

import sys

import lmdb
import msgpack

from grapheekdb.backends.data.base import BaseGraph

from grapheekdb.lib.undef import UNDEFINED

PYTHON3 = sys.version_info.major == 3

class LmdbGraph(BaseGraph):

    def __init__(self, path, map_size=1024 * 1024):
        # create the database object
        self._path = path
        # open the database
        self._env = lmdb.open(path, map_size=map_size)
        super(LmdbGraph, self).__init__()
        self._ensure_prepared()
        self._closed = False

    # Start method overriding :

    def _db_close(self):
        if not self._closed:
            self._env.close()

    def _transaction_begin(self):
        return self._env.begin(write=True)

    def _transaction_commit(self, txn):
        txn.commit()

    def _transaction_rollback(self, txn):
        txn.abort()

    def _has_key(self, key):
        k = key
        if PYTHON3:  # pragma : no cover
            k = bytes(k, encoding='utf8')
        with self._env.begin() as txn:
            return txn.get(k, UNDEFINED) != UNDEFINED

    def _get(self, txn, key):
        k = key
        if PYTHON3:  # pragma : no cover
            k = bytes(k, encoding='utf8')
        if txn is not None:
            raw_data = txn.get(k, UNDEFINED)
            if raw_data == UNDEFINED:
                return UNDEFINED
            res = msgpack.loads(raw_data, encoding='utf8')
            return res
        else:
            with self._env.begin() as txn:
                raw_data = txn.get(k, UNDEFINED)
                if raw_data == UNDEFINED:
                    return UNDEFINED
                res = msgpack.loads(raw_data, encoding='utf8')
                return res

    def _bulk_get(self, txn, keys):
        dic = {}
        for key in keys:
            value = self._get(txn, key)
            if value != UNDEFINED:
                dic[key] = value
        return dic

    def _set(self, txn, key, value):
        k = key
        if PYTHON3:  # pragma : no cover
            k = bytes(k, encoding='utf8')
        txn.put(k, msgpack.dumps(value, encoding='utf8'))

    def _bulk_set(self, txn, updates):
        for key, value in list(updates.items()):
            self._set(txn, key, value)

    def _remove(self, txn, key):
        k = key
        if PYTHON3:  # pragma : no cover
            k = bytes(k, encoding='utf8')
        txn.delete(k)

    def _bulk_remove(self, txn, keys):
        for key in keys:
            self._remove(txn, key)

    def _remove_prefix(self, txn, prefix):
        cursor = txn.cursor()
        for key, _ in cursor:
            if PYTHON3:  # pragma : no cover
                key = str(key, encoding='utf8')
            if key.startswith(prefix):
                cursor.delete()
