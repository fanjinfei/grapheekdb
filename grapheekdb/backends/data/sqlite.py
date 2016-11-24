# -*- coding:utf-8 -*-

"""
Initially contributed by Edwin Cox
CAUTION : given the sqlite3 autocommit feature, I (raphael) cannot ensure that graph won't become inconsistent
"""

import sys
import sqlite3
import json
from grapheekdb.backends.data.base import BaseGraph
from grapheekdb.lib.undef import UNDEFINED

PYTHON2 = sys.version_info.major == 2


class SqliteGraph(BaseGraph):

    def __init__(self, filename):
        # create the database object
        self._filename = filename
        # open the database

        self._db = sqlite3.connect(filename)
        self._initialize_db()
        super(SqliteGraph, self).__init__()
        self._ensure_prepared()
        self._closed = False

    def _initialize_db(self):
        self._c = self._db.cursor()
        self._c.execute("create table if not exists storage (key text PRIMARY KEY not null, value text)")

    # Start method overriding :

    def _db_close(self):
        self._db.close()

    def _transaction_begin(self):
        return True

    def _transaction_commit(self, txn):
        self._db.commit()

    def _transaction_rollback(self, txn):
        self._db.rollback()

    def _has_key(self, key):
        c = self._c
        c.execute("select count(value) from storage where key= ?", (key,))
        value = c.fetchone()
        return int(value[0]) == 1

    def _get(self, txn, key):
        c = self._c
        c.execute("select value from storage where key= ?", (key,))
        raw_data = c.fetchone()
        if not (raw_data is None):
            data = str(raw_data[0])
            if PYTHON2:
                return json.loads(data, encoding='utf8')
            else:
                return json.loads(data)
        else:
            return UNDEFINED  # Not returning None, as None is a valid value

    def _bulk_get(self, txn, keys):
        c = self._c
        result = {}
        for line in c.execute("select key,value from storage where key in (" + ",".join("'{0}'".format(w) for w in keys) + ")"):
            k = line[0]
            if PYTHON2:
                result[k] = json.loads(str(line[1]), encoding='utf8')
            else:
                result[k] = json.loads(str(line[1]))
        return result

    def _set(self, txn, key, value):
        if PYTHON2:
            d = json.dumps(value, encoding='utf8')
        else:
            d = json.dumps(value)
        c = self._c
        c.execute("delete from storage where key = ?", (key,))
        if PYTHON2:
            c.execute("insert into storage (key, value) values (?,?)", (key, sqlite3.Binary(d)))
        else:
            c.execute("insert into storage (key, value) values (?,?)", (key, d))

    def _bulk_set(self, txn, updates):
        keys = list()
        values = list()
        for key, value in list(updates.items()):
            keys.append("'" + key + "'")
            if PYTHON2:
                values.append((key, sqlite3.Binary(json.dumps(value, encoding='utf8'))))
            else:
                values.append((key, json.dumps(value)))
        c = self._c
        c.execute("delete from storage where key in (" + ",".join(keys) + ")")
        c.executemany("insert into storage (key, value) values (?,?)", values)

    def _remove(self, txn, key):
        c = self._c
        c.execute("delete from storage where key = ?", (key,))

    def _bulk_remove(self, txn, keys):
        c = self._c
        c.execute("delete from storage where key in (" + ",".join("'{0}'".format(w) for w in keys) + ")")

    def _remove_prefix(self, txn, prefix):
        c = self._c
        c.execute("delete from storage where key like '" + prefix + "%'")

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
