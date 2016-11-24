# -*- coding:utf-8 -*-

import tempfile


from grapheekdb.lib.exceptions import GrapheekDataException
from grapheekdb.lib.exceptions import GrapheekIndexRemovalFailedException
from grapheekdb.lib.exceptions import GrapheekIndexCreationFailedException

from grapheekdb.backends.data.keys import METADATA_VERTEX_COUNTER, METADATA_EDGE_COUNTER
from grapheekdb.backends.data.keys import METADATA_VERTEX_INDEX_PREFIX, METADATA_EDGE_INDEX_PREFIX

from .test_data_backend_localmem import TLocalMemoryGraph
from .test_data_backend_localmem import TAddingIndexesHasNoEffectsOnResultsLocalMem
from .data_backend_common import FillMethod


class TestKyotoCabinetGraph(TLocalMemoryGraph):

    def setup(self):
        from grapheekdb.backends.data.kyotocab import KyotoCabinetGraph
        self.dbpath = tempfile.mktemp() + '.kch'
        self.graph = KyotoCabinetGraph(self.dbpath)
        self.fill()

    def teardown(self):
        import os
        try:
            self.graph._db_close()
        except:
            pass
        os.remove(self.dbpath)

    def test_reopened_graph_has_same_content(self):
        # saving some metrics and info before closing database :
        before_node_count = self.graph.V().count()
        before_edge_count = self.graph.E().count()
        before_node_indexes = self.graph.get_node_indexes()
        before_edge_indexes = self.graph.get_edge_indexes()
        # closing database (in fact, graph, which indirectly will close db)
        self.graph.close()
        # Reopening database (in fact graph, which indirectly reopen db)
        from grapheekdb.backends.data.kyotocab import KyotoCabinetGraph
        self.graph = KyotoCabinetGraph(self.dbpath)
        after_node_count = self.graph.V().count()
        after_edge_count = self.graph.E().count()
        after_node_indexes = self.graph.get_node_indexes()
        after_edge_indexes = self.graph.get_edge_indexes()
        # Checking that everything is consistent :
        assert(before_node_count == after_node_count)
        assert(before_edge_count == after_edge_count)
        assert(before_node_indexes == after_node_indexes)
        assert(before_edge_indexes == after_edge_indexes)


class TestEmptyKyotoCabinetGraph(object):

    def setup(self):
        from grapheekdb.backends.data.kyotocab import KyotoCabinetGraph
        self.dbpath = tempfile.mktemp() + '.kch'
        self.graph = KyotoCabinetGraph(self.dbpath)

    def teardown(self):
        import os
        self.graph._db_close()
        os.remove(self.dbpath)

    def test_empty_database(self):
        assert(self.graph.V().count() == 0)
        assert(self.graph.E().count() == 0)


class TestKyotoCabinetGraphBadExtension(object):

    def test_bad_file_extension(self):
        from grapheekdb.backends.data.kyotocab import GrapheekDataKyotoCabinetInitFailureException
        exception_raised = False
        try:
            from grapheekdb.backends.data.kyotocab import KyotoCabinetGraph
            self.dbpath = tempfile.mktemp() + '.xxx'
            self.graph = KyotoCabinetGraph(self.dbpath)
        except GrapheekDataKyotoCabinetInitFailureException:
            exception_raised = True
        assert(exception_raised)


class TestAddingIndexesHasNoEffectsOnTestResultsKyotoCab(TAddingIndexesHasNoEffectsOnResultsLocalMem):

    def setup(self):
        from grapheekdb.backends.data.kyotocab import KyotoCabinetGraph
        self.dbpath = tempfile.mktemp() + '.kch'
        self.graph = KyotoCabinetGraph(self.dbpath)
        self.fill()
        self.graph.add_node_index('foo')

    def teardown(self):
        import os
        os.remove(self.dbpath)


class TModifiedBackendLeadToError(FillMethod):

    def setup(self):
        from grapheekdb.backends.data.localmem import LocalMemoryGraph
        self.graph = LocalMemoryGraph()
        self.fill()

    def test_create_index_node(self):
        # Manually modifying backend
        txn = self.graph._transaction_begin()
        try:
            self.graph._remove(txn, METADATA_VERTEX_COUNTER)
            self.graph._transaction_commit(txn)
        except:
            self.graph._transaction_rollback(txn)
        exception_raised = False
        try:
            self.graph.add_node_index('foo')
        except GrapheekIndexCreationFailedException:
            exception_raised = True
        assert(exception_raised)

    def test_remove_index_node(self):
        from grapheekdb.backends.data.indexes import normalize_value
        self.graph.add_node_index('foo')
        txn = self.graph._transaction_begin()
        args = ['foo']
        kwargs = {}
        index_signature = [args, list(kwargs.items())]
        index_signature_string = normalize_value(index_signature)
        index_key = '/'.join([METADATA_VERTEX_INDEX_PREFIX] + [index_signature_string])
        try:
            self.graph._remove(txn, index_key)
            self.graph._transaction_commit(txn)
        except:
            self.graph._transaction_rollback(txn)
        exception_raised = False
        try:
            self.graph.remove_node_index('foo')
        except GrapheekIndexRemovalFailedException:
            exception_raised = True
        assert(exception_raised)

    def test_create_index_edge(self):
        # Manually modifying backend
        txn = self.graph._transaction_begin()
        try:
            self.graph._remove(txn, METADATA_EDGE_COUNTER)
            self.graph._transaction_commit(txn)
        except:
            self.graph._transaction_rollback(txn)
        exception_raised = False
        try:
            self.graph.add_edge_index('foo')
        except GrapheekIndexCreationFailedException:
            exception_raised = True
        assert(exception_raised)

    def test_remove_index_edge(self):
        from grapheekdb.backends.data.indexes import normalize_value
        self.graph.add_edge_index('foo')
        txn = self.graph._transaction_begin()
        args = ['foo']
        kwargs = {}
        index_signature = [args, list(kwargs.items())]
        index_signature_string = normalize_value(index_signature)
        index_key = '/'.join([METADATA_EDGE_INDEX_PREFIX] + [index_signature_string])
        try:
            # Manually modifying backend
            self.graph._remove(txn, index_key)
            self.graph._transaction_commit(txn)
        except:
            self.graph._transaction_rollback(txn)
        exception_raised = False
        try:
            self.graph.remove_edge_index('foo')
        except GrapheekIndexRemovalFailedException:
            exception_raised = True
        assert(exception_raised)

    def test_add_node(self):
        # Manually modifying backend
        txn = self.graph._transaction_begin()
        try:
            self.graph._remove(txn, METADATA_VERTEX_COUNTER)
            self.graph._transaction_commit(txn)
        except:
            self.graph._transaction_rollback(txn)
        exception_raised = False
        try:
            self.graph.add_node(foo=3)
        except GrapheekDataException:
            exception_raised = True
        assert(exception_raised)

    def test_remove_node(self):
        # Manually modifying backend
        from grapheekdb.backends.data.keys import KIND_VERTEX, OUT_EDGES_SUFFIX
        from grapheekdb.backends.data.keys import build_key
        txn = self.graph._transaction_begin()
        try:
            self.graph._remove(txn, build_key(KIND_VERTEX, self.n1.get_id(), OUT_EDGES_SUFFIX))
            self.graph._transaction_commit(txn)
        except:
            self.graph._transaction_rollback(txn)
        exception_raised = False
        try:
            self.n1.remove()
        except GrapheekDataException:
            exception_raised = True
        assert(exception_raised)

    def test_update_node_data(self):
        # Manually modifying backend
        from grapheekdb.backends.data.keys import KIND_VERTEX, DATA_SUFFIX
        from grapheekdb.backends.data.keys import build_key
        txn = self.graph._transaction_begin()
        try:
            self.graph._remove(txn, build_key(KIND_VERTEX, self.n1.get_id(), DATA_SUFFIX))
            self.graph._transaction_commit(txn)
        except:
            self.graph._transaction_rollback(txn)
        exception_raised = False
        try:
            self.n1.foobar = 3
        except GrapheekDataException:
            exception_raised = True
        assert(exception_raised)

    def test_add_edge(self):
        # Manually modifying backend
        txn = self.graph._transaction_begin()
        try:
            self.graph._remove(txn, METADATA_EDGE_COUNTER)
            self.graph._transaction_commit(txn)
        except:
            self.graph._transaction_rollback(txn)
        exception_raised = False
        try:
            self.graph.add_edge(self.n1, self.n2)
        except GrapheekDataException:
            exception_raised = True
        assert(exception_raised)

    def test_remove_edge(self):
        # Manually modifying backend
        from grapheekdb.backends.data.keys import KIND_EDGE, OUT_VERTICES_SUFFIX
        from grapheekdb.backends.data.keys import build_key
        txn = self.graph._transaction_begin()
        try:
            self.graph._remove(txn, build_key(KIND_EDGE, self.e1.get_id(), OUT_VERTICES_SUFFIX))
            self.graph._transaction_commit(txn)
        except:
            self.graph._transaction_rollback(txn)
        exception_raised = False
        try:
            self.e1.remove()
        except GrapheekDataException:
            exception_raised = True
        assert(exception_raised)

    def test_update_edge_data(self):
        # Manually modifying backend
        from grapheekdb.backends.data.keys import KIND_EDGE, DATA_SUFFIX
        from grapheekdb.backends.data.keys import build_key
        txn = self.graph._transaction_begin()
        try:
            self.graph._remove(txn, build_key(KIND_EDGE, self.e1.get_id(), DATA_SUFFIX))
            self.graph._transaction_commit(txn)
        except:
            self.graph._transaction_rollback(txn)
        exception_raised = False
        try:
            self.e1.foobar = 3
        except GrapheekDataException:
            exception_raised = True
        assert(exception_raised)


class TestModifiedBackendLeadToErrorKT(TModifiedBackendLeadToError):

    def setup(self):
        from grapheekdb.backends.data.kyotocab import KyotoCabinetGraph
        self.dbpath = tempfile.mktemp() + '.kch'
        self.graph = KyotoCabinetGraph(self.dbpath)
        self.fill()

    def teardown(self):
        import os
        os.remove(self.dbpath)


class TestKyotoCabinetDatabaseReopening(FillMethod):

    def test_reopening_indexes_ok(self):
        from grapheekdb.backends.data.kyotocab import KyotoCabinetGraph
        self.dbpath = tempfile.mktemp() + '.kch'
        self.graph = KyotoCabinetGraph(self.dbpath)
        self.fill()
        self.graph.add_node_index("foo")
        self.graph.add_edge_index("foo")
        before_node_indexes_count = len(self.graph.get_node_indexes())
        before_edge_indexes_count = len(self.graph.get_edge_indexes())
        del(self.graph)
        self.graph = KyotoCabinetGraph(self.dbpath)
        after_node_indexes_count = len(self.graph.get_node_indexes())
        after_edge_indexes_count = len(self.graph.get_edge_indexes())
        assert(before_node_indexes_count == after_node_indexes_count)
        assert(before_edge_indexes_count == after_edge_indexes_count)

    def test_remove_index_dont_break_graph(self):
        # see https://bitbucket.org/nidusfr/grapheekdb/issue/9
        from grapheekdb.backends.data.kyotocab import KyotoCabinetGraph
        f = tempfile.NamedTemporaryFile(suffix=".kch")
        g = KyotoCabinetGraph(path=f.name)
        g.add_node_index("foo")
        a = g.add_node(foo="bar")
        g.remove_node_index("foo")
        g = KyotoCabinetGraph(path=f.name)

