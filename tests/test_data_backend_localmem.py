# -*- coding:utf-8 -*-

from copy import deepcopy

from grapheekdb.backends.data.base import Node, Edge, BaseGraph

from grapheekdb.lib.exceptions import GrapheekDataException
from grapheekdb.lib.exceptions import GrapheekNoSuchTraversalException
from grapheekdb.lib.exceptions import GrapheekInvalidLookupException
from grapheekdb.lib.exceptions import GrapheekIndexAlreadyExistsException
from grapheekdb.lib.exceptions import GrapheekDataPreparationFailedException
from grapheekdb.lib.exceptions import GrapheekIndexRemovalFailedException
from grapheekdb.lib.exceptions import GrapheekIndexCreationFailedException
from grapheekdb.lib.exceptions import GrapheekSubLookupNotImplementedException
from grapheekdb.lib.exceptions import GrapheekUnknownScriptException

from grapheekdb.backends.data.keys import METADATA_VERTEX_COUNTER, METADATA_EDGE_COUNTER
from grapheekdb.backends.data.keys import METADATA_VERTEX_INDEX_COUNTER, METADATA_EDGE_INDEX_COUNTER
from grapheekdb.backends.data.keys import METADATA_VERTEX_INDEX_PREFIX, METADATA_EDGE_INDEX_PREFIX
from grapheekdb.backends.data.keys import CHUNK_SIZE

from .data_backend_common import FillMethod, CommonMethods


class TLocalMemoryGraph(FillMethod, CommonMethods):  # Not using "Test" in names so that I can import it elsewhere without running tests 2 times

    def setup(self):
        from grapheekdb.backends.data.localmem import LocalMemoryGraph
        self.graph = LocalMemoryGraph()
        self.fill()

    # Type checking :

    def test_self_graph_is_a_base_graph(self):
        assert(isinstance(self.graph, BaseGraph))

    def test_n1_is_a_node(self):
        assert(isinstance(self.n1, Node))

    def test_n2_is_a_node(self):
        assert(isinstance(self.n2, Node))

    def test_e1_is_a_edge(self):
        assert(isinstance(self.e1, Edge))

    # Type checking after a lookup on iterator :

    def test_node_lookup_result_are_node(self):
        items = self.graph.V(foo__gte=1)
        assert(items.count())  # Just want to be sure that the assert clause in loop will be executed
        for item in items:
            assert(isinstance(item, Node))

    def test_edge_lookup_result_are_edge(self):
        items = self.graph.E(common__gte=1)
        assert(items.count())  # Just want to be sure that the assert clause in loop will be executed
        for item in items:
            assert(isinstance(item, Edge))

    # Test invalid traversals :

    def test_edge_oe_is_forbidden(self):
        exception_raised = False
        try:
            self.graph.E(label='knows').outE().count()
        except GrapheekNoSuchTraversalException:
            exception_raised = True
        assert(exception_raised)

    def test_edge_ie_is_forbidden(self):
        exception_raised = False
        try:
            self.graph.E(label='knows').inE().count()
        except GrapheekNoSuchTraversalException:
            exception_raised = True
        assert(exception_raised)

    def test_edge_be_is_forbidden(self):
        exception_raised = False
        try:
            self.graph.E(label='knows').bothE().count()
        except GrapheekNoSuchTraversalException:
            exception_raised = True
        assert(exception_raised)

    def test_node_oeoe(self):
        exception_raised = False
        try:
            self.n2.outE().outE()
        except GrapheekNoSuchTraversalException:
            exception_raised = True
        assert(exception_raised)

    def test_node_oeie(self):
        exception_raised = False
        try:
            self.n3.outE().inE()
        except GrapheekNoSuchTraversalException:
            exception_raised = True
        assert(exception_raised)

    def test_node_ieoe(self):
        exception_raised = False
        try:
            self.n2.inE().outE()
        except GrapheekNoSuchTraversalException:
            exception_raised = True
        assert(exception_raised)

    def test_node_ieie(self):
        exception_raised = False
        try:
            self.n2.inE().outE()
        except GrapheekNoSuchTraversalException:
            exception_raised = True
        assert(exception_raised)

    def test_node_beoe(self):
        exception_raised = False
        try:
            self.n2.bothE().outE()
        except GrapheekNoSuchTraversalException:
            exception_raised = True
        assert(exception_raised)

    def test_node_beie(self):
        exception_raised = False
        try:
            self.n2.bothE().outE()
        except GrapheekNoSuchTraversalException:
            exception_raised = True
        assert(exception_raised)

    # test invalid lookups

    def test_filter_node_invalid_no_such_clause_1(self):
        exception_raised = False
        try:
            self.graph.V(foo__xxxxxxx=1).count()
        except GrapheekInvalidLookupException:
            exception_raised = True
        assert(exception_raised)

    def test_filter_node_invalid_no_such_clause_2(self):
        exception_raised = False
        try:
            self.n1.outV(foo__xxxxxxx=1).count()
        except GrapheekInvalidLookupException:
            exception_raised = True
        assert(exception_raised)

    def test_filter_node_invalid_no_subfield_lookup_1(self):
        # This can evolve in the future : implementing lookup like field__attr__gt=1
        # shouldn't be that complicated
        exception_raised = False
        try:
            self.graph.V(foo__subfoo__gt=1).count()
        except GrapheekSubLookupNotImplementedException:
            exception_raised = True
        assert(exception_raised)

    def test_filter_node_invalid_no_subfield_lookup_2(self):
        # This can evolve in the future : implementing lookup like field__attr__gt=1
        # shouldn't be that complicated
        exception_raised = False
        try:
            self.graph.V(name='Raf').bothV(foo__xxx__gt=1).count()
        except GrapheekSubLookupNotImplementedException:
            exception_raised = True
        assert(exception_raised)

    def test_index_on_lookup(self):
        count = CHUNK_SIZE + 100
        data = []
        for i in range(count):
            data.append({'document_id': i})
        nodes = self.graph.bulk_add_node(data)
        self.graph.add_node_index('document_id')
        # Adding edges from self.n1 to every node :
        data = []
        for node in nodes:
            data.append((self.n1, node, {}))
        self.graph.bulk_add_edge(data)
        # Now doing the test :
        self.graph.V(name='Raf').outV(document_id=500).count()

    def test_node_index_is_used_to_reduce_calls(self):
        import cProfile
        import pstats
        count = CHUNK_SIZE + 100
        """
        Dunno how to do this in a clean way
        So this test is a workaround : I'm just checking that there's far less calls
        when an index exists
        TODO : This test must be recoded or removed
        """
        # Getting exact lookup total calls WITHOUT index :
        data = []
        for i in range(count):
            data.append({'document_id': i})
            #self.graph.add_node(document_id=i)
        self.graph.bulk_add_node(data)
        # Now the hack part :
        pr = cProfile.Profile()
        pr.enable()
        self.graph.V(document_id=500).count()
        pr.disable()
        stats = pstats.Stats(pr)
        total_calls_without_index = stats.total_calls
        # Getting exact lookup total calls WITH index :
        self.graph.add_node_index('document_id')
        pr = cProfile.Profile()
        pr.enable()
        self.graph.V(document_id=500).count()
        pr.disable()
        stats = pstats.Stats(pr)
        total_calls_with_index = stats.total_calls
        assert(total_calls_with_index < total_calls_without_index / 20)  # 20 is totally subjective

    def test_node_index_is_used_to_reduce_calls_in_lookup(self):
        import cProfile
        import pstats
        count = CHUNK_SIZE + 100
        """
        Dunno how to do this in a clean way
        So this test is a workaround : I'm just checking that there's far less calls
        when an index exists
        TODO : This test must be recoded or removed
        """
        # Getting exact lookup total calls WITHOUT index :
        data = []
        for i in range(count):
            data.append(dict(document_id=i))
        self.graph.bulk_add_node(data)
        # Now the hack part :
        pr = cProfile.Profile()
        pr.enable()
        self.graph.V(document_id=500).count()
        pr.disable()
        stats = pstats.Stats(pr)
        total_calls_without_index = stats.total_calls
        # Getting exact lookup total calls WITH index :
        self.graph.add_node_index('document_id')
        pr = cProfile.Profile()
        pr.enable()
        self.graph.V(document_id=500).count()
        pr.disable()
        stats = pstats.Stats(pr)
        total_calls_with_index = stats.total_calls
        assert(total_calls_with_index < total_calls_without_index / 20)  # 20 is totally subjective

    def test_edge_index_is_used(self):
        import cProfile
        import pstats
        count = CHUNK_SIZE + 100
        """
        Dunno how to do this in a clean way
        So this test is a workaround : I'm just checking that there's far less calls
        when an index exists
        TODO : This test must be recoded or removed
        """
        # Getting exact lookup total calls WITHOUT index :
        nodes = []
        data = []
        for i in range(count):
            data.append(dict(document_id=i))
        nodes = self.graph.bulk_add_node(data)
        counter = 0
        data = []
        for start, end in zip(nodes, nodes[1:]):
            data.append((start, end, dict(counter=counter)))
            counter += 1
        self.graph.bulk_add_edge(data)
        # Now the hack part :
        pr = cProfile.Profile()
        pr.enable()
        self.graph.E(counter=500).count()
        pr.disable()
        stats = pstats.Stats(pr)
        total_calls_without_index = stats.total_calls
        # Getting exact lookup total calls WITH index :
        self.graph.add_edge_index('counter')
        pr = cProfile.Profile()
        pr.enable()
        self.graph.E(counter=500).count()
        pr.disable()
        stats = pstats.Stats(pr)
        total_calls_with_index = stats.total_calls
        assert(total_calls_with_index < total_calls_without_index / 20)  # 20 is totally subjective

    def test_index_multi_chunk(self):
        count = CHUNK_SIZE + 100
        data = []
        # I just want to create an chunk with more than <CHUNK_SIZE> entity ids
        # to check that index is still working
        for i in range(count):
            data.append(dict(document_id=1))
        nodes = self.graph.bulk_add_node(data)
        node_ids = [node.get_id() for node in nodes]
        self.graph.add_node_index('document_id')
        # Forcing index use :
        good_index = None
        for index in self.graph._node_indexes:
            if index._fields == ['document_id']:
                good_index = index
        assert(good_index is not None)
        indexed_ids = list(index.ids(None, dict(document_id=1)))
        assert(set(node_ids) == set(indexed_ids))
        assert(len(indexed_ids) > CHUNK_SIZE)  # maybe overkill (?)

    def test_index_tells_that_it_is_incompetent(self):
        unknown_key = 'qklnfmqvnmkljnsfklsmlkdfjqslkdfjlkj'
        # Ensure there's at least an index
        self.graph.add_node_index('foobar')
        # Asking each index an estimation :
        for index in self.graph._node_indexes:
            assert(index.estimate(None, {unknown_key: 1}) == -1)

    def test_index_returns_none_when_it_is_incompetent(self):
        from grapheekdb.lib.exceptions import GrapheekIncompetentIndexException
        unknown_key = 'qklnfmqvnmkljnsfklsmlkdfjqslkdfjlkj'
        # Ensure there's at least an index
        self.graph.add_node_index('foobar')
        # Asking each index an estimation :
        raise_exceptions = []
        for index in self.graph._node_indexes:
            try:
                list(index.ids(None, {unknown_key: 1}))
            except GrapheekIncompetentIndexException:
                raise_exceptions.append(True)
            else:
                raise_exceptions.append(False)
        assert(all(raise_exceptions))

    def test_index_returns_empty_list_when_no_element_match_criteria(self):
        # Ensure there's at least an index
        self.graph.add_node_index('foobar')
        # Forcing index use :
        good_index = None
        for index in self.graph._node_indexes:
            if index._fields == ['foobar']:
                good_index = index
        assert(good_index is not None)
        assert(list(good_index.ids(None, dict(foobar=1))) == [])

    def test_index_estimate_returns_zero_when_no_element_match_criteria(self):
        # Ensure there's at least an index
        self.graph.add_node_index('foobar')
        # Forcing index use :
        good_index = None
        for index in self.graph._node_indexes:
            if index._fields == ['foobar']:
                good_index = index
        assert(good_index is not None)
        assert(good_index.estimate(None, dict(foobar=1)) == 0)

    # test index removal

    def test_index_removal(self):
        import cProfile
        import pstats
        count = CHUNK_SIZE + 1000

        # Getting exact lookup total calls WITHOUT index :
        data = []
        for i in range(count):
            data.append(dict(document_id=i))
        self.graph.bulk_add_node(data)
        # Now the hack part :
        pr = cProfile.Profile()
        pr.enable()
        self.graph.V(document_id=500).count()
        pr.disable()
        stats = pstats.Stats(pr)
        total_calls_without_index1 = stats.total_calls
        # Now adding and removing index, check that total calls is the same :
        self.graph.add_node_index('document_id')
        self.graph.remove_node_index('document_id')
        pr = cProfile.Profile()
        pr.enable()
        self.graph.V(document_id=500).count()
        pr.disable()
        stats = pstats.Stats(pr)
        total_calls_without_index2 = stats.total_calls
        # Hum... bad test :(
        assert(0.90 < float(total_calls_without_index1) / float(total_calls_without_index2) < 1.10)

    def test_unexisting_index_removal(self):
        exception_raised = False
        try:
            self.graph.remove_node_index()
        except GrapheekIndexRemovalFailedException:
            exception_raised = True
        assert(exception_raised)

    # test index multiple addition

    def test_node_index_multiple_addition(self):
        self.graph.add_node_index('foo')
        exception_raised = False
        try:
            self.graph.add_node_index('foo')
        except GrapheekIndexAlreadyExistsException:
            exception_raised = True
        assert(exception_raised)

    def test_edge_index_multiple_addition(self):
        self.graph.add_edge_index('foo')
        exception_raised = False
        try:
            self.graph.add_edge_index('foo')
        except GrapheekIndexAlreadyExistsException:
            exception_raised = True
        assert(exception_raised)

    # Test edge addition by node ids :

    def test_add_edge_by_node_ids(self):
        data = dict(foo='test_add_edge_by_node_ids')
        edge = self.graph.add_edge_by_ids(self.n1.get_id(), self.n2.get_id(), **data)
        # check it had the same effect as usual add_edge :
        assert(self.n1 in edge.inV())
        assert(self.n2 in edge.outV())
        assert(data == edge.data())

    # Testing bulk_add_edge_by_id

    def test_bulk_add_edge_by_ids(self):
        # Just checking that no exception raised and that edge count increased :
        count_before = self.graph.E().count()
        self.graph.bulk_add_edge_by_ids([(self.n3.get_id(), self.n1.get_id(), {}), (self.n2.get_id(), self.n2.get_id(), {})])
        count_after = self.graph.E().count()
        assert(count_after == count_before + 2)

    # Test update_data method :

    def test_data_write_existing_field_using_update_data(self):
        # update_data is useful for client
        from grapheekdb.backends.data.keys import KIND_VERTEX
        self.graph.update_data(KIND_VERTEX, self.n1.get_id(), 'foo', 10)


class TestLocalMemoryGraph(TLocalMemoryGraph):
    pass


class TAddingIndexesHasNoEffectsOnResultsLocalMem(TLocalMemoryGraph):

    def setup(self):
        from grapheekdb.backends.data.localmem import LocalMemoryGraph
        self.graph = LocalMemoryGraph()
        self.graph.add_node_index('foo')
        self.fill()

    def test_adding_a_node_index_dont_mess_lookup(self):
        # Disabling this test : Index already exists
        pass

    def test_node_index_multiple_addition(self):
        # Disabling this test
        pass

    def test_edge_index_multiple_addition(self):
        # Disabling this test
        pass

    def test_index_usage_on_iterator(self):
        # Disabling this test
        pass

    def test_unexisting_index_removal(self):
        # Disabling this test
        pass

    def test_edge_index_removal(self):
        # Disabling this test
        pass

    def test_node_index_removal(self):
        # Disabling this test
        pass


class TestEntityRemoval(object):

    def setup(self):
        from grapheekdb.backends.data.localmem import LocalMemoryGraph
        self.graph = LocalMemoryGraph()

    def test_add_remove_one_free_node_will_reinitialize_raw_data(self):
        from grapheekdb.backends.data.keys import METADATA_VERTEX_ID_LIST_PREFIX
        from grapheekdb.backends.data.keys import METADATA_VERTEX_REMOVED_COUNTER
        raw_data_1 = deepcopy(self.graph._dic)
        node = self.graph.add_node(foo=1, bar=2)
        raw_data_2 = deepcopy(self.graph._dic)
        assert(len(raw_data_1) < len(raw_data_2))
        node.remove()
        raw_data_3 = deepcopy(self.graph._dic)
        vertex_counter_key = 'm/v/c'
        vertex_counter_1 = raw_data_1[vertex_counter_key]
        vertex_counter_3 = raw_data_3[vertex_counter_key]
        # The counter should have increased but other keys shouldn't have changed :
        assert(vertex_counter_3 == vertex_counter_1 + 1)
        del raw_data_1[vertex_counter_key]
        del raw_data_3[vertex_counter_key]
        # entity ids list should be created
        keyv = METADATA_VERTEX_ID_LIST_PREFIX + '/0'
        assert(not(keyv in raw_data_1))
        assert(keyv in raw_data_3)
        del raw_data_3[keyv]
        # entity removed counter should have have been incremented :
        assert(raw_data_3[METADATA_VERTEX_REMOVED_COUNTER] == raw_data_1[METADATA_VERTEX_REMOVED_COUNTER] + 1)
        del raw_data_1[METADATA_VERTEX_REMOVED_COUNTER]
        del raw_data_3[METADATA_VERTEX_REMOVED_COUNTER]
        # but other keys shouldn't have changed :
        assert(raw_data_1 == raw_data_3)

    def test_add_remove_multiple_free_node_will_reinitialize_raw_data(self):
        from grapheekdb.backends.data.keys import METADATA_VERTEX_ID_LIST_PREFIX
        from grapheekdb.backends.data.keys import METADATA_VERTEX_REMOVED_COUNTER
        raw_data_1 = deepcopy(self.graph._dic)
        count = 10
        nodes = []
        for i in range(count):
            node = self.graph.add_node(foo=i, bar=i + 1)
            nodes.append(node)
        raw_data_2 = deepcopy(self.graph._dic)
        assert(len(raw_data_1) < len(raw_data_2))
        for node in nodes:
            node.remove()
        raw_data_3 = deepcopy(self.graph._dic)
        vertex_counter_1 = raw_data_1[METADATA_VERTEX_COUNTER]
        vertex_counter_3 = raw_data_3[METADATA_VERTEX_COUNTER]
        # The counter should have increased but other keys shouldn't have changed :
        assert(vertex_counter_3 == vertex_counter_1 + count)
        del raw_data_1[METADATA_VERTEX_COUNTER]
        del raw_data_3[METADATA_VERTEX_COUNTER]
        # entity ids list should be created
        keyv = METADATA_VERTEX_ID_LIST_PREFIX + '/0'
        assert(not(keyv in raw_data_1))
        assert(keyv in raw_data_3)
        del raw_data_3[keyv]
        # entity removed counter should have have been incremented :
        assert(raw_data_3[METADATA_VERTEX_REMOVED_COUNTER] == raw_data_1[METADATA_VERTEX_REMOVED_COUNTER] + count)
        del raw_data_1[METADATA_VERTEX_REMOVED_COUNTER]
        del raw_data_3[METADATA_VERTEX_REMOVED_COUNTER]
        # but other keys shouldn't have changed :
        assert(raw_data_1 == raw_data_3)

    def test_add_remove_edge(self):
        from grapheekdb.backends.data.keys import METADATA_EDGE_ID_LIST_PREFIX, METADATA_VERTEX_ID_LIST_PREFIX, METADATA_EDGE_REMOVED_COUNTER
        from copy import deepcopy
        node1 = self.graph.add_node(foo=1, bar=2)
        node2 = self.graph.add_node(foo=2, bar=3)
        raw_data_1 = deepcopy(self.graph._dic)
        edge1 = self.graph.add_edge(node1, node2, baz=4)
        raw_data_2 = deepcopy(self.graph._dic)
        assert(len(raw_data_1) < len(raw_data_2))
        edge1.remove()
        raw_data_3 = deepcopy(self.graph._dic)
        edge_counter_1 = raw_data_1[METADATA_EDGE_COUNTER]
        edge_counter_3 = raw_data_3[METADATA_EDGE_COUNTER]
        # The counter should have increased but other keys shouldn't have changed :
        assert(edge_counter_3 == edge_counter_1 + 1)
        del raw_data_1[METADATA_EDGE_COUNTER]
        del raw_data_3[METADATA_EDGE_COUNTER]
        # entity ids list should be created
        keyv = METADATA_VERTEX_ID_LIST_PREFIX + '/0'
        keye = METADATA_EDGE_ID_LIST_PREFIX + '/0'
        assert(keyv in raw_data_1)
        assert(keyv in raw_data_3)
        assert(not(keye in raw_data_1))
        assert(keye in raw_data_3)
        del raw_data_3[keye]
        # entity removed counter should have have been incremented :
        assert(raw_data_3[METADATA_EDGE_REMOVED_COUNTER] == raw_data_1[METADATA_EDGE_REMOVED_COUNTER] + 1)
        del raw_data_1[METADATA_EDGE_REMOVED_COUNTER]
        del raw_data_3[METADATA_EDGE_REMOVED_COUNTER]
        # but other keys shouldn't have changed :
        assert(raw_data_1 == raw_data_3)

    def test_add_remove_multiple_edges(self):
        from grapheekdb.backends.data.keys import METADATA_EDGE_COUNTER, METADATA_EDGE_REMOVED_COUNTER
        from grapheekdb.backends.data.keys import METADATA_EDGE_ID_LIST_PREFIX, METADATA_VERTEX_ID_LIST_PREFIX
        from copy import deepcopy
        node1 = self.graph.add_node(foo=1, bar=2)
        node2 = self.graph.add_node(foo=2, bar=3)
        node3 = self.graph.add_node(foo=3, bar=4)
        raw_data_1 = deepcopy(self.graph._dic)
        edge1 = self.graph.add_edge(node1, node2, baz=4)
        edge2 = self.graph.add_edge(node2, node3, baz=5)
        raw_data_2 = deepcopy(self.graph._dic)
        assert(len(raw_data_1) < len(raw_data_2))
        edge1.remove()
        edge2.remove()
        raw_data_3 = deepcopy(self.graph._dic)
        edge_counter_1 = raw_data_1[METADATA_EDGE_COUNTER]
        edge_counter_3 = raw_data_3[METADATA_EDGE_COUNTER]
        # The counter should have increased but other keys shouldn't have changed :
        assert(edge_counter_3 == edge_counter_1 + 2)
        del raw_data_1[METADATA_EDGE_COUNTER]
        del raw_data_3[METADATA_EDGE_COUNTER]
        # entity ids list should be created
        keyv = METADATA_VERTEX_ID_LIST_PREFIX + '/0'
        keye = METADATA_EDGE_ID_LIST_PREFIX + '/0'
        assert(keyv in raw_data_1)
        assert(keyv in raw_data_3)
        assert(not(keye in raw_data_1))
        assert(keye in raw_data_3)
        del raw_data_3[keye]
        # entity removed counter should have have been incremented :
        assert(raw_data_3[METADATA_EDGE_REMOVED_COUNTER] == raw_data_1[METADATA_EDGE_REMOVED_COUNTER] + 2)
        del raw_data_1[METADATA_EDGE_REMOVED_COUNTER]
        del raw_data_3[METADATA_EDGE_REMOVED_COUNTER]
        # but other keys shouldn't have changed :
        assert(raw_data_1 == raw_data_3)

    def test_removing_node_removes_its_edges_too(self):
        from grapheekdb.backends.data.keys import METADATA_EDGE_COUNTER, METADATA_VERTEX_COUNTER
        from grapheekdb.backends.data.keys import METADATA_EDGE_REMOVED_COUNTER, METADATA_VERTEX_REMOVED_COUNTER

        from grapheekdb.backends.data.keys import METADATA_EDGE_ID_LIST_PREFIX, METADATA_VERTEX_ID_LIST_PREFIX
        from copy import deepcopy
        # I will create a "star" : one central node linked to 3 other nodes
        # Then remove the central and check that all edges have been removed properly
        node1 = self.graph.add_node(foo=1, bar=2)
        node2 = self.graph.add_node(foo=2, bar=3)
        node3 = self.graph.add_node(foo=3, bar=4)
        raw_data_1 = deepcopy(self.graph._dic)
        vertex_counter_1 = raw_data_1[METADATA_VERTEX_COUNTER]
        edge_counter_1 = raw_data_1[METADATA_EDGE_COUNTER]
        center = self.graph.add_node(foo=0, bar=1)
        self.graph.add_edge(center, node1, idx=1)
        self.graph.add_edge(center, node2, idx=2)
        self.graph.add_edge(node3, center, idx=3)  # this edge has a different direction
        # Now removing center
        center.remove()
        raw_data_2 = deepcopy(self.graph._dic)
        vertex_counter_2 = raw_data_2[METADATA_VERTEX_COUNTER]
        edge_counter_2 = raw_data_2[METADATA_EDGE_COUNTER]
        # node and edge counters should have increased
        assert(edge_counter_2 > edge_counter_1)
        assert(vertex_counter_2 > vertex_counter_1)
        del raw_data_1[METADATA_VERTEX_COUNTER]
        del raw_data_2[METADATA_VERTEX_COUNTER]
        del raw_data_1[METADATA_EDGE_COUNTER]
        del raw_data_2[METADATA_EDGE_COUNTER]
        # entity ids list should be created
        keyv = METADATA_VERTEX_ID_LIST_PREFIX + '/0'
        keye = METADATA_EDGE_ID_LIST_PREFIX + '/0'
        assert(keyv in raw_data_1)
        assert(keyv in raw_data_2)
        assert(not(keye in raw_data_1))
        assert(keye in raw_data_2)
        del raw_data_2[keye]
        # entity removed counter should have have been incremented :
        assert(raw_data_2[METADATA_VERTEX_REMOVED_COUNTER] == raw_data_1[METADATA_VERTEX_REMOVED_COUNTER] + 1)
        del raw_data_1[METADATA_VERTEX_REMOVED_COUNTER]
        del raw_data_2[METADATA_VERTEX_REMOVED_COUNTER]
        assert(raw_data_2[METADATA_EDGE_REMOVED_COUNTER] == raw_data_1[METADATA_EDGE_REMOVED_COUNTER] + 3)
        del raw_data_1[METADATA_EDGE_REMOVED_COUNTER]
        del raw_data_2[METADATA_EDGE_REMOVED_COUNTER]
        # but other keys shouldn't have changed :
        assert(raw_data_1 == raw_data_2)


class TestAddingIndexesHasNoEffectsOnResultsLocalMem(TAddingIndexesHasNoEffectsOnResultsLocalMem):
    pass


class TestIncompleteBackend1(object):

    def setup(self):
        self.graph = BaseGraph()

    def teardown(self):
        pass

    def __check_method_is_not_implemented(self, name, *args):
        method = getattr(self.graph, name)
        exception_raised = False
        try:
            method(*args)
        except NotImplementedError:
            exception_raised = True
        assert(exception_raised)

    def test_db_close_is_not_implemented(self):
        self.__check_method_is_not_implemented('_db_close')

    def test_transaction_begin_is_not_implemented(self):
        self.__check_method_is_not_implemented('_transaction_begin')

    def test_transaction_commit_is_not_implemented(self):
        self.__check_method_is_not_implemented('_transaction_commit', None)

    def test_transaction_rollback_is_not_implemented(self):
        self.__check_method_is_not_implemented('_transaction_rollback', None)

    def test_has_key_is_not_implemented(self):
        self.__check_method_is_not_implemented('_has_key', 'foo')

    def test_get_is_not_implemented(self):
        self.__check_method_is_not_implemented('_get', None, 'foo')

    def test_bulk_get_is_not_implemented(self):
        self.__check_method_is_not_implemented('_bulk_get', None, 'foo')

    def test_set_is_not_implemented(self):
        self.__check_method_is_not_implemented('_set', 'foo', 'bar')

    def test_bulk_set_is_not_implemented(self):
        self.__check_method_is_not_implemented('_bulk_set', None, 'foo')

    def test_remove_is_not_implemented(self):
        self.__check_method_is_not_implemented('_remove', 'foo')

    def test_bulk_remove_is_not_implemented(self):
        self.__check_method_is_not_implemented('_bulk_remove', None, ['foo'])

    def test_remove_prefix_is_not_implemented(self):
        self.__check_method_is_not_implemented('_remove_prefix', '[0-9]+')


class TestIncompleteBackend2(object):
    """Mainly done for test coverage"""

    def setup(self):
        from grapheekdb.backends.data.localmem import LocalMemoryGraph

        class UnpreparableGraph(LocalMemoryGraph):
            def _prepare_database(self):
                raise NotImplementedError
        self.GraphClass = UnpreparableGraph

    def test_preparation_faild(self):
        exception_raised = False
        try:
            self.graph = self.GraphClass()
        except GrapheekDataPreparationFailedException:
            exception_raised = True
        assert(exception_raised)


class TestModifiedBackendLeadToError(FillMethod):

    def setup(self):
        from grapheekdb.backends.data.localmem import LocalMemoryGraph
        self.graph = LocalMemoryGraph()
        self.fill()

    def test_create_index_node(self):
        txn = self.graph._transaction_begin()
        try:
            # Manually modifying backend
            self.graph._remove(txn, METADATA_VERTEX_INDEX_COUNTER)
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
            # Manually modifying backend
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
        txn = self.graph._transaction_begin()
        try:
            # Manually modifying backend
            self.graph._remove(txn, METADATA_EDGE_INDEX_COUNTER)
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
        # Manually modifying backend
        exception_raised = False
        try:
            self.graph.remove_edge_index('foo')
        except GrapheekIndexRemovalFailedException:
            exception_raised = True
        assert(exception_raised)

    def test_add_node(self):
        txn = self.graph._transaction_begin()
        try:
            # Manually modifying backend
            self.graph._remove(txn, METADATA_VERTEX_COUNTER)
            self.graph._transaction_commit(txn)
        except:
            self.graph._transaction_rollback(txn)
        # Manually modifying backend
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
            # Manually modifying backend
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
            # Manually modifying backend
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
            # Manually modifying backend
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
            # Manually modifying backend
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
            # Manually modifying backend
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

    # Test invalid data for bulk add node

    def test_invalid_bulk_add_node_1(self):
        count = 2
        data = []
        for i in range(count):
            data.append(('document_id', i))  # this should be a dictionnary not a 2-uple
        exception_raised = False
        try:
            self.graph.bulk_add_node(data)
        except GrapheekDataException:
            exception_raised = True
        assert(exception_raised)

    def test_invalid_bulk_add_node_2(self):
        count = 2
        data = []
        for i in range(count):
            data.append(dict(document_id=i))
        txn = self.graph._transaction_begin()
        try:
            # Manually modifying backend
            self.graph._remove(txn, METADATA_VERTEX_COUNTER)  # This is a hack
            self.graph._transaction_commit(txn)
        except:
            self.graph._transaction_rollback(txn)
        exception_raised = False
        try:
            self.graph.bulk_add_node(data)
        except GrapheekDataException:
            exception_raised = True
        assert(exception_raised)

    # Test rollbacked update

    def test_node_iterator_invalid_update(self):
        exception_raised = False
        try:
            # hack : direct modification of backend data :
            self.graph._dic = {}
            self.graph.V().update(updated=True)
        except GrapheekDataException:
            exception_raised = True
        assert(exception_raised)

    """
    # Test get index and hacking backend

    def test_node_index_retrieval_with_hacked_data(self):
        exception_raised = False
        try:
            # hack : direct modification of backend data :
            self.graph._dic = {}
            self.graph.get_node_indexes()
        except GrapheekDataException:
            exception_raised = True
        assert(exception_raised)

    def test_edge_index_retrieval_with_hacked_data(self):
        exception_raised = False
        try:
            # hack : direct modification of backend data :
            self.graph._dic = {}
            self.graph.get_edge_indexes()
        except GrapheekDataException:
            exception_raised = True
        assert(exception_raised)
    """


class TestServerSideScript(object):

    def setup(self):
        from grapheekdb.backends.data.localmem import LocalMemoryGraph
        self.graph = LocalMemoryGraph()
        # setup server scripts
        self.graph.setup_server_scripts()
        self.n1 = self.graph.add_node(nid=1)
        self.n2 = self.graph.add_node(nid=2)
        self.n3 = self.graph.add_node(nid=3)
        self.graph.add_edge(self.n1, self.n2)
        self.graph.add_edge(self.n2, self.n3)

    def test_server_script_add_edge(self):
        edge_count_before = self.graph.E().count()
        self.graph.V(nid=1).aka('x').outV().outV().aka('y').call('add_edge', 'x', 'y', foo=1)
        edge_count_after = self.graph.E().count()
        assert(edge_count_after == edge_count_before + 1)

    def test_server_check_call_is_silent(self):
        res = self.graph.V(nid=1).aka('x').outV().outV().aka('y').call('add_edge', 'x', 'y', foo=1)
        assert(res is None)

    def test_server_check_request_is_verbose(self):
        self.graph.setup_server_scripts('grapheekdb.server.dummy_scripts')
        res = self.graph.V(nid=1).aka('x').outV().outV().aka('y').request('echo', msg="hello")
        assert(isinstance(res, list))

    def test_server_script_unknown_function(self):
        exception_raised = False
        try:
            self.graph.V(nid=1).aka('x').outV().outV().aka('y').call('this_function_doesnt_exist', 'x', 'y', foo=1)
        except GrapheekUnknownScriptException:
            exception_raised = True
        assert(exception_raised)

    def test_user_custom_script(self):
        # "Install" custom script
        self.graph.setup_server_scripts('grapheekdb.server.dummy_scripts')
        tmp = dict(foo=1, bar=2, baz=3)
        result = self.graph.V(nid=1).aka('x').outV().outV().aka('y').request('echo', 'x', 'y', **tmp)
        assert(len(result))
        res0 = result[0]
        assert(res0['args'] == ('x', 'y'))
        assert(res0['kwargs'] == tmp)

    # Test dot generation - representation

    def test_dot_generation_basic(self):
        str(self.graph.V().dot())

    def test_dot_generation_with_filter(self):
        str(self.graph.V(foo=1).dot())

    def test_dot_generation_boolean_label_1(self):
        # Adding a node with a boolean property
        self.graph.add_node(visible=True)
        # Get the representation
        str(self.graph.V().dot('visible'))

    def test_dot_generation_boolean_label_2(self):
        # Adding a node with a boolean property
        self.graph.add_node(visible=True)
        # Get the representation, check that using node_label works
        str(self.graph.V().dot(node_label='visible'))

    def test_dot_generation_edge_boolean_label(self):
        # Adding a node with a boolean property
        self.graph.add_edge(self.n2, self.n1, baz=True)
        # Get the representation, check that using node_label works
        str(self.graph.V().dot(edge_label='baz'))

    def test_dot_with_limit(self):
        str(self.graph.V(foo=1).dot(limit=1))

    # test name conflicts

    def test_frequenty_used_variable_name_dont_mess_filter(self):
        self.graph.add_node(kind='foo')
        self.graph.V(kind='foo').count()

        self.graph.add_node(entity_id='foo')
        self.graph.V(entity_id='foo').count()

        self.graph.add_node(txn='foo')
        self.graph.V(txn='foo').count()

        self.graph.add_node(fname='foo')
        self.graph.V(fname='foo').count()


class TestIndexAndBulk(object):

    def setup(self):
        from grapheekdb.backends.data.localmem import LocalMemoryGraph
        self.graph = LocalMemoryGraph()

    def test_bulk_added_nodes_after_index_creation_are_also_indexed(self):
        self.graph.add_node_index('foo')
        node_defns = []
        for i in range(1000):
            node_defns.append(dict(foo=i))
        self.graph.bulk_add_node(node_defns)
        # a bit hackish : get node index instance :
        index = self.graph._node_indexes[0]
        assert(index.estimate(None, dict(foo=42)) == 1)

    def test_bulk_added_edges_after_index_creation_are_also_indexed(self):
        self.graph.add_edge_index('foo')
        n1 = self.graph.add_node()
        n2 = self.graph.add_node()
        n3 = self.graph.add_node()
        n4 = self.graph.add_node()
        edge_defns = []
        edge_defns.append((n1, n2, dict(foo=1)))
        edge_defns.append((n2, n3, dict(foo=2)))
        edge_defns.append((n3, n4, dict(foo=3)))
        self.graph.bulk_add_edge(edge_defns)
        # a bit hackish : get edge index instance :
        index = self.graph._edge_indexes[0]
        assert(index.estimate(None, dict(foo=1)) == 1)
