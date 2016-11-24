# -*- coding:utf-8 -*-

from grapheekdb.lib.exceptions import GrapheekDataException
from grapheekdb.lib.exceptions import GrapheekInvalidDataTypeException
from grapheekdb.lib.exceptions import GrapheekUnknownAlias
from grapheekdb.lib.exceptions import GrapheekException
from grapheekdb.lib.exceptions import GrapheekInvalidExpression
from grapheekdb.lib.exceptions import GrapheekUnknownMethod
from grapheekdb.lib.exceptions import GrapheekMixedKindException


class FillMethod(object):

    def fill(self):
        self.ndata1 = dict(foo=1, bar=2, name='Raf', baz='for isnull test :)')
        self.n1 = self.graph.add_node(**self.ndata1)
        self.ndata2 = dict(foo=1, bar=3, name='Flo')
        self.n2 = self.graph.add_node(**self.ndata2)
        self.ndata3 = dict(foo=2, bar=3, name='Theo')
        self.n3 = self.graph.add_node(**self.ndata3)
        self.edata1 = dict(label='knows', since=1991, where='paris', foo='AbcD', baz='for isnull test :)', common=1)
        self.e1 = self.graph.add_edge(self.n1, self.n2, **self.edata1)
        self.edata2 = dict(label='is_parent', since=1996, where='Paris', foo='bcde', common=1)
        self.e2 = self.graph.add_edge(self.n2, self.n3, **self.edata2)


class CommonMethods(object):

    # Basic traversal tests starting from a node :

    def test_node_outV(self):
        assert(self.n2 in self.n1.outV())

    def test_node_inV(self):
        assert(self.n1 in self.n2.inV())

    def test_node_bothV(self):
        assert(self.n1 in self.n2.bothV())
        assert(self.n2 in self.n1.bothV())

    def test_node_outE(self):
        assert(self.e1 in self.n1.outE())

    def test_node_inE(self):
        assert(self.e1 in self.n2.inE())

    def test_node_bothE(self):
        assert(self.e1 in self.n1.bothE())
        assert(self.e1 in self.n2.bothE())

    # Basic traversal tests starting from an edge :

    def test_edge_outV(self):
        assert(self.n2 in self.e1.outV())

    def test_edge_inV(self):
        assert(self.n1 in self.e1.inV())

    def test_edge_bothV(self):
        assert(self.n1 in self.e1.bothV())
        assert(self.n2 in self.e1.bothV())

    # test data access

    def test_data_read_existing_field(self):
        self.n1.foo

    def test_data_read_missing_field(self):
        exception_raised = False
        try:
            self.n1.xxx
        except AttributeError:
            exception_raised = True
        assert(exception_raised)

    def test_data_write_existing_field(self):
        self.n1.foo = 10

    def test_data_write_missing_field(self):
        # Note : contrary to read, this must not fail
        # (It will add the attribute on the fly)
        self.n1.xxx = 10

    def test_data_read_field_using_get(self):
        assert(self.n1.get('foo') == 1)

    def test_data_read_missing_field_using_get_and_default(self):
        assert(self.n1.get('oof', 'sth else') == 'sth else')

    # More complex traversal tests :

    #    ov + sth

    def test_node_ovov(self):
        assert(self.n3 in self.n1.outV().outV())

    def test_node_oviv(self):
        assert(self.n1 in self.n1.outV().inV())

    def test_node_ovbv(self):
        assert(self.n1 in self.n1.outV().bothV())

    def test_node_ovoe(self):
        assert(self.e2 in self.n1.outV().outE())

    def test_node_ovie(self):
        assert(self.e1 in self.n1.outV().inE())

    def test_node_ovbe(self):
        assert(self.e2 in self.n1.outV().bothE())

    #    iv + sth

    def test_node_ivov(self):
        assert(self.n2 in self.n2.inV().outV())

    def test_node_iviv(self):
        assert(self.n1 in self.n3.inV().inV())

    def test_node_ivbv(self):
        assert(self.n1 in self.n3.inV().bothV())

    def test_node_ivoe(self):
        assert(self.e1 in self.n2.inV().outE())

    def test_node_ivie(self):
        assert(self.e1 in self.n3.inV().inE())

    def test_node_ivbe(self):
        assert(self.e1 in self.n3.inV().bothE())

    #    bv + sth

    def test_node_bvov(self):
        assert(self.n3 in self.n1.bothV().outV())

    def test_node_bviv(self):
        assert(self.n1 in self.n1.bothV().inV())

    def test_node_bvbv(self):
        assert(self.n1 in self.n1.bothV().bothV())

    def test_node_bvoe(self):
        assert(self.e2 in self.n1.bothV().outE())

    def test_node_bvie(self):
        assert(self.e1 in self.n1.bothV().inE())

    def test_node_bvbe(self):
        assert(self.e2 in self.n1.bothV().bothE())

    #    oe + sth

    def test_node_oeov(self):
        assert(self.n2 in self.n1.outE().outV())

    def test_node_oeiv(self):
        assert(self.n1 in self.n1.outE().inV())

    #    ie + sth

    def test_node_ieov(self):
        assert(self.n2 in self.n2.inE().outV())

    def test_node_ieiv(self):
        assert(self.n1 in self.n2.inE().inV())

    #    be + sth

    def test_node_beov(self):
        assert(self.n2 in self.n2.bothE().outV())

    def test_node_beiv(self):
        assert(self.n1 in self.n2.bothE().inV())

    # Same for edges :

    #    ov + sth

    def test_edge_ovov(self):
        assert(self.n3 in self.e1.outV().outV())

    def test_edge_oviv(self):
        assert(self.n1 in self.e1.outV().inV())

    def test_edge_ovoe(self):
        assert(self.e2 in self.e1.outV().outE())

    def test_edge_ovie(self):
        assert(self.e1 in self.e1.outV().inE())

    #    iv + sth

    def test_edge_ivov(self):
        assert(self.n2 in self.e1.inV().outV())

    def test_edge_iviv(self):
        assert(self.n1 in self.e2.inV().inV())

    def test_edge_ivoe(self):
        assert(self.e1 in self.e1.inV().outE())

    def test_edge_ivie(self):
        assert(self.e1 in self.e2.inV().inE())

    #    bv + sth

    def test_edge_bvov(self):
        assert(self.n2 in self.e1.bothV().outV())

    def test_edge_bviv(self):
        assert(self.n1 in self.e2.bothV().inV())

    def test_edge_bvoe(self):
        assert(self.e1 in self.e1.bothV().outE())

    def test_edge_bvie(self):
        assert(self.e1 in self.e2.bothV().inE())

    # Node and Edge data tests :

    def test_node_handles_data(self):
        assert(self.n1.data() == self.ndata1)

    def test_node_handles_data_with_args(self):
        assert(self.n1.data('foo') == {'foo': self.ndata1['foo']})

    def test_node_handles_data_with_args_and_flat(self):
        assert(self.n1.data('foo', flat=True) == [self.ndata1['foo']])

    def test_node_clone_handles_data(self):
        assert(self.graph.v(self.n1.get_id()).data() == self.ndata1)

    def test_edge_handles_data(self):
        assert(self.e1.data() == self.edata1)

    def test_edge_clone_handles_data(self):
        assert(self.graph.e(self.e1.get_id()).data() == self.edata1)

    def test_node_accessors(self):
        for k, v in list(self.ndata1.items()):
            assert(getattr(self.n1, k) == v)

    def test_clone_node_accessors(self):
        for k, v in list(self.ndata1.items()):
            assert(getattr(self.graph.v(self.n1.get_id()), k) == v)

    def test_edge_accessors(self):
        for k, v in list(self.edata1.items()):
            assert(getattr(self.e1, k) == v)

    def test_clone_edge_accessors(self):
        for k, v in list(self.edata1.items()):
            assert(getattr(self.graph.e(self.e1.get_id()), k) == v)

    # Testing traversal shortcuts (IN/OUT/BOTH) :
    # First starting from iterator :

    def test_node_iterator_out_1(self):
        assert(list(self.graph.V(name='Raf').out_(since=1991)) == [self.n2])

    def test_node_iterator_out_2(self):
        assert(list(self.graph.V(name='Raf').out_(since=1996)) == [])

    def test_node_iterator_out_3(self):
        assert(list(self.graph.V(name='Raf').out_(2)) == [self.n3])

    def test_node_iterator_in_1(self):
        assert(list(self.graph.V(name='Theo').in_(since=1996)) == [self.n2])

    def test_node_iterator_in_2(self):
        assert(list(self.graph.V(name='Theo').in_(since=1991)) == [])

    def test_node_iterator_in_3(self):
        assert(list(self.graph.V(name='Theo').in_(2)) == [self.n1])

    def test_node_iterator_both_1(self):
        assert(list(self.graph.V(name='Raf').both_(since=1991)) == [self.n2])

    def test_node_iterator_both_2(self):
        assert(list(self.graph.V(name='Theo').both_(since=1996)) == [self.n2])

    def test_node_iterator_both_3(self):
        assert(list(self.graph.V(name='Raf').both_(2, since=1991)) == [self.n1])

    def test_node_iterator_both_4(self):
        assert(list(self.graph.V(name='Theo').both_(2, since=1996)) == [self.n3])

    def test_node_iterator_both_5(self):
        res = list(self.graph.V(name='Theo').both_(2))
        ref = [self.n1, self.n3]
        assert(len(res) == len(ref))
        for node in ref:
            assert(node in res)

    def test_node_iterator_both_6(self):
        res = list(self.graph.V(name='Theo').both_(2, since__gt=1990))
        ref = [self.n1, self.n3]
        assert(len(res) == len(ref))
        for node in ref:
            assert(node in res)

    def test_node_iterator_both_7(self):
        res = list(self.graph.V(name='Theo').both_(2, since__gt=2000))
        assert(len(res) == 0)

    # Then, directly from node :

    def test_node_out_1(self):
        assert(list(self.n1.out_(since=1991)) == [self.n2])

    def test_node_out_2(self):
        assert(list(self.n1.out_(since=1996)) == [])

    def test_node_out_3(self):
        assert(list(self.n1.out_(2)) == [self.n3])

    def test_node_in_1(self):
        assert(list(self.n3.in_(since=1996)) == [self.n2])

    def test_node_in_2(self):
        assert(list(self.n3.in_(since=1991)) == [])

    def test_node_in_3(self):
        assert(list(self.n3.in_(2)) == [self.n1])

    def test_node_both_1(self):
        assert(list(self.n1.both_(since=1991)) == [self.n2])

    def test_node_both_2(self):
        assert(list(self.n3.both_(since=1996)) == [self.n2])

    def test_node_both_3(self):
        assert(list(self.n1.both_(2, since=1991)) == [self.n1])

    def test_node_both_4(self):
        assert(list(self.n3.both_(2, since=1996)) == [self.n3])

    def test_node_both_5(self):
        res = list(self.n3.both_(2))
        ref = [self.n1, self.n3]
        assert(len(res) == len(ref))
        for node in ref:
            assert(node in res)

    def test_node_both_6(self):
        res = list(self.n3.both_(2, since__gt=1990))
        ref = [self.n1, self.n3]
        assert(len(res) == len(ref))
        for node in ref:
            assert(node in res)

    def test_node_both_7(self):
        res = list(self.n3.both_(2, since__gt=2000))
        assert(len(res) == 0)

    # Node comparison checking

    def test_same_data_doesnt_mean_same_node(self):
        n3 = self.graph.add_node(foo=2, bar=3)
        assert(n3 != self.n2)

    def test_node_and_node_by_id_should_appear_as_equal(self):
        node_id = self.n1.get_id()
        assert(self.graph.v(node_id) == self.n1)

    def test_node_by_id_must_have_the_same_outV_as_original(self):
        node_id = self.n1.get_id()
        outv_original_ids = [item.get_id() for item in self.n1.outV()]
        outv_clone_ids = [item.get_id() for item in self.graph.v(node_id).outV()]
        assert(set(outv_original_ids) == set(outv_clone_ids))

    def test_node_by_id_must_have_the_same_inV_as_original(self):
        node_id = self.n2.get_id()
        inv_original_ids = [item.get_id() for item in self.n2.inV()]
        inv_clone_ids = [item.get_id() for item in self.graph.v(node_id).inV()]
        assert(set(inv_original_ids) == set(inv_clone_ids))

    def test_edge_and_edge_by_id_should_appear_as_equal(self):
        edge_id = self.e1.get_id()
        assert(self.graph.e(edge_id) == self.e1)

    def test_edge_by_id_must_have_the_same_outV_and_inV_as_original(self):
        edge_id = self.e1.get_id()
        assert(self.graph.e(edge_id) == self.e1)

    # test limit feature :

    def test_limit_feature_0(self):
        assert(self.n1.outV().limit(0).count() == 0)

    def test_limit_feature_1(self):
        assert(self.n1.outV().limit(1).count() == 1)

    # test filters feature :

    def test_filter_node_exact_1(self):
        assert(self.graph.V(foo=1).count() == 2)

    def test_filter_node_exact_2(self):
        assert(self.graph.V(bar=2).count() == 1)

    def test_filter_node_exact_3(self):
        assert(self.graph.V(name='Raf').count() == 1)

    def test_filter_node_exact_4(self):
        # exact is case sensitive
        assert(self.graph.V(name='raf').count() == 0)

    def test_filter_node_exact_clause_1(self):
        assert(self.graph.V(foo__exact=1).count() == 2)

    def test_filter_node_exact_clause_2(self):
        assert(self.graph.V(bar__exact=2).count() == 1)

    def test_filter_node_exact_clause_3(self):
        assert(self.graph.V(name__exact='Raf').count() == 1)

    def test_filter_node_exact_clause_4(self):
        # exact is case sensitive
        assert(self.graph.V(name__exact='raf').count() == 0)

    def test_filter_node_iexact_1(self):
        assert(self.graph.V(name__iexact='raf').count() == 1)

    def test_filter_node_iexact_2(self):
        assert(self.graph.V(name__iexact='RAF').count() == 1)

    def test_filter_node_contains_1(self):
        assert(self.graph.V(name__contains='R').count() == 1)

    def test_filter_node_contains_2(self):
        assert(self.graph.V(name__contains='o').count() == 2)

    def test_filter_node_contains_3(self):
        assert(self.graph.V(name__contains='x').count() == 0)

    def test_filter_node_contains_4(self):
        assert(self.graph.V(name__contains='r').count() == 0)

    def test_filter_node_icontains_1(self):
        assert(self.graph.V(name__icontains='r').count() == 1)

    def test_filter_node_in(self):
        assert(self.graph.V(name__in=['Raf', 'Flo']).count() == 2)

    def test_filter_node_gt(self):
        assert(self.graph.V(foo__gt=1).count() == 1)

    def test_filter_node_gte(self):
        assert(self.graph.V(foo__gte=1).count() == 3)

    def test_filter_node_lt(self):
        assert(self.graph.V(bar__lt=3).count() == 1)

    def test_filter_node_lte(self):
        assert(self.graph.V(bar__lte=3).count() == 3)

    def test_filter_node_startswith_1(self):
        assert(self.graph.V(name__startswith='Ra').count() == 1)

    def test_filter_node_startswith_2(self):
        assert(self.graph.V(name__startswith='aa').count() == 0)

    def test_filter_node_istartswith_1(self):
        assert(self.graph.V(name__istartswith='ra').count() == 1)

    def test_filter_node_istartswith(self):
        assert(self.graph.V(name__istartswith='RA').count() == 1)

    def test_filter_node_endswith_1(self):
        assert(self.graph.V(name__endswith='o').count() == 2)

    def test_filter_node_endswith_2(self):
        assert(self.graph.V(name__endswith='O').count() == 0)

    def test_filter_node_iendswith(self):
        assert(self.graph.V(name__iendswith='O').count() == 2)

    def test_filter_node_isnull_1(self):
        assert(self.graph.V(baz__isnull=False).count() == 1)

    def test_filter_node_isnull_2(self):
        assert(self.graph.V(baz__isnull=True).count() == 2)

    # EDGE :

    def test_filter_edge_exact_1(self):
        assert(self.graph.E(since=1991).count() == 1)

    def test_filter_edge_exact_2(self):
        assert(self.graph.E(where='paris').count() == 1)

    def test_filter_edge_exact_3(self):
        assert(self.graph.E(since=1993).count() == 0)

    def test_filter_edge_exact_clause_1(self):
        assert(self.graph.E(since__exact=1991).count() == 1)

    def test_filter_edge_exact_clause_2(self):
        assert(self.graph.E(where__exact='paris').count() == 1)

    def test_filter_edge_exact_clause_3(self):
        assert(self.graph.E(since__exact=1993).count() == 0)

    def test_filter_edge_exact_clause_4(self):
        assert(self.graph.E(where__exact='Paris').count() == 1)

    def test_filter_edge_iexact_1(self):
        assert(self.graph.E(where__iexact='paris').count() == 2)

    def test_filter_edge_iexact_2(self):
        assert(self.graph.E(where__iexact='PaRiS').count() == 2)

    def test_filter_edge_contains_1(self):
        assert(self.graph.E(foo__contains='cde').count() == 1)

    def test_filter_edge_contains_2(self):
        assert(self.graph.E(foo__contains='bc').count() == 2)

    def test_filter_edge_contains_3(self):
        assert(self.graph.E(foo__contains='x').count() == 0)

    def test_filter_edge_contains_4(self):
        assert(self.graph.E(foo__contains='A').count() == 1)

    def test_filter_edge_icontains_1(self):
        assert(self.graph.E(foo__icontains='a').count() == 1)

    def test_filter_edge_icontains_2(self):
        assert(self.graph.E(foo__icontains='bcd').count() == 2)

    def test_filter_edge_in(self):
        assert(self.graph.E(where__in=['paris', 'calais']).count() == 1)

    def test_filter_edge_gt_1(self):
        assert(self.graph.E(since__gt=1990).count() == 2)

    def test_filter_edge_gt_2(self):
        assert(self.graph.E(since__gt=1991).count() == 1)

    def test_filter_edge_gt_3(self):
        assert(self.graph.E(since__gt=1996).count() == 0)

    def test_filter_edge_gt_4(self):
        assert(self.graph.E(since__gt=1997).count() == 0)

    def test_filter_edge_gte_1(self):
        assert(self.graph.E(since__gte=1990).count() == 2)

    def test_filter_edge_gte_2(self):
        assert(self.graph.E(since__gte=1991).count() == 2)

    def test_filter_edge_lt_1(self):
        assert(self.graph.E(since__lt=1990).count() == 0)

    def test_filter_edge_lt_2(self):
        assert(self.graph.E(since__lt=1991).count() == 0)

    def test_filter_edge_lt_3(self):
        assert(self.graph.E(since__lt=1997).count() == 2)

    def test_filter_edge_lte_1(self):
        assert(self.graph.E(since__lte=1990).count() == 0)

    def test_filter_edge_lte_2(self):
        assert(self.graph.E(since__lte=1991).count() == 1)

    def test_filter_edge_lte_3(self):
        assert(self.graph.E(since__lte=1997).count() == 2)

    def test_filter_edge_startswith_1(self):
        assert(self.graph.E(where__startswith='par').count() == 1)

    def test_filter_edge_startswith_2(self):
        assert(self.graph.E(where__startswith='Par').count() == 1)

    def test_filter_edge_startswith_3(self):
        assert(self.graph.E(where__startswith='ar').count() == 0)

    def test_filter_edge_istartswith_1(self):
        assert(self.graph.E(where__istartswith='pa').count() == 2)

    def test_filter_edge_istartswith_2(self):
        assert(self.graph.E(where__istartswith='PA').count() == 2)

    def test_filter_edge_istartswith_3(self):
        assert(self.graph.E(where__istartswith='ar').count() == 0)

    def test_filter_edge_endswith_1(self):
        assert(self.graph.E(where__endswith='is').count() == 2)

    def test_filter_edge_endswith_2(self):
        assert(self.graph.E(where__endswith='ri').count() == 0)

    def test_filter_edge_isnull_1(self):
        assert(self.graph.E(foo__isnull=False).count() == 2)

    def test_filter_edge_isnull_2(self):
        assert(self.graph.E(baz__isnull=True).count() == 1)

    # test other iterator methods :

    def test_it_dedup(self):
        assert(self.n2.bothV().bothV().count() > self.n2.bothV().bothV().dedup().count())
        assert(self.n2.bothV().bothV().dedup().count() == 1)

    def test_it_dedup_on_alias(self):
        assert(list(self.graph.V(name__in=['Raf', 'Theo']).aka('x').bothV().dedup('x')) == [self.n2, self.n2])

    def test_it_dedup_on_bad_alias(self):
        exception_raised = False
        try:
            next(self.graph.V(name__in=['Raf', 'Theo']).aka('x').bothV().dedup('y'))
        except GrapheekUnknownAlias:
            exception_raised = True
        except GrapheekException:
            if self.__class__.__name__ == 'TestClient':
                exception_raised = True
        assert(exception_raised)

    def test_it_data(self):
        assert(self.graph.V(name='Raf').data() == [self.ndata1])

    def test_it_remove(self):
        self.graph.V(foo=1).remove()
        assert(self.graph.V().count() == 1)

    def test_filter_node_invalid_clause_for_a_type(self):
        assert(self.n1.outV(foo__contains="aaa").count() == 0)

    # test basic indexing features

    def test_adding_a_node_index_dont_mess_lookup(self):
        self.graph.add_node_index("foo")
        assert(self.graph.V(foo=1).count() == 2)

    def test_adding_a_edge_index_dont_mess_lookup(self):
        self.graph.add_edge_index("label")
        assert(self.graph.E(label='knows').count() == 1)

    def test_adding_a_node_index_and_edge_index_on_same_value_dont_mess_lookup(self):
        # Inspired from https://bitbucket.org/nidusfr/grapheekdb/issue/7/ine-oute-is-buggy-when-using-indexes-as
        a = self.graph.add_node(n="a")
        b = self.graph.add_node(n="b")
        self.graph.add_edge(a, b, k="1")
        self.graph.add_edge(a, b, k="2")
        self.graph.add_edge(a, b, k="3")
        self.graph.add_edge(a, b, k="4")
        before_count = len(list(b.inE(k="1")))
        self.graph.add_node_index("k")
        self.graph.add_edge_index("k")
        after_count = len(list(b.inE(k="1")))
        assert(before_count == after_count)

    # Test edge index removal

    def test_edge_index_removal(self):
        self.graph.add_edge_index('foo')
        self.graph.remove_edge_index('foo')

    # Test node index removal

    def test_node_index_removal(self):
        self.graph.add_node_index('foo')
        self.graph.remove_node_index('foo')

    # Test index usage on iterator

    def test_index_usage_on_iterator(self):
        self.graph.add_node_index('foo')
        self.graph.V(name='Raf').bothV(foo=1).data()  # ".bothV(foo=1)" is the real test

    # Test partial indexes features :

    def test_partial_indexes_1(self):
        self.graph.add_node_index(foo=1)
        assert(self.graph.V(foo=1).count() == 2)

    def test_partial_indexes_2(self):
        self.graph.add_node_index(foo=1)
        # (This test is a coverage test)
        # Below, what is really tested is that previous indexes is incompetent and shouldn't been used
        assert(self.graph.V(foo=76765765356).count() == 0)

    def test_partial_indexes_3(self):
        self.graph.add_node_index(foo=1)
        # (This test is a coverage test)
        # Below, what is really tested is that new node won't be inserted in the index
        self.graph.add_node(foo=3)

    def test_partial_indexes_4(self):
        for i in range(1000):
            self.graph.add_node(an_id=i)
        self.graph.add_node_index(an_id=500)
        # get previous index :
        index = self.graph._node_indexes[-1]
        # (This test is a coverage test)
        # Checking that for lookup on different fields, this partail index is incompetent
        assert(index.estimate(None, dict(foo=1)) == -1)

    # Test partial indexes removal features :

    def test_partial_indexes_removal(self):
        self.graph.add_node_index(foo=1)
        assert(self.graph.V(foo=1).count() == 2)
        before = len(self.graph.get_node_indexes())
        self.graph.remove_node_index(foo=1)
        after = len(self.graph.get_node_indexes())
        assert(after == before - 1)
        assert(self.graph.V(foo=1).count() == 2)

    # Test entities removal

    def test_node_removal(self):
        node_count_before = self.graph.V().count()
        self.n1.remove()
        node_count_after = self.graph.V().count()
        assert(node_count_after == node_count_before - 1)

    def test_edge_removal(self):
        edge_count_before = self.graph.E().count()
        self.e1.remove()
        edge_count_after = self.graph.E().count()
        assert(edge_count_after == edge_count_before - 1)

    # Test invalid data for bulk add edge

    def test_invalid_bulk_add_edge_1(self):
        exception_raised = False
        try:
            self.graph.bulk_add_edge([(self.n1, self.n2)])  # Missing data
        except GrapheekDataException:
            exception_raised = True
        assert(exception_raised)

    def test_invalid_bulk_add_edge_2(self):
        exception_raised = False
        try:
            self.graph.bulk_add_edge([(self.n1, self.n2, [])])  # Data is not a dict
        except GrapheekDataException:
            exception_raised = True
        assert(exception_raised)

    def test_invalid_bulk_add_edge_3(self):
        exception_raised = False
        try:
            self.graph.bulk_add_edge((self.n1, self.n2, {}))  # only one 3-uple, should be a list of 3-uple
        except GrapheekDataException:
            exception_raised = True
        assert(exception_raised)

    def test_invalid_bulk_add_edge_4(self):
        from grapheekdb.backends.data.base import Node
        exception_raised = False
        try:
            n4 = Node(1000, self.graph)  # instantiate a Node but don't save it
            n5 = Node(1001, self.graph)  # instantiate a Node but don't save it
            # It must not be possible to add an edge between 2 unknown nodess
            self.graph.bulk_add_edge([(n4, n5, {})])
        except GrapheekDataException:
            exception_raised = True
        assert(exception_raised)

    # Test agg method

    def test_iterator_agg_method_1(self):
        self.n4 = self.graph.add_node(data=4)
        self.e3 = self.graph.add_edge(self.n2, self.n4)
        self.e4 = self.graph.add_edge(self.n1, self.n3)
        self.e5 = self.graph.add_edge(self.n3, self.n4)
        assert(list(map(list, self.graph.V(name='Raf').outV().outV().sum())) == [[self.n4, 2], [self.n3, 1]])

    def test_iterator_agg_method_2(self):
        self.n4 = self.graph.add_node(data=4)
        self.e3 = self.graph.add_edge(self.n2, self.n4)
        self.e4 = self.graph.add_edge(self.n1, self.n3)
        self.e5 = self.graph.add_edge(self.n3, self.n4)
        assert(list(map(list, self.graph.V(data=4).inV().inV().sum())) == [[self.n1, 2], [self.n2, 1]])

    def test_iterator_agg_method_3(self):
        assert(list(map(list, self.graph.V(name='Flo').bothV().bothV().sum())) == [[self.n2, 2]])

    def test_iterator_agg_method_4(self):
        # Testing option 'asc'
        self.n4 = self.graph.add_node(data=4)
        self.e3 = self.graph.add_edge(self.n2, self.n4)
        self.e4 = self.graph.add_edge(self.n1, self.n3)
        self.e5 = self.graph.add_edge(self.n3, self.n4)
        assert(list(map(list, self.graph.V(name='Raf').outV().outV().sum(asc=True))) == [[self.n3, 1], [self.n4, 2]])

    def test_iterator_agg_method_5(self):
        # Testing arg '%'
        self.n4 = self.graph.add_node(data=4)
        self.e3 = self.graph.add_edge(self.n2, self.n4)
        self.e4 = self.graph.add_edge(self.n1, self.n3)
        self.e5 = self.graph.add_edge(self.n3, self.n4)
        lst1, lst2 = list(map(list, self.graph.V(name='Raf').outV().outV().percent()))
        node1, prop1 = lst1
        assert(node1 == self.n4)
        assert(int(prop1 * 1000000) == 666666)
        node2, prop2 = lst2
        assert(node2 == self.n3)
        assert(int(prop2 * 1000000) == 333333)

    def test_iterator_agg_method_6(self):
        self.n4 = self.graph.add_node(data=4)
        self.e3 = self.graph.add_edge(self.n2, self.n4)
        self.e4 = self.graph.add_edge(self.n1, self.n3)
        self.e5 = self.graph.add_edge(self.n3, self.n4)
        assert(list(map(list, self.graph.V(data=4).inV().inV().sum().limit(1))) == [[self.n1, 2]])

    def test_iterator_agg_method_7(self):
        self.n4 = self.graph.add_node(data=4)
        self.e3 = self.graph.add_edge(self.n2, self.n4)
        self.e4 = self.graph.add_edge(self.n1, self.n3)
        self.e5 = self.graph.add_edge(self.n3, self.n4)
        assert(list(map(list, self.graph.V(data=4).inV().inV().sum('bar'))) == [[self.n1, 4], [self.n2, 3]])

    def test_iterator_agg_method_8(self):
        self.n4 = self.graph.add_node(data=4)
        self.e3 = self.graph.add_edge(self.n2, self.n4)
        self.e4 = self.graph.add_edge(self.n1, self.n3)
        self.e5 = self.graph.add_edge(self.n3, self.n4)
        assert(list(map(list, self.graph.V(data=4).inV().inV().sum().limit(1).entities().sum())) == [[self.n1, 1]])

    def __iterator_agg_with_reference_to_context_helper(self):
        n4 = self.graph.add_node()
        n5 = self.graph.add_node()
        n6 = self.graph.add_node()
        n7 = self.graph.add_node()
        # "diamond structure"
        self.graph.add_edge(n4, n5, weight=2)
        self.graph.add_edge(n4, n6, weight=3)
        self.graph.add_edge(n5, n7)
        self.graph.add_edge(n6, n7)
        return n4, n5, n6, n7

    def test_iterator_agg_with_reference_to_context_1(self):
        n4, n5, n6, n7 = self.__iterator_agg_with_reference_to_context_helper()
        assert(len(list(n4.outE().aka('x').outV().outV().sum('_.x.weight'))) == 1)
        assert(list(list(n4.outE().aka('x').outV().outV().sum('_.x.weight'))[0]) == [n7, 5.0])

    def test_iterator_agg_with_reference_to_context_2(self):
        n4, n5, n6, n7 = self.__iterator_agg_with_reference_to_context_helper()
        assert(list(list(n4.outE().aka('x').outV().outV().sum('_.x.weight+1'))[0]) == [n7, 7.0])

    def test_iterator_agg_with_reference_to_context_3(self):
        n4, n5, n6, n7 = self.__iterator_agg_with_reference_to_context_helper()
        assert(list(list(n4.outE().aka('x').outV().outV().sum('1 if _.x.weight>0 else 0'))[0]) == [n7, 2.0])

    def test_iterator_agg_with_reference_to_context_forbidden_expression_1(self):
        n4, n5, n6, n7 = self.__iterator_agg_with_reference_to_context_helper()
        exception_raised = False
        try:
            list(n4.outE().aka('x').outV().outV().sum('import string'))  # list(...) force evaluation
        except GrapheekInvalidExpression:
            exception_raised = True
        assert(exception_raised)

    def test_iterator_agg_with_reference_to_context_forbidden_expression_2(self):
        n4, n5, n6, n7 = self.__iterator_agg_with_reference_to_context_helper()
        exception_raised = False
        try:
            list(n4.outE().aka('x').outV().outV().sum('(1).__class__.__bases__[0].__subclasses__'))  # this string is often used to demonstrate python eval security hole
        except GrapheekInvalidExpression:
            exception_raised = True
        assert(exception_raised)

    def test_iterator_agg_with_reference_to_context_forbidden_expression_3(self):
        n4, n5, n6, n7 = self.__iterator_agg_with_reference_to_context_helper()
        exception_raised = False
        try:
            list(n4.outE().aka('x').outV().outV().sum('(1).__class__.__bases__[0].__subclasses__()'))  # this string is often used to demonstrate python eval security hole
        except GrapheekInvalidExpression:
            exception_raised = True
        assert(exception_raised)

    def test_iterator_agg_method_to_entities(self):
        self.n4 = self.graph.add_node(data=4)
        self.e3 = self.graph.add_edge(self.n2, self.n4)
        self.e4 = self.graph.add_edge(self.n1, self.n3)
        self.e5 = self.graph.add_edge(self.n3, self.n4)
        assert(list(self.graph.V(data=4).inV().inV().sum().entities()) == [self.n1, self.n2])

    # test recursive method (starting from entity iterator)

    def test_recursive_iterator_outv(self):
        assert(list(self.graph.V(name='Raf').outV().outV()) == list(self.graph.V(name='Raf').outV(2)))

    def test_recursive_iterator_inv(self):
        assert(list(self.graph.V(name='Raf').inV().inV()) == list(self.graph.V(name='Raf').inV(2)))

    def test_recursive_iterator_bothv(self):
        assert(list(self.graph.V(name='Raf').bothV().bothV()) == list(self.graph.V(name='Raf').bothV(2)))

    # test recursive method (starting from entity)

    def test_recursive_entity_outv(self):
        assert(list(self.n1.outV().outV()) == list(self.n1.outV(2)))

    def test_recursive_entity_inv(self):
        assert(list(self.n1.inV().inV()) == list(self.n1.inV(2)))

    def test_recursive_entity_bothv(self):
        assert(list(self.n1.bothV().bothV())) == list(self.n1.bothV(2))

    # test .all method

    def test_node_all(self):
        assert(len(self.n1.outV().all()) == 1)

    # test aka and collect methods :
    # 1 : starting from an entity
    # 1 - a : entity is a node

    def test_node_aka_ov_ov_collect_1(self):
        assert(self.n1.aka('x').outV().outV().aka('y').collect('x', 'y') == [[self.n1, self.n3]])

    def test_node_aka_ov_ov_collect_2(self):
        # Almost the same as previous but changing order of collect
        assert(self.n1.aka('x').outV().outV().aka('y').collect('y', 'x') == [[self.n3, self.n1]])

    def test_node_aka_ov_ov_collect_3(self):
        assert(self.n1.aka('x').outV().aka('y').outV().aka('z').collect('x', 'y', 'z') == [[self.n1, self.n2, self.n3]])

    def test_node_aka_ov_ov_collect_4(self):
        # Adding node & edge for further testing
        self.n4 = self.graph.add_node()
        self.graph.add_edge(self.n2, self.n4)
        assert(self.n1.aka('x').outV().outV().aka('y').collect('x', 'y') == [[self.n1, self.n3], [self.n1, self.n4]])

    def test_node_aka_ov_bv_collect(self):
        assert(self.n1.aka('x').outV().bothV().aka('y').collect('x', 'y') == [[self.n1, self.n1], [self.n1, self.n3]])

    # 1 - b : entity is an edge

    def test_edge_aka_ov_ov_collect_1(self):
        assert(self.e1.aka('x').outV().outV().aka('y').collect('x', 'y') == [[self.e1, self.n3]])

    def test_edge_aka_ov_ov_collect_2(self):
        assert(self.e1.aka('x').outV().outE().aka('y').collect('x', 'y') == [[self.e1, self.e2]])

    # 2nd : starting from an iterator :

    def test_node_iterator_aka_ov_ov_collect(self):
        assert(self.graph.V(name='Raf').aka('x').outV().outV().aka('y').collect('x', 'y') == [[self.n1, self.n3]])

    def test_edge_iterator_aka_ov_oe_collect(self):
        assert(self.graph.E(since=1991).aka('x').outV().outE().aka('y').collect('x', 'y') == [[self.e1, self.e2]])

    # Test iterator .next :

    def test_node_iterator_next(self):
        assert(self.n1 == next(self.graph.V(name='Raf')))

    def test_edge_iterator_next(self):
        assert(self.e1 == next(self.graph.E(since=1991)))

    # Test iterator update (batch update)

    def test_node_iterator_update_1(self):
        assert(self.graph.V(updated=True).count() == 0)
        self.graph.V().update(updated=True)
        assert(self.graph.V(updated=True).count() == self.graph.V().count())

    def test_node_iterator_update_2(self):
        assert(self.graph.V(foo=1, updated=True).count() == 0)
        assert(self.graph.V(foo=1).count() > 0)
        self.graph.V(foo=1).update(updated=True)
        assert(self.graph.V(foo=1, updated=True).count() == self.graph.V(foo=1).count())

    def test_edge_iterator_update_1(self):
        assert(self.graph.E(updated=True).count() == 0)
        self.graph.E().update(updated=True)
        assert(self.graph.E(updated=True).count() == self.graph.E().count())

    def test_edge_iterator_update_2(self):
        assert(self.graph.E(label='knows', updated=True).count() == 0)
        assert(self.graph.E(label='knows').count() > 0)
        self.graph.E(label='knows').update(updated=True)
        assert(self.graph.E(label='knows', updated=True).count() == self.graph.E(label='knows').count())

    def test_node_update(self):
        exception_raised = False
        try:
            self.n1.updated
        except AttributeError:
            exception_raised = True
        assert(exception_raised)
        self.n1.update(updated=True)
        assert(self.n1.updated is True)

    def test_edge_update(self):
        exception_raised = False
        try:
            self.e1.updated
        except AttributeError:
            exception_raised = True
        assert(exception_raised)
        self.e1.update(updated=True)
        assert(self.e1.updated is True)

    # Testing node and edge representation :

    def test_node_repr(self):
        # Very basic test...
        assert('node' in repr(self.n1))
        assert('id' in repr(self.n1))

    def test_edge_repr(self):
        # Very basic test...
        assert('edge' in repr(self.e1))
        assert('id' in repr(self.e1))

    # Testing bulk_add_node

    def test_bulk_add_node(self):
        # Just checking that no exception and node count increased :
        count_before = self.graph.V().count()
        self.graph.bulk_add_node([dict(foo=10), dict(bar=11), dict(baz=12)])
        count_after = self.graph.V().count()
        assert(count_after == count_before + 3)

    # Testing bulk_add_edge

    def test_bulk_add_edge(self):
        # Just checking that no exception raised and that edge count increased :
        count_before = self.graph.E().count()
        self.graph.bulk_add_edge([(self.n3, self.n1, {}), (self.n2, self.n2, {})])
        count_after = self.graph.E().count()
        assert(count_after == count_before + 2)

    # Test invalid field name :

    def test_invalid_field_name_1(self):
        exception_raised = False
        try:
            self.graph.add_node(_toto=1)
        except GrapheekInvalidDataTypeException:
            exception_raised = True
        assert(exception_raised)

    def test_invalid_field_name_2(self):
        exception_raised = False
        try:
            self.graph.add_node(toto_=1)
        except GrapheekInvalidDataTypeException:
            exception_raised = True
        assert(exception_raised)

    def test_invalid_field_name_3(self):
        exception_raised = False
        try:
            self.graph.add_node(to__to=1)
        except GrapheekInvalidDataTypeException:
            exception_raised = True
        assert(exception_raised)

    def test_invalid_field_value(self):
        exception_raised = False

        class A:
            pass

        try:
            self.graph.add_node(foo=[1, 2, A()])
        except GrapheekInvalidDataTypeException:
            exception_raised = True
        assert(exception_raised)

    # Testing add_edge with invalid nodes

    def test_add_invalid_edge_string_instead_of_node(self):
        exception_raised = False
        try:
            self.graph.add_edge('not_a_node', 'neither', foo=1)
        except GrapheekInvalidDataTypeException:
            exception_raised = True
        assert(exception_raised)

    # Test graph export/import

    def test_export_import(self):
        import tempfile
        self.graph.add_node_index('bar')
        self.graph.add_edge_index('since')
        before_node_count = self.graph.V().count()
        before_edge_count = self.graph.E().count()
        before_node_indexes = self.graph.get_node_indexes()
        before_edge_indexes = self.graph.get_edge_indexes()
        path = tempfile.mktemp() + '.msgpack'
        # Dumping :
        self.graph.write(path)
        # Reloading in a memory database :
        from grapheekdb.backends.data.localmem import LocalMemoryGraph
        newgraph = LocalMemoryGraph()
        newgraph.read(path)
        after_node_count = newgraph.V().count()
        after_edge_count = newgraph.E().count()
        after_node_indexes = newgraph.get_node_indexes()
        after_edge_indexes = newgraph.get_edge_indexes()
        assert(after_node_count == before_node_count)
        assert(after_edge_count == before_edge_count)
        assert(after_node_indexes == before_node_indexes)
        assert(after_edge_indexes == before_edge_indexes)

    # Test dot generation - to_dot method

    def test_dot_to_dot_generation_basic(self):
        str(self.graph.V().dot().to_dot())

    def test_dot_to_dot_generation_with_filter(self):
        str(self.graph.V(foo=1).dot().to_dot())

    def test_dot_to_dot_generation_boolean_label_1(self):
        # Adding a node with a boolean property
        self.graph.add_node(visible=True)
        # Get the representation
        s = repr(self.graph.V().dot('visible').to_dot())
        assert('digraph' in s)

    def test_dot_to_dot_generation_basic_2(self):
        s = repr(self.graph.E().dot().to_dot())
        assert('digraph' in s)

    # Test entity iterator without method :

    def test_iterator_without_method(self):
        assert(list(self.graph.V(name='Raf').aka('x').out_().both_().without('x')) == [self.n3])

    # Check attribute access after graph closure raises an exception :

    def test_double_close(self):
        self.graph.close()
        exception_raised = False
        try:
            self.graph.V().count()
        except GrapheekDataException:
            exception_raised = True
        assert(exception_raised)

    def test_graph_as_nx_graph(self):
        try:
            import networkx  # noqa
            G = self.graph.nx_graph()
            assert(len(G.node) == self.graph.V().count())
        except ImportError:
            pass

    # Test entity random method :

    def test_entity_random_method_inv(self):
        self.n4 = self.graph.add_node(data=4)
        self.e3 = self.graph.add_edge(self.n4, self.n2)
        lst = []
        for i in range(1000):
            assert(self.n2.random('inV').count() == 1)
            lst.append(self.n2.random('inV').next().get_id())
        assert(len(set(lst)) > 1)

    def test_edge_random_method_inv(self):
        self.n4 = self.graph.add_node(data=4)
        self.e3 = self.graph.add_edge(self.n4, self.n2)
        assert(self.e3.random('inV').count() == 1)

    def test_entity_random_method_outv(self):
        self.n4 = self.graph.add_node(data=4)
        self.e3 = self.graph.add_edge(self.n1, self.n4)
        lst = []
        for i in range(1000):
            assert(self.n1.random('outV').count() == 1)
            lst.append(self.n1.random('outV').next().get_id())
        assert(len(set(lst)) > 1)

    def test_entity_random_method_bothv(self):
        self.n4 = self.graph.add_node(data=4)
        self.e3 = self.graph.add_edge(self.n1, self.n4)
        lst = []
        for i in range(1000):
            assert(self.n1.random('bothV').count() == 1)
            lst.append(self.n1.random('bothV').next().get_id())
        assert(len(set(lst)) > 1)

    def test_entity_random_method_ine(self):
        self.n4 = self.graph.add_node(data=4)
        self.e3 = self.graph.add_edge(self.n4, self.n2)
        lst = []
        for i in range(1000):
            assert(self.n2.random('inE').count() == 1)
            lst.append(self.n2.random('inE').next().get_id())
        assert(len(set(lst)) > 1)

    def test_entity_random_method_oute(self):
        self.n4 = self.graph.add_node(data=4)
        self.e3 = self.graph.add_edge(self.n1, self.n4)
        lst = []
        for i in range(1000):
            assert(self.n1.random('outE').count() == 1)
            lst.append(self.n1.random('outE').next().get_id())
        assert(len(set(lst)) > 1)

    def test_entity_random_method_bothe(self):
        self.n4 = self.graph.add_node(data=4)
        self.e3 = self.graph.add_edge(self.n1, self.n4)
        lst = []
        for i in range(1000):
            assert(self.n1.random('bothE').count() == 1)
            lst.append(self.n1.random('bothE').next().get_id())
        assert(len(set(lst)) > 1)

    # Test iterator random method :

    def test_iterator_random_method_inv(self):
        self.n4 = self.graph.add_node(data=4)
        self.e3 = self.graph.add_edge(self.n4, self.n2)
        lst = []
        for i in range(1000):
            assert(self.graph.V(name='Flo').random('inV').count() == 1)
            lst.append(self.graph.V(name='Flo').random('inV').next().get_id())
        assert(len(set(lst)) > 1)

    def test_iterator_random_method_outv(self):
        self.n4 = self.graph.add_node(data=4)
        self.e3 = self.graph.add_edge(self.n1, self.n4)
        lst = []
        for i in range(1000):
            assert(self.graph.V(name='Raf').random('outV').count() == 1)
            lst.append(self.graph.V(name='Raf').random('outV').next().get_id())
        assert(len(set(lst)) > 1)

    def test_iterator_random_method_bothv(self):
        self.n4 = self.graph.add_node(data=4)
        self.e3 = self.graph.add_edge(self.n1, self.n4)
        lst = []
        for i in range(1000):
            assert(self.graph.V(name='Raf').random('bothV').count() == 1)
            lst.append(self.graph.V(name='Raf').random('bothV').next().get_id())
        assert(len(set(lst)) > 1)

    def test_iterator_random_method_ine(self):
        self.n4 = self.graph.add_node(data=4)
        self.e3 = self.graph.add_edge(self.n4, self.n2)
        lst = []
        for i in range(1000):
            assert(self.graph.V(name='Flo').random('inE').count() == 1)
            lst.append(self.graph.V(name='Flo').random('inE').next().get_id())
        assert(len(set(lst)) > 1)

    def test_iterator_random_method_oute(self):
        self.n4 = self.graph.add_node(data=4)
        self.e3 = self.graph.add_edge(self.n1, self.n4)
        lst = []
        for i in range(1000):
            assert(self.graph.V(name='Raf').random('outE').count() == 1)
            lst.append(self.graph.V(name='Raf').random('outE').next().get_id())
        assert(len(set(lst)) > 1)

    def test_iterator_random_method_bothe(self):
        self.n4 = self.graph.add_node(data=4)
        self.e3 = self.graph.add_edge(self.n1, self.n4)
        lst = []
        for i in range(1000):
            assert(self.graph.V(name='Raf').random('bothE').count() == 1)
            lst.append(self.graph.V(name='Raf').random('bothE').next().get_id())
        assert(len(set(lst)) > 1)

    # Test random methods combined with other (reproduction, filters, aggregation)

    def test_iterator_random_method_inv_reproduction(self):
        self.n4 = self.graph.add_node(data=4)
        self.e3 = self.graph.add_edge(self.n4, self.n2)
        # Just checking that no exception is raised :
        for i in range(10):
            list(self.graph.V(name='Raf').random('outV', i))

    def test_iterator_random_method_inv_count(self):
        self.n4 = self.graph.add_node(data=4)
        self.e3 = self.graph.add_edge(self.n4, self.n2)
        # Just checking that no exception is raised :
        int(self.graph.V(name='Raf').random('outV').count())

    def test_iterator_random_method_inv_filters(self):
        self.n4 = self.graph.add_node(data=4)
        self.e3 = self.graph.add_edge(self.n4, self.n2)
        # Just checking that no exception is raised :
        int(self.graph.V(name='Raf').random('outV', zorglub__isnull=True).count())

    # Test random method with unknown traversal :
    # for both entity and iterator :

    def test_entity_random_method_unknown_method(self):
        self.n4 = self.graph.add_node(data=4)
        self.e3 = self.graph.add_edge(self.n4, self.n2)
        # Just checking that AN exception IS raised :
        exception_raised = False
        try:
            self.n1.random('zorglub').count()
        except GrapheekUnknownMethod:
            exception_raised = True
        assert(exception_raised)

    def test_edge_random_method_unknown_method(self):
        self.n4 = self.graph.add_node(data=4)
        self.e3 = self.graph.add_edge(self.n4, self.n2)
        # Just checking that AN exception IS raised :
        exception_raised = False
        try:
            self.e3.random('zorglub').count()
        except GrapheekUnknownMethod:
            exception_raised = True
        assert(exception_raised)

    def test_iterator_random_method_unknown_method(self):
        self.n4 = self.graph.add_node(data=4)
        self.e3 = self.graph.add_edge(self.n4, self.n2)
        # Just checking that AN exception IS raised :
        exception_raised = False
        try:
            self.graph.V(name='Raf').random('zorglub').count()
        except GrapheekUnknownMethod:
            exception_raised = True
        assert(exception_raised)

    def test_iterator_random_method_unknown_method_2(self):
        self.n4 = self.graph.add_node(data=4)
        self.e3 = self.graph.add_edge(self.n4, self.n2)
        # Just checking that AN exception IS raised :
        exception_raised = False
        try:
            self.graph.E(label='knows').random('zorglub').count()
        except GrapheekUnknownMethod:
            exception_raised = True
        assert(exception_raised)

    def test_iterator_random_method_unknown_method_3(self):
        self.n4 = self.graph.add_node(data=4)
        self.e3 = self.graph.add_edge(self.n4, self.n2)
        # Just checking that AN exception IS raised :
        exception_raised = False
        try:
            self.graph.E(label='knows').random('outE').count()
        except GrapheekUnknownMethod:
            exception_raised = True
        assert(exception_raised)

    def test_ids_method_on_nodes(self):
        assert(self.graph.V().count() == len(list(self.graph.V().ids())))

    def test_ids_method_on_edges(self):
        assert(self.graph.E().count() == len(list(self.graph.E().ids())))

    def test_in_method_on_nodes_1(self):
        it1 = self.graph.V()
        n1 = self.graph.V().next()
        assert(n1 in it1)

    def test_in_method_on_nodes_2(self):
        it1 = self.graph.V(foo=1)
        n1 = self.graph.V(foo=2).next()
        assert(n1 not in it1)

    def test_in_method_on_edges_1(self):
        it1 = self.graph.E()
        e1 = self.graph.E().next()
        assert(e1 in it1)

    def test_in_method_on_edges_2(self):
        it1 = self.graph.E(since=1991)
        e1 = self.graph.E(since=1996).next()
        assert(e1 not in it1)

    def test_in_stupid_test_1(self):
        it1 = self.graph.V()
        e1 = self.graph.E().next()
        assert(e1 not in it1)  # an edge cannot be in node iterator

    def test_in_stupid_test_2(self):
        it1 = self.graph.V()
        sth = 1
        assert(sth not in it1)  # node iterator can only "contains" nodes

    def test_issubset_method_on_nodes(self):
        it1 = self.graph.V()
        it2 = self.graph.V(bar=3)
        assert(self.graph.issubset(it2, it1))
        assert(it2 <= it1)

    def test_issubset_method_on_edges(self):
        it1 = self.graph.E()
        it2 = self.graph.E(since=1996)
        assert(self.graph.issubset(it2, it1))
        assert(it2 <= it1)

    def test_issuperset_method_on_nodes(self):
        it1 = self.graph.V()
        it2 = self.graph.V(bar=3)
        assert(self.graph.issuperset(it1, it2))
        assert(it1 >= it2)

    def test_issuperset_method_on_edges(self):
        it1 = self.graph.E()
        it2 = self.graph.E(since=1996)
        assert(self.graph.issuperset(it1, it2))
        assert(it1 >= it2)

    def test_union_method_on_nodes(self):
        it1 = self.graph.V(foo=1)
        it2 = self.graph.V(bar=3)
        assert(self.graph.union(it1, it2).count() == 3)
        assert((it1 | it2).count() == 3)  # using operator

    def test_union_method_on_edges(self):
        it1 = self.graph.E(since=1991)
        it2 = self.graph.E(since=1996)
        assert(self.graph.union(it1, it2).count() == 2)
        assert((it1 | it2).count() == 2)

    def test_mixed_type_is_forbidden_in_union(self):
        v = self.graph.V()
        e = self.graph.E()
        # Just checking that AN exception IS raised :
        exception_raised = False
        try:
            list(self.graph.union(v, e))  # casting to list to force iteration start
        except GrapheekMixedKindException:
            exception_raised = True
        except GrapheekException:
            if self.__class__.__name__ == 'TestClient':
                exception_raised = True
        assert(exception_raised)

    def test_intersection_method_on_nodes(self):
        it1 = self.graph.V(foo=1)
        it2 = self.graph.V(bar=3)
        assert(self.graph.intersection(it1, it2).count() == 1)
        assert((it1 & it2).count() == 1)

    def test_intersection_method_on_edges(self):
        it1 = self.graph.E()
        it2 = self.graph.E(since=1996)
        assert(self.graph.intersection(it1, it2).count() == 1)
        assert((it1 & it2).count() == 1)

    def test_difference_method_on_nodes(self):
        it1 = self.graph.V(foo=1)
        it2 = self.graph.V(bar=3)
        assert(self.graph.difference(it1, it2).count() == 1)
        assert((it1 - it2).count() == 1)

    def test_difference_method_on_edges(self):
        it1 = self.graph.E()
        it2 = self.graph.E(since=1996)
        assert(self.graph.difference(it1, it2).count() == 1)
        assert((it1 - it2).count() == 1)

    def test_symmetric_difference_method_on_nodes(self):
        it1 = self.graph.V(foo=1)
        it2 = self.graph.V(bar=3)
        assert(self.graph.symmetric_difference(it1, it2).count() == 2)
        assert((it1 ^ it2).count() == 2)

    def test_symmetric_difference_method_on_edges(self):
        it1 = self.graph.E()
        it2 = self.graph.E(since=1996)
        assert(self.graph.symmetric_difference(it1, it2).count() == 1)
        assert((it1 ^ it2).count() == 1)

    def test_method_on_traversal_iterators_does_not_raise_exception(self):
        it1 = self.graph.V(foo=1).in_()
        it2 = self.graph.V(bar=3).out_()
        for op_name in ['union', 'intersection', 'difference', 'symmetric_difference']:
            method = getattr(self.graph, op_name)
            method(it1, it2).count()

    # Graph and entity iterators methods should not exhaust internal iterators
    # Except those obviously iterating (next,iter)
    # (it was the case for 0.0.18)

    # Checking that set operation don't exhaust operand internal operators :

    def test_no_exhaust_using_set_operations_1(self):
        it1 = self.graph.V(foo=1)
        it2 = self.graph.V(foo=2)
        count1 = it1.count()
        count2 = it2.count()
        for op_name in ['union', 'intersection', 'difference', 'symmetric_difference']:
            method = getattr(self.graph, op_name)
            method(it1, it2).all()  # all is here only to force iteration
            assert(it1.count() == count1)
            assert(it2.count() == count2)

    def test_no_exhaust_using_set_operations_2(self):
        it1 = self.graph.V(foo=1)
        it2 = self.graph.V(foo=2)
        count1 = it1.count()
        count2 = it2.count()
        for op_name in ['issubset', 'issuperset']:
            method = getattr(self.graph, op_name)
            method(it1, it2)  # force computing of the result (boolean)
            assert(it1.count() == count1)
            assert(it2.count() == count2)

    #### count

    def test_no_exhaust_count_1(self):
        it = self.graph.V()
        assert(it.count() == it.count())

    def test_no_exhaust_count_2(self):
        it = self.graph.V(foo=1)
        assert(it.count() == it.count())

    def test_no_exhaust_count_3(self):
        it = self.graph.E()
        assert(it.count() == it.count())

    def test_no_exhaust_count_4(self):
        it = self.graph.E(since=1996)
        assert(it.count() == it.count())

    #### all

    def test_no_exhaust_all_1(self):
        it = self.graph.V()
        assert(len(it.all()) == len(it.all()))

    def test_no_exhaust_all_2(self):
        it = self.graph.V(foo=1)
        assert(len(it.all()) == len(it.all()))

    def test_no_exhaust_all_3(self):
        it = self.graph.E()
        assert(len(it.all()) == len(it.all()))

    def test_no_exhaust_all_4(self):
        it = self.graph.E(since=1996)
        assert(len(it.all()) == len(it.all()))

    #### ids

    def test_no_exhaust_ids_1(self):
        it = self.graph.V()
        assert(set(it.ids()) == set(it.ids()))

    def test_no_exhaust_ids_2(self):
        it = self.graph.V(foo=1)
        assert(set(it.ids()) == set(it.ids()))

    def test_no_exhaust_ids_3(self):
        it = self.graph.E()
        assert(set(it.ids()) == set(it.ids()))

    def test_no_exhaust_ids_4(self):
        it = self.graph.E(since=1996)
        assert(set(it.ids()) == set(it.ids()))

    #### data

    def test_no_exhaust_data_1(self):
        it = self.graph.V()
        assert(it.data() == it.data())

    def test_no_exhaust_data_2(self):
        it = self.graph.V(foo=1)
        assert(it.data() == it.data())

    def test_no_exhaust_data_3(self):
        it = self.graph.E()
        assert(it.data() == it.data())

    def test_no_exhaust_data_4(self):
        it = self.graph.E(since=1996)
        assert(it.data() == it.data())

    ### various path traversal + count/all/ids/data

    def test_no_exhaust_path_traversal_count_starting_from_node(self):
        for method_name in ['inV', 'outV', 'bothV', 'in_', 'out_', 'both_', 'inE', 'outE', 'bothE']:
            method = getattr(self.n1, method_name)
            it = method()
            assert(it.count() == it.count())
            assert(len(it.all()) == it.count() == len(it.all()) == len(it.ids()))
            assert(set(it.ids()) == set(it.ids()))
            assert(it.data() == it.data())

    def test_no_exhaust_path_traversal_count_starting_from_edge(self):
        for method_name in ['inV', 'outV', 'bothV']:
            method = getattr(self.n1, method_name)
            it = method()
            assert(it.count() == it.count())
            assert(len(it.all()) == it.count() == len(it.all()) == len(it.ids()))
            assert(set(it.ids()) == set(it.ids()))
            assert(it.data() == it.data())

    # dedup

    def test_no_exhaust_dedup(self):
        it = self.graph.V()
        assert(it.dedup().data() == it.dedup().data())

    # without

    def test_no_exhaust_without(self):
        it = self.graph.V(foo=1)
        assert(it.aka('start').outV().inV().without('start').data() == it.aka('start').outV().inV().without('start').data())

    # limit

    def test_no_exhaust_limit(self):
        it = self.graph.V()
        assert(it.limit(2).data() == it.limit(2).data())

    # collect

    def test_no_exhaust_collect(self):
        it = self.graph.V().aka('start')
        assert(len(it.collect('start')) == len(it.collect('start')))

    # sum

    def test_no_exhaust_sum(self):
        it = self.graph.V().outV().outV().sum()
        assert(list(map(list, it)) == list(map(list, it)))

    # percent

    def test_no_exhaust_percent(self):
        it = self.graph.V().outV().outV().percent()
        assert(list(map(list, it)) == list(map(list, it)))

    # order_by

    def test_order_by_asc_1(self):
        it = self.graph.V().order_by('name')
        n1 = it.next()
        n2 = it.next()
        n3 = it.next()
        assert(n1.name == 'Flo')
        assert(n2.name == 'Raf')
        assert(n3.name == 'Theo')

    def test_order_by_asc_2(self):
        it = self.graph.V().order_by('+name')  # same as previous method with the optionnal '+'
        n1 = it.next()
        n2 = it.next()
        n3 = it.next()
        assert(n1.name == 'Flo')
        assert(n2.name == 'Raf')
        assert(n3.name == 'Theo')

    def test_order_by_desc(self):
        it = self.graph.V().order_by('-name')
        n1 = it.next()
        n2 = it.next()
        n3 = it.next()
        assert(n1.name == 'Theo')
        assert(n2.name == 'Raf')
        assert(n3.name == 'Flo')

    def test_order_by_mix(self):
        it = self.graph.V().order_by('bar', '-foo')
        n1 = it.next()
        n2 = it.next()
        n3 = it.next()
        assert(n1.name == 'Raf')
        assert(n2.name == 'Theo')
        assert(n3.name == 'Flo')
