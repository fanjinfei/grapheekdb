# -*- coding:utf-8 -*-

import re
from grapheekdb.lib.exceptions import GrapheekInvalidExpression

CLAUSE_REGEXP = re.compile(r'''(?P<direction>\-|\+)?(?P<field>[a-zA-Z][a-zA-Z_]*)''')


def build_order_func(*clauses):
    comparers = []
    for clause in clauses:
        m = CLAUSE_REGEXP.match(clause)
        if m is None:
            raise GrapheekInvalidExpression
        ascending = m.group('direction') != '-'
        field = m.group('field')
        comparers.append((field, 1 if ascending else -1))

    def comparer(item1, item2):
        for field, multiplier in comparers:
            value1 = item1.get(field, None)
            value2 = item2.get(field, None)
            if value1 < value2:
                return -1 * multiplier
            if value1 > value2:
                return multiplier
        else:
            return 0

    return comparer
