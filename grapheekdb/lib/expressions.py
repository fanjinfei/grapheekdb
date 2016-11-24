import ast
import operator as op
import re

from grapheekdb.lib.exceptions import GrapheekInvalidExpression

# supported operators
operators = {ast.Add: op.add, ast.Sub: op.sub, ast.Mult: op.mul,
             ast.Div: op.truediv, ast.FloorDiv: op.floordiv, ast.Pow: op.pow}
cmps = {ast.Lt: op.lt, ast.LtE: op.le, ast.Gt: op.gt, ast.GtE: op.ge, ast.Eq: op.eq}

# valid attribute name regex :
VALID_ATTRIBUTE = r'''^[a-zA-Z][a-zA-Z0-9_]*$'''
VALID_ATTRIBUTE_REGEX = re.compile(VALID_ATTRIBUTE)


def eval_expr(context, expr):
    try:
        return eval_(context, ast.parse(expr).body[0].value)
    except GrapheekInvalidExpression:
        raise
    except:
        raise GrapheekInvalidExpression


def eval_(context, node):
    if isinstance(node, ast.Num):
        return node.n
    elif isinstance(node, ast.operator):
        return operators[type(node)]
    elif isinstance(node, ast.cmpop):
        return cmps[type(node)]
    elif isinstance(node, ast.IfExp):
        return eval_(context, node.body) if eval_(context, node.test) else eval_(context, node.orelse)
    elif isinstance(node, ast.BinOp):
        return eval_(context, node.op)(eval_(context, node.left), eval_(context, node.right))
    elif isinstance(node, ast.Name):
        return context[node.id]
    elif isinstance(node, ast.Attribute):
        attr = node.attr
        if VALID_ATTRIBUTE_REGEX.match(attr):
            return eval_(context, node.value).get(node.attr, 0)
        raise GrapheekInvalidExpression("attributes must be of the form %s" % (VALID_ATTRIBUTE,))
    elif isinstance(node, ast.Compare):
        result = eval_(context, node.left)
        for op, comp in zip(node.ops, node.comparators):
            result = eval_(context, op)(result, eval_(context, comp))
        return result
    else:
        raise GrapheekInvalidExpression(repr(node))
