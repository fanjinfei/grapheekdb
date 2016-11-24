# -*- coding:utf-8 -*-
"""
This module is mainly used for tests
It exposes a single function that echoes arguments and keyword arguments
"""


def echo(g, _ctxs, *args, **kwargs):
    return [dict(args=args, kwargs=kwargs) for ctx in _ctxs]
