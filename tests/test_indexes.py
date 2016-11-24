#!/usr/bin/env
# -*- coding: utf-8 -*-

from grapheekdb.backends.data.indexes import BaseIndex


class TestBaseIndex(object):

    def setup(self):
        self.index = BaseIndex(None, None, None)

    def test_bulk_add_is_not_implemented(self):
        exception_raised = False
        try:
            self.index.bulk_add(None, None)
        except NotImplementedError:
            exception_raised = True
        assert(exception_raised)

    def test_add_is_not_implemented(self):
        exception_raised = False
        try:
            self.index.add(None, None, None)
        except NotImplementedError:
            exception_raised = True
        assert(exception_raised)

    def test_remove_is_not_implemented(self):
        exception_raised = False
        try:
            self.index.remove(None, None, None)
        except NotImplementedError:
            exception_raised = True
        assert(exception_raised)

    def test_estimate_is_not_implemented(self):
        exception_raised = False
        try:
            self.index.estimate(None, None)
        except NotImplementedError:
            exception_raised = True
        assert(exception_raised)

    def test_ids_is_not_implemented(self):
        exception_raised = False
        try:
            self.index.ids(None, None)
        except NotImplementedError:
            exception_raised = True
        assert(exception_raised)
