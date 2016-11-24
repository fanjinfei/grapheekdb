# -*- coding:utf-8 -*-

from grapheekdb.lib.exceptions import GrapheekInvalidDataTypeException
from grapheekdb.lib.expressions import VALID_ATTRIBUTE_REGEX

try:  # pragma : no cover
    LONG = long
except NameError:  # pragma : no cover
    LONG = int
NONETYPE = type(None)
try:  # pragma : no cover
    UNICODE = unicode
except NameError:  # pragma : no cover
    UNICODE = str


def check_valid_value(value):
    if isinstance(value, (bool, float, int, str, LONG, NONETYPE, UNICODE)):
        return True
    elif isinstance(value, list):
        return all([check_valid_value(subvalue) for subvalue in value])
    elif isinstance(value, dict):
        return check_valid_data(value, check_dict=False)
    return False


def check_valid_data(data, check_dict=True):
    # Checking that data is a dictionnary
    if check_dict and not isinstance(data, dict):
        raise GrapheekInvalidDataTypeException('data must be a dictionnary')
    # Ckecking that data keys match '^[a-zA-A][a-zA-Z0-9]*$' regular expression
    if not(all(VALID_ATTRIBUTE_REGEX.match(key) for key in data)):
        raise GrapheekInvalidDataTypeException('a data key must only contain letter and digit and start with a letter')
    # Ckecking that data keys don't contain a double underscore (could mess lookup)
    if any('__' in key for key in data):
        raise GrapheekInvalidDataTypeException('a data key must not contain a double underscore')
    # Ckecking that data keys don't end with an underscore (could mess lookup)
    if any(key.endswith('_') for key in data):
        raise GrapheekInvalidDataTypeException('a data key must not end with an underscore')
    # Values validations  :
    if not all([check_valid_value(value) for value in list(data.values())]):
        raise GrapheekInvalidDataTypeException('value must be one of following type : boolean/float/integer/long/none/string/unicode/dict/list')
    return True
