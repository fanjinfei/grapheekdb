# -*- coding:utf-8 -*-


# Base Exception

class GrapheekException(Exception):
    pass


# Marshall/Unmarshall exceptions :

class GrapheekMarshallingException(GrapheekException):
    pass


class GrapheekUnmarshallingException(GrapheekException):
    pass


# Client Exceptions

class GrapheekNotIterableException(GrapheekException):
    pass


class GrapheekUnknownEntityException(GrapheekException):
    pass


class GrapheekNotIntegerException(GrapheekException):
    pass


# Data (<-> backend) exceptions :

class GrapheekDataException(GrapheekException):
    pass


class GrapheekDataPreparationFailedException(GrapheekDataException):
    pass


class GrapheekSubLookupNotImplementedException(GrapheekDataException):
    pass


class GrapheekInvalidLookupException(GrapheekDataException):
    pass


class GrapheekIndexAlreadyExistsException(GrapheekDataException):
    pass


class GrapheekIndexCreationFailedException(GrapheekDataException):
    pass


class GrapheekIndexRemovalFailedException(GrapheekDataException):
    pass


class GrapheekIncompetentIndexException(GrapheekDataException):
    pass


class GrapheekNoSuchTraversalException(GrapheekDataException):
    pass


class GrapheekMissingKeyException(GrapheekDataException):
    pass


class GrapheekInvalidDataTypeException(GrapheekDataException):
    pass


class GrapheekUnknownScriptException(GrapheekDataException):
    pass


class GrapheekUnknownAlias(GrapheekDataException):
    pass


class GrapheekUnknownMethod(GrapheekDataException):
    pass


class GrapheekMixedKindException(GrapheekDataException):
    pass


# Expr exceptions :

class GrapheekInvalidExpression(GrapheekDataException):
    pass


# Algorithms exceptions :

class GrapheekUnreachableNode(Exception):
    pass
