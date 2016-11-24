# -*- coding:utf-8 -*-

"""
This module contains keys pattern that are used to turn a KVS (Key Value Store) into a Graph Database
And various utilities related to keys

Here is the keys tree :
-----------------------

.
├── init                      INITIALIZED ?
├── xxxx                      Forbidden key
├── m                         METADATA
│   ├── e                     edge
│   │   ├── c                 (m/e/c)  edges counter (it will never decrease as it will serve to get new id)
│   │   ├── r                 (m/e/r)  removed edges counter
│   │   ├── id                edge ids list
│   │   │   └── <id % 1000>   (m/e/id/<id % 1000>) a list of max 1000 edge ids (helps to speed id iteration)
│   │   ├── ci                (m/e/ci) edges counter for index
│   │   ├── il                (m/e/il) edge indexes list
│   │   ├── if                (m/e/if) edge indexes fields
│   │   │   └── <id>          (m/e/i/<id>) edge index id fields
│   │   └── i                 index
│   │       └── *key sorted   (m/e/i/*sorted_key)  indexes
│   └── v                     vertex (aka node)
│       ├── c                 (m/v/c)  vertices counter (it will never decrease as it will serve to get new id)
│       ├── r                 (m/v/r)  removed vertices counter
│       ├── id                vertex ids list
│       │   └── <id % 1000>   (m/e/id/<id % 1000>) a list of max 1000 vertex ids (helps to speed id iteration)
│       ├── ci                (m/v/ci) vertices counter for index
│       ├── il                (m/v/il) vertices indexes list
│       ├── if                (m/e/if) vertices indexes fields
│       │   └── <id>          (m/e/i/<id>) vertex index id fields
│       └── i                 index
│           └── *key sorted   (m/v/i/*sorted_key)  indexes
├── i                         INDEXES
│   ├── e                     edge
│   │   └── <id>              edge index id
│   │       └── (* values)    values tuple (can be understand when relation is done with (m/v/i/*sorted_key))
│   └── v                     vertex
│       └── <id>              vertex index id
│           └── (* values)    values tuple (can be understand when relation is done with (m/v/i/*sorted_key))
├── e                         EDGE (DATA & RELATIONS)
│   └── <id>                  edge id
│       ├── d                 edge data
│       ├── i                 edge indexes containing this edge (this is a list of index ids)
│       ├── iv                incoming vertices
│       │   └── c             incoming vertices count
│       ├── ov                outgoing vertices
│       │   └── c             outgoing vertices count
│       └── bv                both vertices
│           └── c             both vertices count
└── v                         VERTICES (DATA & RELATIONS)
    └── <id>                  vertex id
        ├── d                 vertex data
        ├── i                 vertex indexes containing this vertex (this is a list of index ids)
        ├── ie                incoming edges
        │   └── c             incoming edges count
        ├── iv                incoming vertices
        │   └── c             incoming vertices count
        ├── oe                outgoing edges
        │   └── c             outgoing edges count
        ├── ov                outgoing vertices
        │   └── c             outgoing vertices count
        ├── be                both edges
        │   └── c             both edges count
        └── bv                both vertices
            └── c             both vertices count

"""

import re

CHUNK_SIZE                          = 1000

PREPARED                            = 'prep'
FORBIDDEN_KEY                       = 'xxxx'

METADATA_EDGE_COUNTER               = 'm/e/c'
METADATA_EDGE_REMOVED_COUNTER       = 'm/e/r'
METADATA_EDGE_ID_LIST_PREFIX        = 'm/e/id'
METADATA_EDGE_INDEX_COUNTER         = 'm/e/ci'
METADATA_EDGE_INDEX_LIST            = 'm/e/il'
METADATA_EDGE_INDEX_FIELDS_PREFIX   = 'm/e/if'
METADATA_EDGE_INDEX_PREFIX          = 'm/e/i'
METADATA_VERTEX_COUNTER             = 'm/v/c'
METADATA_VERTEX_REMOVED_COUNTER     = 'm/v/r'
METADATA_VERTEX_ID_LIST_PREFIX      = 'm/v/id'
METADATA_VERTEX_INDEX_COUNTER       = 'm/v/ci'
METADATA_VERTEX_INDEX_LIST          = 'm/v/il'
METADATA_VERTEX_INDEX_FIELDS_PREFIX = 'm/v/if'
METADATA_VERTEX_INDEX_PREFIX        = 'm/v/i'

DATA_SUFFIX                         = 'd'
IN_EDGES_SUFFIX                     = 'ie'
IN_VERTICES_SUFFIX                  = 'iv'
OUT_EDGES_SUFFIX                    = 'oe'
OUT_VERTICES_SUFFIX                 = 'ov'
BOTH_EDGES_SUFFIX                   = 'be'
BOTH_VERTICES_SUFFIX                = 'bv'
COUNT_SUFFIX                        = 'c'
COUNT_REMOVE_SUFFIX                 = 'r'

KIND_EDGE                           = 'e'
KIND_VERTEX                         = 'v'
KIND_INDEX                          = 'i'

DATA_REGEXP_NOGROUP                 = '^%s/[0-9]+/%s$'
DATA_REGEXP_GROUP                   = '^%s/([0-9]+)/%s$'
EDGE_DATA_REGEXP                    = DATA_REGEXP_NOGROUP % (KIND_EDGE, DATA_SUFFIX)
VERTEX_DATA_REGEXP                  = DATA_REGEXP_NOGROUP % (KIND_VERTEX, DATA_SUFFIX)
EDGE_DATA_PATTERN                   = re.compile(DATA_REGEXP_GROUP % (KIND_EDGE, DATA_SUFFIX))
VERTEX_DATA_PATTERN                 = re.compile(DATA_REGEXP_GROUP % (KIND_VERTEX, DATA_SUFFIX))


def build_key(*args):
    return '/'.join([str(arg) for arg in args])
