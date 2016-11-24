#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Example of big dataset load and analysis
# The data sample can be downloaded from http://snap.stanford.edu/data/web-Google.html

import time

# Change path to fit your needs :
google_file_path = '/path/to/web-Google.txt'
db_path = '/path/to/database'

########## 1st part : google file parsing - NO use of grapheekdb so far ##########

from collections import defaultdict

# Change the path
google_file = open(google_file_path, 'r')

print("Reading and parsing Google file", end=' ')

# Load file
lines = google_file.read().split('\n')

# Remove comments and empty lines :
items = [item for item in lines if item and not(item.startswith('#'))]

# Build a list of (source, target)
source_target = [item.split() for item in items]

# create a dictionnary holding google source_id -> *all* related google target_id
source_to_targets = defaultdict(list)

edge_count = 0
google_node_ids = set()
for source_id, target_id in source_target:
    edge_count += 1
    google_node_ids.add(source_id)
    google_node_ids.add(target_id)
    source_to_targets[source_id].append(target_id)

node_count = len(google_node_ids)

print("DONE")

########## 2nd part : import data in GrapheekDB ##########

# Given the dataset size, I won't use the client API so far
# But instead, I will directy use the backend
# The good news is that the API is the same (as ensured by test : tests/test_client.py : TestClient)

# I don't want to insert data every time I need to use the Google datasets
# So I won't use LocalMemory backend
# But instead use a persistent backend : kyotocabinet :

from grapheekdb.backends.data.kyotocab import KyotoCabinetGraph
# caution : as explained in KyotoCabinet docs :
# http://fallabs.com/kyotocabinet/pythonlegacydoc/kyotocabinet.DB-class.html#open
# file extension defines the database kind
# By using a .kch extension, I'm implicitely creating a file hash database
# For bnum/opts/msiz parameters, check KyotoCabinet doc at :
# http://fallabs.com/kyotocabinet/spex.html & http://fallabs.com/kyotocabinet/pythonlegacydoc/kyotocabinet.DB-class.html#open
g = KyotoCabinetGraph(db_path + '.kch#bnum=50000000#opts=l#msiz=512000000')  # Change path to fit your needs :)

# This dictionnary will hold google_id -> grapheekdb_id mapping
google_id_to_grapheekdb_id = {}

start = time.time()

print("Creating %s nodes" % (node_count,))
print('----------------------------------------')

counter = 0

for google_node_id in iter(google_node_ids):
    node = g.add_node(gid=google_node_id)
    google_id_to_grapheekdb_id[google_node_id] = node.get_id()
    counter += 1
    if counter % 10000 == 0:
        print('%s nodes inserted' % (counter,))

node_end = time.time()
print("** node insert is finished in %s sec**" % (node_end - start,))
speed = int(float(node_count) / (node_end - start))
print("speed : %s nodes/sec" % (speed,))

print()
print("Now, creating %s edges" % (edge_count,))
print('----------------------------------------')

counter = 0
for google_source_id, google_target_ids in source_to_targets.items():
    for google_target_id in google_target_ids:
        grapheek_source_id = google_id_to_grapheekdb_id[google_source_id]
        grapheek_target_id = google_id_to_grapheekdb_id[google_target_id]
        g.add_edge_by_ids(grapheek_source_id, grapheek_target_id)
        counter += 1
        if counter % 10000 == 0:
            print('%s edges inserted' % (counter,))

edge_end = time.time()

print("** edge insert is finished in %s sec**" % (edge_end - node_end,))
speed = int(float(edge_count) / (edge_end - node_end))
print("speed : %s nodes/sec" % (speed,))

print()
print("Now, indexing node on google id (field : 'gid') ")
print('----------------------------------------')

g.add_node_index('gid')

end = time.time()

print("Indexation finished in %s sec" % (end - edge_end, ))
print()
print("TOTAL TIME : %s sec" % (end - start,))

print("Don't hesitate to make a copy of resulting database (google.kch)")
print("before playing with it ;)")


