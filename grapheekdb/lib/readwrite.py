# -*- coding:utf-8 -*-

import json


class GraphReadWrite(object):

    def write(self, path):
        # Open file soon to avoid building a big structure
        # if file cannot be written
        fp = open(path, 'w')
        # ---
        nodes = []
        edges = []
        node_indexes = []
        edge_indexes = []
        for node in self.V():
            data = node.data().copy()
            data['_id'] = node.get_id()
            nodes.append(data)
        for edge in self.E():
            data = edge.data().copy()
            data['_src'] = edge.inV().next().get_id()  # could be optimized
            data['_tgt'] = edge.outV().next().get_id()  # ditto
            edges.append(data)
        node_indexes = self.get_node_indexes()
        edge_indexes = self.get_edge_indexes()
        dic = {
            'nodes': nodes,
            'edges': edges,
            'node_indexes': node_indexes,
            'edge_indexes': edge_indexes
        }
        json.dump(dic, fp)
        # finished, close file
        fp.close()

    def read(self, path, node_dump_id_to_node=None):
        with open(path, 'r') as fp:
            dic = json.load(fp)
        nodes = dic.get('nodes', [])
        # reloading nodes :
        ids = [data.pop('_id') for data in nodes]
        if node_dump_id_to_node is None:
            node_dump_id_to_node = {}
        node_dump_id_to_node.update(zip(ids, self.bulk_add_node(nodes)))
        del nodes
        # reloading edges :
        edges = dic.get('edges', [])
        edge_data = [
            (node_dump_id_to_node[data.pop('_src')],
             node_dump_id_to_node[data.pop('_tgt')],
             data)
            for data in edges]
        self.bulk_add_edge(edge_data)
        del edges
        # Adding node indexes :
        for fields, filters in dic.get('node_indexes', []):
            self.add_node_index(*fields)
        # Adding edge indexes :
        for fields, filters in dic.get('edge_indexes', []):
            self.add_edge_index(*fields, **filters)
        return node_dump_id_to_node
