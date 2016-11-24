# -*- coding:utf-8 -*-


class GraphNx(object):

    def nx_graph(self):
        # This may raise an exception
        # let's it raise to warn user that
        # networkx should be installed for this method to be used
        import networkx as nx
        G = nx.DiGraph()
        node_ids = set()
        edge_ids = set()
        for node in self.V():
            node_id = node.get_id()
            if not node_id in node_ids:
                G.add_node(node_id, **node.data())
                node_ids.add(node_id)
            for out_edge, out_node in zip(node.outE(), node.outV()):
                out_edge_id = out_edge.get_id()
                out_node_id = out_node.get_id()
                if not out_node_id in node_ids:
                    G.add_node(out_node_id, **out_node.data())
                    node_ids.add(out_node_id)
                if not out_edge_id in edge_ids:
                    G.add_edge(node_id, out_node_id, **out_edge.data())
                    edge_ids.add(out_edge_id)
        return G
