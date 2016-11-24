from random import randint
import time

from locust import Locust, events, task, TaskSet

from grapheekdb.client.api import ProxyGraph
from grapheekdb.lib.exceptions import GrapheekException

graph = ProxyGraph('tcp://127.0.0.1:5555')
try:
    graph.add_node_index('foo') 
except GrapheekException:
    pass

class GraphUser(TaskSet):
    def on_start(self):
        """ on_start is called when a Locust start before any task is scheduled """
        self.graph = ProxyGraph('tcp://127.0.0.1:5555')

    @task(999)
    def add_node(self):
        start = time.time()
        try:
            self.graph.add_node(foo=randint(1, 1000))
        except:
            events.request_failure.fire(request_type="grapheekdb", name="add_node", response_time=1, response_length=10)
        else:
            end = time.time()
            total_time = end - start
            events.request_success.fire(request_type="grapheekdb", name="add_node", response_time=total_time, response_length=10)
        

    @task(5)
    def remove_node(self):
        start = time.time()
        try:
            self.graph.V(foo=randint(1, 1000)).remove()
        except:
            events.request_failure.fire(request_type="grapheekdb", name="remove_node", response_time=total_time, response_length=10)
        else:
            end = time.time()
            total_time = end - start
            events.request_success.fire(request_type="grapheekdb", name="remove_node", response_time=total_time, response_length=10)


class WebsiteUser(Locust):
    task_set = GraphUser
    min_wait = 200
    max_wait = 1000
