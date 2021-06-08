from prometheus_client import start_http_server, Metric, REGISTRY
import json
import requests
import time
import re

UNITS = {
    's': 1,
    'ms': 1000,
    'us': 1000000
}


class JsonCollector(object):
    def __init__(self, endpoint):
        self._endpoint = endpoint

    def collect(self):
        response = json.loads(requests.get(self._endpoint).content.decode('UTF-8'))

        metric = Metric('num_queries_in_statuses',
                        'Number of queries in states: in flight, executing, waiting_queries', 'summary')
        metric.add_sample('num_in_flight_queries',
                          value=response['num_in_flight_queries'], labels={})
        metric.add_sample('num_executing_queries',
                          value=response['num_executing_queries'], labels={})
        metric.add_sample('num_waiting_queries',
                          value=response['num_waiting_queries'], labels={})
        yield metric

        metric = Metric('queries_duration',
                        'Queries duration', 'summary')
        queries_duration = [q['duration'] for q in response['completed_queries']]

        for q in queries_duration:
            pattern = re.compile('(\d+\.\d+)(\w+)')
            query_duration, unit = pattern.match(q).groups()
            query_duration = float(query_duration) / UNITS[unit]
            metric.add_sample('query_duration', value=query_duration, labels={})
        yield metric


if __name__ == '__main__':
    # this will create small http server on each pod and will access query logs
    start_http_server(9123)
    REGISTRY.register(JsonCollector('http://localhost:25000/queries?json'))

    while True:
        time.sleep(1)
