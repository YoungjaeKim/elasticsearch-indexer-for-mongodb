"""
A simple search to investigate ElasticSearch indexed data
- python search.py --config example.yaml --field 'text' --query 'this is good'
"""
from elasticsearch import Elasticsearch
import argparse
import yaml


class Parameter:
    es_endpoint: str = ""
    es_port: int = ""
    es_username: str = ""
    es_password: str = ""
    es_index_name: str = ""

    def read(self, config):
        config_elastic = config['elasticsearch']
        self.es_endpoint = str(config_elastic['endpoint'])
        self.es_port = int(config_elastic['port'])
        self.es_username = str(config_elastic['username'])
        self.es_password = str(config_elastic['password'])
        self.es_index_name = str(config_elastic['index_name'])


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", help="set configuration yaml file", type=str,
                        default='config.yaml')
    parser.add_argument("--field", help="field name to query", type=str)
    parser.add_argument("--query", help="query", type=str)
    args = parser.parse_args()

    param = Parameter()
    with open(args.config, 'rt', encoding='UTF-8') as fin:
        print("config file: {0}".format(args.config))
        yaml_text = fin.read()
        config_nodes = yaml.load(yaml_text, Loader=yaml.Loader)
        param.read(config_nodes)

    field = args.field
    query = args.query

    if not field:
        print("ERROR: no field name supplied. use '--field' argument")
        exit(1)
    if not query:
        print("WARN: no query supplied. use '--query' argument")

    es = Elasticsearch(hosts=param.es_endpoint, port=param.es_port, http_auth=(param.es_username, param.es_password))
    result = es.search(
        index=param.es_index_name,
        body={
            "query": {
                "match": {
                    field: {
                        "query": query
                    }
                }
            }
        }
    )

    for i in result['hits']['hits']:
        source = i['_source']
        print("{0}: {1}".format(source['id'], source['text']))
