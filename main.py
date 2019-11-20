"""
a MongoDB to ElasticSearch index feeder.
- WARNING: This script contains `eval()` function to convert query input to MongoDB-aware query. Use it at your own risk.

Usage example
  $ python main.py --config test.yaml --collection sample
"""
import argparse
import json
import os

import pymongo
import datetime
from uuid import UUID
import jsonpath_ng
import yaml
from bson import ObjectId
from timeit import default_timer as timer
from datetime import timedelta
from itertools import islice, chain
from elasticsearch import Elasticsearch


class Parameter:
    enable: bool = False
    mongodb_query: str = ""
    mongodb_url: str = ""
    mongodb_database: str = ""
    mongodb_collection: str = ""
    es_port: str = ""
    es_endpoint: str = ""
    es_username: str = ""
    es_password: str = ""
    es_index_name: str = ""
    field_key: str = "_id"
    contents: [{}] = []
    chunk_size: int = 50

    def read(self, config):
        self.enable = bool(config['enable'])
        self.chunk_size = int(config['chunk_size'])
        # MongoDB
        config_mongo = config['mongodb']
        self.mongodb_url = str(config_mongo['url'])
        self.mongodb_database = str(config_mongo['database'])
        self.mongodb_collection = str(config_mongo['collection'])
        self.mongodb_query = str(config_mongo['query'])
        # ElasticSearch
        config_es = config['elasticsearch']
        self.es_endpoint = str(config_es['endpoint'])
        self.es_port = str(config_es['port'])
        self.es_username = str(config_es['username'])
        self.es_password = str(config_es['password'])
        self.es_index_name = str(config_es['index_name'])
        # Fields
        config_field = config['field']
        self.field_key = str(config_field['key'])
        self.contents = config_field['contents']


def mongo_to_elasticsearch(mongo_docs, contents: []) -> (int, {}):
    """
    Mongo document to ElasticSearch request body conversion logic
    :param contents: a list of {name, jsonpath} to map Mongo document to ElasticSearch content
    :param mongo_docs: MongoDB documents
    :return: (item count, a request body for ElasticSearch indexer)
    """
    count = 0
    feeds = []
    for doc in mongo_docs:
        json_str_doc = json.dumps(doc, cls=MongoDbEncoder)  # convert Mongo-specific data to encoded values
        json_doc = json.loads(json_str_doc)

        # extract contents from MongoDB document
        content = {}
        for p in contents:
            value = jsonpath_ng.parse(p["jsonpath"]).find(json_doc)
            if len(value) == 1:
                content[p["name"]] = value[0].value
            else:
                print("WARN: [{0}:{1}] has {2} result for document id '{3}' - content ignored"
                      .format(p["name"], p["jsonpath"], len(value), doc['_id']))

        # add item to ElasticSearch request body. https://stackoverflow.com/a/53614667/361100
        feeds.append({'index': {'_id': doc['_id']}})
        feeds.append(content)

        count += 1
        print("#{0} added '{1}' to request body".format(count, doc['_id']))
    return count, feeds


def iter_in_slices(iterator, size=None):
    """
    Change generator results into size of chunks
    https://stackoverflow.com/a/44320132/361100
    """
    while True:
        slice_iter = islice(iterator, size)
        # If no first object this is how StopIteration is triggered

        try:
            peek = next(slice_iter)
        except StopIteration:
            yield None
        # Put the first object back and return slice
        yield chain([peek], slice_iter)


class MongoDbEncoder(json.JSONEncoder):
    """
    a custom encoder for MongoDB to json.dumps()
    https://stackoverflow.com/a/48159596/361100
    """

    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.astimezone().strftime("%Y-%m-%dT%H:%M:%S.%f%z")
        if isinstance(obj, UUID):
            # if the obj is uuid, we simply return the value of uuid
            return obj.hex
        return json.JSONEncoder.default(self, obj)


if __name__ == '__main__':
    start_time: float = timer()

    # region Parameter setup
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", help="set configuration yaml file", type=str,
                        default='config.yaml')
    parser.add_argument("--database", help="MongoDB database name", type=str)
    parser.add_argument("--collection", help="MongoDB collection name", type=str)
    parser.add_argument("--query", help="MongoDB query", type=str)
    parser.add_argument("--es_username", help="ElasticSearch username", type=str)
    parser.add_argument("--es_password", help="ElasticSearch password", type=str)
    parser.add_argument("--es_index", help="ElasticSearch index name", type=str)
    args = parser.parse_args()

    if not os.path.isfile(args.config):
        raise ValueError("ERROR: no config file exists at {0}".format(args.config))

    param = Parameter()
    with open(args.config, 'rt', encoding='UTF-8') as fin:
        print("config file: {0}".format(args.config))
        yaml_text = fin.read()
        config_nodes = yaml.load(yaml_text, Loader=yaml.Loader)
        param.read(config_nodes)

    if args.database:
        param.mongodb_database = args.database
    if args.collection:
        param.mongodb_collection = args.collection
    if args.query:
        param.mongodb_query = args.query
    if args.es_username:
        param.es_username = args.es_username
    if args.es_password:
        param.es_password = args.es_password
    if args.es_index:
        param.es_index_name = args.es_index

    mongo_client = pymongo.MongoClient(param.mongodb_url)
    mongo_database = mongo_client[param.mongodb_database]
    mongo_collection = mongo_database[param.mongodb_collection]

    query = eval(param.mongodb_query)  # security warning! use at your own risk.
    documents = mongo_collection.find(query)
    # endregion Parameter setup

    documents_chunk = iter_in_slices(documents, param.chunk_size)
    es = Elasticsearch(hosts=param.es_endpoint, port=param.es_port, http_auth=(param.es_username, param.es_password))

    try:
        check_iterable = iter(documents_chunk)
        for x in documents_chunk:
            count, result = mongo_to_elasticsearch(x, param.contents)
            print("start uploading {0} items to ElasticSearch...".format(count))
            response = es.bulk(index=param.es_index_name, doc_type='_doc', body=result)
            print("-response (Success: {0})".format(str((response['errors'] is False))))
    except TypeError:  # None type returned
        print("document ends.")

    end_time: float = timer()
    print("main process finished. ({0} elapsed)".format(timedelta(seconds=end_time - start_time)))
    exit(0)
