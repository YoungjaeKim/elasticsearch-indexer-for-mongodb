# elasticsearch-indexer-for-mongodb

This script fetches documents from MongoDB and inserts them to [ElasticSearch](https://www.elastic.co/kr/).

There are three benefits in this script;
1. It uploads with a `chunk_size` of Mongo items. 
2. It embraces json-path to selectively extract index items from a Mongo data. JsonPath can be get by [VSCode extension](https://marketplace.visualstudio.com/items?itemName=richie5um2.vscode-statusbar-json-path) easily.
3. It can be easily managed by yaml file and commandline arguments.

## How to use

```bash
$ python main.py
$ python main.py --database 'wiki' --collection 'news' --query '{"Attendee": None}'
```

Supported arguments are listed as below;
- `config`: a yaml filename. By default, `config.yaml` is used.
- `query`: a MongoDB query to fetch
- `database`: a MongoDB database name
- `collection`: a MongoDB collection name
- `es_username`: a ElasticSearch username
- `es_password`: a ElasticSearch password
- `es_index`: a ElasticSearch index name

In `config.yaml` file.
```yaml
enable: True
chunk_size: 50
mongodb:
  url: "mongodb://10.0.0.48:27017/"
  database: "sample"
  collection: "record"
  query: '{}'
elasticsearch:
  endpoint: "http://your.elasticsearch.host.com"
  port: 10200
  username: your_username
  password: your_password
  index_name: "example"
field:
  key: '_id'
  contents:  # a value map of { ElasticSearch contents key: MongoDB document jsonPath}
    - { name: "id", jsonpath: "_id" }
    - { name: "text", jsonpath: "Parameters[0].ParameterData.RawText" }
```
