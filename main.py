import urllib.request
from elasticsearch import Elasticsearch
from pprint import pprint

conf_ = {
    "host": "localhost",
    "port": 9200,
    "index": "twitter-*",
    "doc_type": "doc",
}


def resolveUrlsAndUpdateElasticsearchIndex(document, es):
    if "expandURLs" in document["_source"]:
        print("already resolved!")
        return 1
    if "urls" in document["_source"]:
        document_id = document["_id"]
        expandUrls = []
        for url in document["_source"]["urls"]:
            shortUrl = url
            try:
                with urllib.request.urlopen(shortUrl) as f:
                    expandUrl = f.geturl()
                    expandUrls.append(expandUrl)
            except urllib.error.HTTPError as err:
                print(err, ": access to ", shortUrl)
                return 1
        es.index(
            index="urls",
            doc_type="doc",
            id=document_id,
            body={
                "expandURLs": expandUrls,
            }
        )
    return 0


if __name__ == '__main__':
    es = Elasticsearch("{}:{}".format(conf_["host"], conf_["port"]))

    response = es.search(
        index=conf_["index"],
        body={
            "query": {
                "range": {
                    "@timestamp": {
                        "gte": "2018-10-28T00:00:00Z",
                        "lt": "2018-11-04T00:00:00Z"
                    }
                }
            },
            "size": 10000
        }
    )

    for document in response["hits"]["hits"]:
        resolveUrlsAndUpdateElasticsearchIndex(document, es)
