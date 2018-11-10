import urllib.request
import urllib.parse
from http.client import RemoteDisconnected
from urllib.error import URLError
from elasticsearch import Elasticsearch
from pprint import pprint
from tqdm import tqdm

conf_ = {
    "host": "localhost",
    "port": 9200,
    "index": "twitter-*",
    "doc_type": "doc",
}

headers = {
    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:47.0) Gecko/20100101 Firefox/47.0",
}

def removeAllQuery(url):
    return urllib.parse.urlunparse(urllib.parse.urlparse(url)._replace(query=None))

def resolveUrlsAndUpdateElasticsearchIndex(document, es):
    if "expandURLs" in document["_source"]:
        print("already resolved!")
        return 1
    document_id = document["_id"]
    expandUrls = []
    for url in document["_source"]["entities"]["urls"]:
        shortUrl = url["expanded_url"]
        try:
            request = urllib.request.Request(url=shortUrl, headers=headers)
            with urllib.request.urlopen(request) as f:
                expandUrl = f.geturl()
                expandUrlWithoutQuery = removeAllQuery(expandUrl)
                expandUrls.append(expandUrlWithoutQuery)
        except urllib.error.HTTPError as err:
            print(err, ": access to ", shortUrl)
            return 1
        except RemoteDisconnected as err:
            print(err, ": remote disconnected from ", shortUrl)
            return 1
        except URLError as err:
            print(err, ": invalid URL ", shortUrl)
            return 1
    if expandUrls:
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
                        "gte": "2018-11-05T00:00:00Z",
                        "lt": "2018-11-11T00:00:00Z"
                    }
                }
            },
            "size": 10000
        }
    )

    for document in tqdm(response["hits"]["hits"]):
        resolveUrlsAndUpdateElasticsearchIndex(document, es)
