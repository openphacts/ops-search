#!/usr/bin/env python3

from bottle import route, run, Bottle, get, post, request, response, static_file
import os.path
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
import yaml
import sys
import json
from rdflib import Graph, plugin
import mimerender

mimerender.register_mime("turtle", ("text/turtle","text/n3"))
mimerender.register_mime("rdfxml", ("application/rdf+xml", "application/xml"))
mimerender.register_mime("nt", ("application/n-triples",))
#mimerender.register_mime("jsonld", ("application/ld+json",))
produces = mimerender.BottleMimeRender()

conf = {}

def find_static():
    cwd = None
    try:
        cwd = os.path.dirname(__file__)
    except NameError:
        cwd = "."
    return os.path.abspath(os.path.join(cwd, "static"))

static_root = find_static()


es=None
def elasticsearch():
    global es
    if es is not None:
        return es
    es_hosts = conf.get("elasticsearch")
    print(es_hosts)
    es = Elasticsearch(es_hosts)
    return es

def es_search(query):
    search = { "query": {
                  "query_string": {
                          "query": query,
                          "default_operator": "AND"
                        },
                    },
                    "size": 25,
             }
    return elasticsearch().search(body = search)

@get("/")
def index():
    return static_file("index.html", static_root)

def render_rdf(doc, format):
    g = Graph().parse(data=json.dumps(doc), format="json-ld")
    return g.serialize(format=format)


@get("/search/:query")
@produces(
    default = "json",
    #json = lambda **doc: doc,
    json = lambda **doc: json.dumps(doc, indent=4, sort_keys=True),
    jsonld = lambda **doc: json.dumps(doc),
    html = lambda **doc: "<pre>%s</pre>" % doc,
    turtle = lambda **doc: render_rdf(doc, "turtle"),
    rdfxml = lambda **doc: render_rdf(doc, "xml"),
    nt = lambda **doc: render_rdf(doc, "nt")
)
def search_json(query):
    json = { "@context": {"@vocab": "http://example.com/"}, "@id": "/search/%s" % query, "query": query, "hits": [] }
    hits = json["hits"]
    search = es_search(query)
    for hit in search["hits"]["hits"]:
        hits.append(hit["_source"])
        #hits.append({"@id": hit["_id"]})
    return json


def main(config_file, *args):
    global conf
    with open(config_file) as f:
        conf = yaml.load(f)
    run(host='localhost', port=8839, reloader=True)

if __name__ == "__main__":
   main(*sys.argv[1:])
