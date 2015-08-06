#!/usr/bin/env python3

from bottle import route, run, Bottle, get, post, request, response, static_file, url
from urllib.parse import quote
import os.path
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
import yaml
import sys
import json
from rdflib import Graph, plugin
import mimerender
import cgi

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
    g = Graph().parse(data=json.dumps(doc), format="json-ld", publicID=request.url)
    return g.serialize(format=format)

def json_pretty(doc):
    return json.dumps(doc, indent=4, sort_keys=True)

def html_pre(json):
    template = """<!DOCTYPE html><html><body>
    <pre>
%s
    </pre>
    </body></html>
    """
    return template % cgi.escape(json_pretty(json))

@get("/search")
@get("/search/<query>")
@produces(
    default = "json",
    #json = lambda **doc: doc,
    json = lambda **doc: json_pretty(doc),
    jsonld = lambda **doc: json.dumps(doc),
    html = lambda **doc: html_pre(doc),
    turtle = lambda **doc: render_rdf(doc, "turtle"),
    rdfxml = lambda **doc: render_rdf(doc, "xml"),
    nt = lambda **doc: render_rdf(doc, "nt")
)
def search_json(query=None):
    if query is None:
        # Get from ?q parameter instead, if exist
        query = request.query.q
    id = quote(url("/search/<query>", query=query))
    response.set_header("Content-Location", id)
    # CORS header
    response.set_header("Access-Control-Allow-Origin", "*")
    json = { "@context": {"@vocab": "http://example.com/"}, "@id": id, "query": query, "hits": [] }
    hits = json["hits"]
    search = es_search(query)
    json["total"] = search["hits"]["total"]
    for hit in search["hits"]["hits"]:
        source = hit["_source"]
        score = hit["_score"]
        ops_type = hit["_type"]
        source["@score"] = score
        source["@ops_type"] = ops_type
        hits.append(source)
    return json

@post("/search")
@produces(
    default = "json",
    #json = lambda **doc: doc,
    json = lambda **doc: json_pretty(doc),
    jsonld = lambda **doc: json.dumps(doc),
    html = lambda **doc: html_pre(doc),
    turtle = lambda **doc: render_rdf(doc, "turtle"),
    rdfxml = lambda **doc: render_rdf(doc, "xml"),
    nt = lambda **doc: render_rdf(doc, "nt")
)
def search_json_post(query=None):
    query = request.json["query"]["query_string"]["query"]
    id = quote(url("/search/<query>", query=query))
    response.set_header("Content-Location", id)
    # CORS header
    response.set_header("Access-Control-Allow-Origin", "*")
    json = { "@context": {"@vocab": "http://example.com/"}, "@id": id, "query": query, "hits": [] }
    hits = json["hits"]
    search = es_search(query)
    json["total"] = search["hits"]["total"]
    for hit in search["hits"]["hits"]:
        source = hit["_source"]
        score = hit["_score"]
        ops_type = hit["_type"]
        source["@score"] = score
        source["@ops_type"] = ops_type
        hits.append(source)
    return json

def main(config_file, port="8839", *args):
    global conf
    with open(config_file) as f:
        conf = yaml.load(f)
    run(host='localhost', port=int(port), reloader=True)

if __name__ == "__main__":
   main(*sys.argv[1:])
