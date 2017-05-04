#!/usr/bin/env python3

import bottle
from bottle import hook, route, run, Bottle, get, post, request, response, static_file, url
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
import html
import re

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
    es = Elasticsearch(es_hosts)
    return es

def es_search(query, branch, ops_type, limit):
    if ops_type is None:
        ops_type = "_all"
    search = {
        "query": {
          "multi_match": {
            "query":    query,
            "fields": [ "label^2", "prefLabel^2", "description", "altLabel", "Synonym", "Definition" ],
            "fuzziness": 1
        }
      },
      "size": limit
    }
    return elasticsearch().search(index=branch, doc_type=ops_type, body = search)

@hook('after_request')
def enable_cors():
    response.headers['Access-Control-Allow-Origin'] = '*'

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
        branch = request.query.b
        limit = request.query.l
        ops_type = request.query.t
    id = quote(url("/search/<query>", query=query))
    response.set_header("Content-Location", id)
    # CORS header
    response.set_header("Access-Control-Allow-Origin", "*")
    json = { "@context": {"@vocab": "http://example.com/"}, "@id": id, "query": query, "hits": [] }
    hits = json["hits"]
    if limit == "":
        limit = "25"
    if ops_type == "":
        ops_type = None
    search = es_search(query, branch, ops_type, limit)
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
    limit = request.json["limit"]
    branch = request.json["branch"]
    ops_type = request.json["type"]
    id = quote(url("/search/<query>", query=query))
    response.set_header("Content-Location", id)
    # CORS header
    response.set_header("Access-Control-Allow-Origin", "*")
    json = { "@context": {"@vocab": "http://example.com/"}, "@id": id, "query": query, "hits": [] }
    hits = json["hits"]
    if limit == "":
        limit = "25"
    if ops_type == "":
        ops_type = None
    if branch == "":
      branch = None
    search = es_search(query, branch, ops_type, limit)
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
    ws_host = conf["webservice"]["host"]
    ws_port = conf["webservice"]["port"]
    run(host=ws_host, port=int(ws_port), reloader=True)

if __name__ == "__main__":
   main(*sys.argv[1:])

application = bottle.default_app()
