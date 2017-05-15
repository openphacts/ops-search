#!/usr/bin/env python3

import bottle
from bottle import hook, route, run, Bottle, get, post, request, response, static_file, url
from urllib.parse import quote
import os.path
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q
from elasticsearch_dsl.query import MultiMatch, Match
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

def es_search(query_string, branch, ops_type, limit):
    s = Search(using=elasticsearch(), index=(branch), doc_type=ops_type)
    s = s[0:int(limit)]
    q = Q('multi_match', query=query_string, fields=['label^2', 'title^2', 'prefLabel^2', 'identifier', 'description', 'altLabel', 'Synonym', 'Definition'], fuzziness=1, type='best_fields')
    s = s.highlight('label', 'title', 'identifier', 'description', 'prefLabel', 'description', 'altLabel', 'Synonym', 'Definition')
    s = s.query(q)
    es_response = s.execute()
    return es_response.to_dict()

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

@get("/indexes")
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
def index_info():
    response.content_type = 'application/json'
    response.set_header("Access-Control-Allow-Origin", "*")
    indexes = []
    for index in conf.get("indexes"):
      indexes.append(index)
    return {"indexes": indexes}

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
        query = request.query.query
        branch = request.query.getall("branch")
        limit = request.query.limit
        ops_type = request.query.type
        options = request.query.getall("options")
    id = quote(url("/search/<query>", query=query))
    response.set_header("Content-Location", id)
    # CORS header
    response.set_header("Access-Control-Allow-Origin", "*")
    if limit == "":
        limit = "25"
    if ops_type == "":
        ops_type = None
    if ops_type != None and ops_type not in conf["indexes"]:
        response.status = 422
        response.content_type = 'application/json'
        return json.dumps({'error': 'Branch is not available for searching'})
    search = es_search(query, branch, ops_type, limit)
    if ops_type == None:
        search["type"] = "_all"
    else:
        search["type"] = ops_type
    if branch == "":
        search["branch"] = "_all"
    else:
       search["branch"] = branch
    if options != None and "uris_only" in options:
      uris = []
      for hit in search["hits"]["hits"]:
          uris.append(hit["_id"])
      return {"uris": uris}
    else:
      search.pop("_shards", None)
      return search

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
    postdata = request.body.read()
    query = None
    limit = None
    ops_type = None
    branch = None
    options = None
    if "query" in request.json:
        query = request.json["query"]
    if "limit" in request.json:
        limit = request.json["limit"]
    if "branch" in request.json:
        branch = request.json["branch"]
    if "type" in request.json:
        ops_type = request.json["type"]
    if "options" in request.json:
        options = request.json["options"]
    response.set_header("Content-Location", id)
    # CORS header
    response.set_header("Access-Control-Allow-Origin", "*")
    if limit == None:
        limit = "25"
    search = es_search(query, branch, ops_type, limit)
    if ops_type == None:
        search["type"] = "_all"
    else:
        search["type"] = ops_type
    if branch == None:
        search["branch"] = "_all"
    else:
       search["branch"] = branch
    if options != None and "uris_only" in options:
      uris = []
      for hit in search["hits"]["hits"]:
          uris.append(hit["_id"])
      return {"uris": uris}
    else: 
      search.pop("_shards", None)
      return search

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
