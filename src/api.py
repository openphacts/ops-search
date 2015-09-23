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
import urllib

mimerender.register_mime("turtle", ("text/turtle","text/n3"))
mimerender.register_mime("rdfxml", ("application/rdf+xml", "application/xml"))
mimerender.register_mime("nt", ("application/n-triples",))
#mimerender.register_mime("jsonld", ("application/ld+json",))
produces = mimerender.BottleMimeRender()

conf = {}
uri_map = {}
#all_uri_list = []
map_uri_url = None

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
        search = {
                     "query": {
                         "query_string": {
                             "query": query,
                             "default_operator": "AND"
                         },
                     },
                     "size": limit,
                 }
    else:
        search = {
                     "query" : {
                         "filtered" : { 
                             "query" : {
                                 "query_string" : {
                                     "query": query,
		                     "default_operator": "AND"
                                 } 
                             },
                             "filter" : {
                                 "type" : { 
                                     "value": ops_type
                                 }
                             }
                         }
                     },
                     "size": limit
                 }
    #TODO use IMS to remove duplicates
    return elasticsearch().search(index=branch, body = search)

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
    already_added_uris = []
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
        # fetch the equivalent uris for this one
        check_map_uri(hit["_id"])
        # is this hot equaivalent to another one found in this search
        for uri in already_added_uris:
          if hit["_id"] in uri_map[uri]:
            # add the data for this one to the existing record
            source = hit["_source"]
            score = hit["_score"]
            ops_type = hit["_type"]
            source["@ops_type"] = ops_type
            hits.append(source)
        already_added_uris.append(hit)
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
    # keep track of what results are in the json response
    already_added_uris = []
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
      if not hit["_id"] in already_added_uris:
        source = hit["_source"]
        score = hit["_score"]
        ops_type = hit["_type"]
        source["@score"] = score
        source["@ops_type"] = ops_type
        hits.append(source)
        add_mapped_uris_for_uri(already_added_uris, hit["_id"])
      else:
        # we have already added data for something this maps to
	# so grab the metadata and add it to that record
        add_to_existing_record(hits)
    return json

def add_to_existing_record(hits, hit):
    return True

def check_map_uris(hit):
    # if we don't have the equivalent uris for this one then downloed them
    if not hit in uri_map:
      print('mapping ' + hit["_id"])
      # grab all the mapped uris for this one
      map_uris(hit)

def map_uris(uri):
    params = urllib.parse.urlencode({'Uri': uri})
    req_url = map_uri_url + "?%s" %params
    print('request url: ' + req_url)
    req = urllib.request.Request(req_url)
    req.add_header('Accept', 'application/json')
    resp = urllib.request.urlopen(req)
    JSON_response = json.loads(resp.read().decode())
    uri_map["uri"] = []
    #all_uri_list.append(uri)
    for mapped_uri in JSON_response["Mapping"]["targetUri"]:
        #print(uri)
        #all_uri_list.append(mapped_uri)
        uri_map["uri"].append(mapped_uri)

def main(config_file, port="8839", *args):
    global conf
    global map_uri_url
    with open(config_file) as f:
        conf = yaml.load(f)
        map_uri_url = conf.get("map_uri_url")
    run(host='localhost', port=int(port), reloader=True)

if __name__ == "__main__":
   main(*sys.argv[1:])
