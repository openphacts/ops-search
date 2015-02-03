#!/usr/bin/env python3
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import json
import time
try:
    import ijson.backends.yajl2 as ijson
except ex:
    print("Can't find yajl2, JSON parsing will be slower")
    import ijson

from urllib.request import FancyURLopener
from urllib.parse import urljoin, quote, urlencode, urlencode, urlencode, urlencode

ENDPOINT="http://ops2.few.vu.nl:3030/chembl19/sparql"
TIMEOUT=30*60 # 30 minutes
QUERY="""
PREFIX chembl: <http://rdf.ebi.ac.uk/terms/chembl#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX dc: <http://purl.org/dc/elements/1.1/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>


SELECT *
WHERE {
  GRAPH <http://rdf.ebi.ac.uk/terms/chembl> {
    ?substanceType a owl:Class.
    ?substanceType rdfs:subClassOf+ chembl:Substance .
  }  
  ?substance a ?substanceType.
  ?substance rdfs:label ?label .
  OPTIONAL { ?substance dct:title ?title }
  OPTIONAL { ?substance dc:title ?title2 }      
  OPTIONAL { ?substance rdfs:description ?desc } 
  OPTIONAL { ?substance skos:prefLabel ?prefLabel } 
  OPTIONAL { ?substance skos:altLabel ?altLabel }   
}
"""


SPARQL = urljoin(ENDPOINT, 
            "?" + urlencode(dict(query=QUERY, timeout=TIMEOUT)))


es = Elasticsearch()# host="elasticsearch")
try:
        es.indices.delete(index="chembl19")
except:
        pass

altLabels = {}

def uri_to_qname(uri):
    return uri.replace("http://rdf.ebi.ac.uk/terms/chembl#", "chembl:")


count = 0
start = time.time()
lastCheck = start

REPORT_EVERY=1200

def binding_as_doc(node):
    global count, lastCheck
    # now, assume it's in sparql JSON format, e.g.
#{
#    "substanceType": { "type": "uri" , "value": "http://rdf.ebi.ac.uk/terms/chembl#Antibody" } ,
#    "substance": { "type": "uri" , "value": "http://rdf.ebi.ac.uk/resource/chembl/molecule/CHEMBL1201579" } ,
#    "label": { "type": "literal" , "value": "CAPROMAB PENDETIDE" } ,
#    "prefLabel": { "type": "literal" , "value": "CAPROMAB PENDETIDE" } ,
#    "altLabel": { "type": "literal" , "value": "CAPROMAB PENDETIDE" }
#} 
    count += 1
    if (count % REPORT_EVERY == 0):
        now = time.time()
        speed = REPORT_EVERY/(now-lastCheck)
        avgSpeed = count/(now-start)
        print("n=%s, speed=%0.1f/sec, avgSpeed=%0.1f/sec" % (count, speed, avgSpeed))
        lastCheck = now
    uri = node["substance"]["value"]
    substanceType = uri_to_qname(node["substanceType"]["value"])
    label = node["label"]["value"]
    prefLabel = node["label"]["value"]
    if not "altLabel" in node:
    #    print("Straaaaaaaaaange", node)
        altLabel = None
    else:
        altLabel = node["altLabel"]["value"]
    if uri not in altLabels: 
        body = {
                            "@id": uri,
                            "@type": [substanceType, "chembl:Substance"],
                            "label": label,
                            "prefLabel": prefLabel,
                            }
        if altLabel:
            body["altLabel"] = [altLabel]
        msg = {
                    "_index": "chembl19",
                    "_type": "substance",
                    "_id": uri,
                    "_source": body
                }
        altLabels[uri] = set([altLabel])
        return msg
    else:
        labels = altLabels[uri]
        if altLabel in labels:
                print("Something else changed", uri)
                return None # skip
        labels.add(altLabel)
        msg = {
            "_op_type": "update",
            "_index": "chembl19",
            "_type": "substance",
            "_id": uri,
            "doc": {
              "altLabel": list(labels)
              }
            }
        return msg


urlOpener = FancyURLopener()
urlOpener.addheader("Accept", "application/sparql-results+json, applicaton/json;q=0.1")

def json_reader():
    with urlOpener.open(SPARQL) as jsonFile:
    #with open("substance.json", mode="rb") as jsonFile:
        bindings = ijson.items(jsonFile, "results.bindings.item")
        for binding in bindings:
#            print(".", end="", sep="", flush=True)
            n = binding_as_doc(binding)
            if n is not None:
                yield n

bulk(es, json_reader())
altLabels = {}
