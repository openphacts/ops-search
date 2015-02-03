#!/usr/bin/env python3
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import json
import time
import ijson


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
        print("Straaaaaaaaaange", node)
        altLabel = None
    else:
        altLabel = node["altLabel"]["value"]
    if uri not in altLabels: 
        body = {
                    "_index": "chembl19",
                    "_type": "substance",
                    "_id": uri,
                    "_source": {
                            "@id": uri,
                            "@type": [substanceType, "chembl:Substance"],
                            "label": label,
                            "prefLabel": prefLabel,
                            "altLabel": [altLabel] ## to allow later additions
                    }
                }
        #q.put(body)
        #es.index(index="chembl19", doc_type="substance", id=uri, body=body)
        #print("+", uri)
        altLabels[uri] = set([altLabel])
        return body
    else:
        labels = altLabels[uri]
        if altLabel in labels:
                print("Something else changed", uri)
                return None # skip
        labels.add(altLabel)
        body = {
            "_op_type": "update",
            "_index": "chembl19",
            "_type": "substance",
            "_id": uri,
            "doc": {
              "altLabel": list(labels)
              }
            }
        #es.update(index="chembl19", doc_type="substance", id=uri, body=body)
        #print("*", uri)
        #q.put(body)
        return body



def json_reader():
    with open("substance.json", mode="rb") as jsonFile:
        bindings = ijson.items(jsonFile, "results.bindings.item")
        for binding in bindings:
            n = binding_as_doc(binding)
            if n is not None:
                yield n

bulk(es, json_reader())
altLabels = {}
