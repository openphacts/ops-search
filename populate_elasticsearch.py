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

import yaml
from urllib.request import FancyURLopener
from urllib.parse import urljoin, quote, urlencode, urlencode, urlencode, urlencode

REPORT_EVERY=1200


with open("config.yaml") as f:
    conf = yaml.load(f)


def uri_to_qname(uri):
    for p,u in conf.get("prefixes", {}).items():
        if uri.startswith(u):
            return uri.replace(u, p+":", 1)
    return uri

def sparql_prefixes():
    sparql=[]
    for p,u in conf.get("prefixes", {}).items():
        sparql.append("PREFIX %s: <%s>" % (p,u))
    return "\n".join(sparql)

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


class Populater:

    es_host = conf.get("elasticsearch", {}).get("host", "localhost")
    es = Elasticsearch(host=es_host)

    urlOpener = FancyURLopener()
    urlOpener.addheader("Accept", "application/sparql-results+json, applicaton/json;q=0.1")

    def __init__(self, index, doc_type):
        self.index = index
        self.doc_type = doc_type
        self.conf = conf["indexes"][index][doc_type]
        self.added = set()
        # for stats
        self.count = 0
        self.start = time.time()
        self.lastCheck = start

    def sparql():
        sparql = []
        sparql.append(sparql_prefixes())
        sparql.append("SELECT *")
        sparql.append("WHERE {")
        if "graph" in self.conf:
            sparql.append(" GRAPH <%s>" % self.conf["graph"])

        ## clever bit here

        if "graph" in self.conf:
            sparl.append(" }")
        sparql.append("}")


    def sparqlUrl(self):
        return urljoin(conf["sparql"]["uri"],
            "?" + urlencode(dict(query=self.sparql(),
                                timeout=conf["sparql"].get("timeout_s", 300))))


    def json_reader(self):
        with self.urlOpener.open(SPARQL) as jsonFile:
        #with open("substance.json", mode="rb") as jsonFile:
            bindings = ijson.items(jsonFile, "results.bindings.item")
            for binding in bindings:
    #            print(".", end="", sep="", flush=True)
                n = binding_as_doc(binding)
                if n is not None:
                    yield n


for index in conf["indexes"]:
    try:
            es.indices.delete(index=index)
    except:
            pass




#bulk(es, json_reader())
#altLabels = {}
