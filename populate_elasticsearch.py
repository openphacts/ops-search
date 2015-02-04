#!/usr/bin/env python3
import sys
import time
import json
from urllib.request import FancyURLopener
from urllib.parse import urljoin, quote, urlencode, urlencode, urlencode, urlencode

import yaml

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

try:
    import ijson.backends.yajl2 as ijson
except ex:
    print("Can't find py-yajl, JSON parsing will be slower")
    import ijson

REPORT_EVERY=1200
DEFAULT_SPARQL_TIMEOUT=60

## TODO: Parameterize the config
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


class Indexer:

    es_hosts = conf.get("elasticsearch", None)
    es = Elasticsearch(es_hosts)

    urlOpener = FancyURLopener()
    urlOpener.addheader("Accept",
        "application/sparql-results+json, applicaton/json;q=0.1")

    def __init__(self, index, doc_type):
        self.index = index
        self.doc_type = doc_type
        self.conf = conf["indexes"][index][doc_type]
        self.indexed = set()
        self.reset_stats()
        self.properties = {}

    def reset_stats(self):
        self.count = 0
        self.start = time.time()
        self.lastCheck = self.start

    def sparql_property(self, prop):
        short = prop.split(":")[-1]
        if short not in self.properties:
            name = short
        else:
            name = prop.replace(":", "_")
            ## FIXME: Any other illegal chars for SPARQL VARNAME?
            # http://www.w3.org/TR/sparql11-query/#rVARNAME
        self.properties[name] = prop

    def sparql(self):
        sparql = []
        sparql.append(sparql_prefixes())
        sparql.append("SELECT *")
        sparql.append("WHERE {")
        if "graph" in self.conf:
            sparql.append(" GRAPH <%s> {" % self.conf["graph"])

        optionals = False
        if "type" in self.conf:
            sparql.append("   ?uri a %s ." % self.conf["type"])
            optionals = True
        else:
            sparql.append("   ?uri ")

        properties = []

        if "common_properties" in conf:
            properties.extend(conf["common_properties"])

        if "properties" in self.conf:
            properties.extend(self.conf["properties"])

        if not properties:
            raise Exception("No properties for %s %s" % (self.index, self.doc_type))

        print("Indexing properties ", properties)
        sparql.extend(map(self.sparql_property, properties))

        if "graph" in self.conf:
            sparql.append(" }")
        sparql.append("}")
        if "limit" in conf["sparql"]:
            sparql.append("LIMIT %s" % conf["sparql"]["limit"])

        sparqlStr = "\n".join(sparql)
        print(sparqlStr)
        return sparqlStr

    def sparqlURL(self):
        return urljoin(conf["sparql"]["uri"],
            "?" + urlencode(dict(query=self.sparql(),
                            timeout=conf["sparql"].get("timeout_s",
                                    DEFAULT_SPARQL_TIMEOUT))))

    def json_reader(self):
        url = self.sparqlURL()
        with self.urlOpener.open(url) as jsonFile:
        #with open("substance.json", mode="rb") as jsonFile:
            bindings = ijson.items(jsonFile, "results.bindings.item")
            for binding in bindings:
    #            print(".", end="", sep="", flush=True)
                n = binding_as_doc(binding)
                if n is not None:
                    yield n

    def binding_as_doc(self, node):
        # now, assume it's in sparql JSON format, e.g.
    #{
    #    "substanceType": { "type": "uri" , "value": "http://rdf.ebi.ac.uk/terms/chembl#Antibody" } ,
    #    "substance": { "type": "uri" , "value": "http://rdf.ebi.ac.uk/resource/chembl/molecule/CHEMBL1201579" } ,
    #    "label": { "type": "literal" , "value": "CAPROMAB PENDETIDE" } ,
    #    "prefLabel": { "type": "literal" , "value": "CAPROMAB PENDETIDE" } ,
    #    "altLabel": { "type": "literal" , "value": "CAPROMAB PENDETIDE" }
    #}
        self.count += 1
        if (self.count % REPORT_EVERY == 0):
            now = time.time()
            speed = REPORT_EVERY/(now-self.lastCheck)
            avgSpeed = self.count/(now-self.start)
            print("n=%s, speed=%0.1f/sec, avgSpeed=%0.1f/sec" % (count, speed, avgSpeed))
            self.lastCheck = now
        uri = node["substance"]["value"]
        substanceType = uri_to_qname(node["substanceType"]["value"])

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

    def load(self):
        self.reset_stats()
        bulk(self.es, self.json_reader())

def main(*args):

    for index in conf["indexes"]:
        try:
            es.indices.delete(index=index)
        except:
            pass
        for doc_type in conf["indexes"][index]:
            indexer = Indexer(index, doc_type)
            indexer.load()


if __name__ == "__main__":
    main(*sys.argv[1:])
