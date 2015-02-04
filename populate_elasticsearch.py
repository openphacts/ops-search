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

# SPARQL variable name for the resource
ID="id"
# and for the rdf:type (on subclasses)
TYPE="type"

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
        self.blanknodes = {}

    def reset_stats(self):
        self.count = 0
        self.start = time.time()
        self.lastCheck = self.start

    def variable_for_property(self, prop):
        short = prop.split(":")[-1]
        if short not in self.properties:
            name = short
        else:
            name = prop.replace(":", "_")
            ## FIXME: Any other illegal chars for SPARQL VARNAME?
            # http://www.w3.org/TR/sparql11-query/#rVARNAME
        self.properties[name] = prop
        return name

    def sparql_property(self, prop):
        return "    ?%s %s ?%s ." % (ID, prop, self.variable_for_property(prop))

    def sparql(self):
        sparql = []
        sparql.append(sparql_prefixes())
        sparql.append("SELECT *")
        sparql.append("WHERE {")

        if "graph" in self.conf:
            sparql.append(" GRAPH <%s> {" % self.conf["graph"])

        optionals = False
        if "type" in self.conf:
            sparql.append("   ?%s a %s ." % (ID, self.conf["type"]))
            optionals = True
        properties = []

        if "common_properties" in conf:
            properties.extend(conf["common_properties"])
        if "properties" in self.conf:
            properties.extend(self.conf["properties"])

        if not properties:
            raise Exception("No properties configured for %s %s" % (self.index, self.doc_type))

        print("Properties:\n  ", " ".join(properties))
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
        timeout = int(conf["sparql"].get("timeout_s",
                        DEFAULT_SPARQL_TIMEOUT)) * 1000
        return urljoin(conf["sparql"]["uri"],
            "?" + urlencode(dict(query=self.sparql(),
                                 timeout=timeout)))

    def json_reader(self):
        url = self.sparqlURL()
        with self.urlOpener.open(url) as jsonFile:
            bindings = ijson.items(jsonFile, "results.bindings.item")
            for binding in bindings:
                print(binding)
                n = self.binding_as_doc(binding)
                if n is not None:
                    print(n)
                    yield n

    def skolemize(self, bnode):
        if bnode in self.blanknodes:
            return self.blanknodes[bnode]
        uri = uuid.uuid4().urn
        self.blanknodes[bnode] = uri
        return uri

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


        if node[ID]["type"] == "uri":
            uri = node[ID]["value"]
        else:
            # a blank node, we need a new uri
            uri = self.skolemize(node[ID]["value"])

        body = { "@id": uri }
        ## TODO: IRI-ize any URIs with ASCII escapes, like
        # http://dbpedia.org/resource/%C3%81ngel_Gim%C3%A9nez
        # so they are also searchable
        types = []
        if "type" in self.conf:
            types.append(self.conf["type"])
        if TYPE in node:
            types.append(node[TYPE])
        if types:
            body["@type"] = types

        for var in node:
            if var in (ID,TYPE):
                continue
            if node[var] is None or node.get("value") is None:
                continue
            body[var] = node[var]

        ## TODO: Make mapping for JSON-LD

        # Return ElasticSearch bulk message
        doc_id = uri
        self.indexed.add(uri)
        # FIXME: What if it's already indexed? Need
        # child documents?
        msg = { "_index": self.index,
                "_type": self.doc_type,
                "_id": doc_id,
                "_source": body
              }

        return msg

    def load(self):
        print("Index %s type %s" % (self.index, self.doc_type))
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
