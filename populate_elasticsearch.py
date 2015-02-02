#!/usr/bin/env python3
from elasticsearch import ElasticSearch
from elasticsearch.helpers import bulk
import json

es = ElasticSearch()# host="elasticsearch")
es.delete_mapping(index="chembl19", doc_type="substance")

loaded = set()

def uri_to_qname(uri):
    return uri.replace("http://rdf.ebi.ac.uk/terms/chembl#", "chembl:")


def json_object_hook(node):
    if not "substance" in node:
        return None # Ignore
    # now, assume blindly it's in sparql JSON format, e.g.
#{
#    "substanceType": { "type": "uri" , "value": "http://rdf.ebi.ac.uk/terms/chembl#Antibody" } ,
#    "substance": { "type": "uri" , "value": "http://rdf.ebi.ac.uk/resource/chembl/molecule/CHEMBL1201579" } ,
#    "label": { "type": "literal" , "value": "CAPROMAB PENDETIDE" } ,
#    "prefLabel": { "type": "literal" , "value": "CAPROMAB PENDETIDE" } ,
#    "altLabel": { "type": "literal" , "value": "CAPROMAB PENDETIDE" }
#} 

    uri = node["substance"]["value"]
    substanceType = uri_to_qname(node["substanceType"]["value"])
    label = node["label"]["value"]
    prefLabel = node["label"]["value"]
    altLabel = node["label"]["value"]
    if uri not in loaded: 
        body = {
                    "@id": uri,
                    "@type": [substanceType, "chembl:Substance"],
                    "label": label,
                    "prefLabel": prefLabel,
                    "altLabel": [altLabel] ## to allow later additions
                }
        es.index(index="chembl19", doc_type="substance", id=uri, body=body)
        loaded.add(uri)
    else:
        body = {
            "doc" {
              "altLabel": [altLabel]
              }
            }
        es.update(index="chembl19", doc_type="substance", id=uri, body=body)


with open("substance.json") as jsonFile:
    json.load(jsonFile, object_hook=json_object_hook)



