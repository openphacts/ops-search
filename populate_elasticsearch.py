#!/usr/bin/env python3
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import json
import time
import queue


class IterableQueue(Queue): 

    _sentinel = object()

    def __iter__(self):
        return iter(self.get, self._sentinel)

    def close(self):
        self.put(self._sentinel)

q = queue.IterableQueue(maxsize=1000)

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

REPORT_EVERY=100

def json_object_hook(node):
    global count, lastCheck
    if not "substance" in node or node["substance"] is None:
        return node # Ignore
#    print node
    # now, assume blindly it's in sparql JSON format, e.g.
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
	print "n=%s, speed=%0.2f /sec" % (count, speed)
        lastCheck = now
    uri = node["substance"]["value"]
    substanceType = uri_to_qname(node["substanceType"]["value"])
    label = node["label"]["value"]
    prefLabel = node["label"]["value"]
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
	q.put(body)
        #es.index(index="chembl19", doc_type="substance", id=uri, body=body)
	#print "+", uri
        altLabels[uri] = set([altLabel])
    else:
        labels = altLabels[uri]
	if altLabel in labels:
		print "Something else changed", uri
		return None
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
	#print "*", uri
        q.put(body)


elasticsearch.helpers.bulk(es, q)


with open("substance.json") as jsonFile:
    json.load(jsonFile, object_hook=json_object_hook)

q.close()
q.join()


