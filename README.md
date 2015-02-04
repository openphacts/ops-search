# openphacts-irs
IRS2 data loading scripts

Populates an [ElasticSearch](http://www.elasticsearch.org) instance with [JSON-LD])(http://www.w3.org/TR/json-ld/) 
documents with searceable labels extracted from [SPARQL queries])(http://www.w3.org/TR/sparql11-query/) from
a configured [SPARQL service])(http://www.w3.org/TR/sparql11-protocol/).


## License
License: [MIT license](http://opensource.org/licenses/MIT)

(c) 2014-2015 University of Manchester, UK

See [LICENSE](LICENSE) for details.

The additional Python libraries used are:

* [elasticsearch](https://pypi.python.org/pypi/elasticsearch/) ([Apache License 2.0](http://www.apache.org/licenses/LICENSE-2.0))
* [ijson](https://pypi.python.org/pypi/ijson/) ([BSD license](https://github.com/isagalaev/ijson/blob/master/LICENSE.txt))
* [PyYaml](https://pypi.python.org/pypi/pyaml/) ([WTFPL license](https://github.com/mk-fg/pretty-yaml/blob/master/COPYING))
* [ijson](https://pypi.python.org/pypi/ijson/) ([BSD license](https://github.com/isagalaev/ijson/blob/master/LICENSE.txt))
* [yajl](https://pypi.python.org/pypi/yajl) (optional) ([BSD license?](https://github.com/rtyler/py-yajl/issues/28))


## Installation

You will need Python 3 and pip, in addition to some dependencies.

In Ubuntu 14.04, this easiest achieved using:

    sudo apt-get install git python3-pip libyajl2 python3-yaml 
    sudo pip3 install elasticsearch ijson yajl

You will also need an [ElasticSearch](http://www.elasticsearch.org) installation. You can test it out with [Docker](https://www.docker.com/):

    docker run --name elasticsearch -d -p 9200:9200 dockerfile/elasticsearch

You can test this at: http://localhost:9200/_search?q=alice

## Running

After modifying the [configuration](#Configuration), simply run:

    python3 sparql_index.py

## Configuration

You need to modify [config.yml](config.yml) to configure the ElasticSearch data loading. The configuration 
file is read from the current directory when running the script.

### ElasticSearch

The default configuration is:

    elasticsearch:
        - host: localhost
          port: 9200

Multiple hosts can be given to address the cluster:

    elasticsearch:
        - host: server1
        - host: server2
          port: 9201
        - host: server3

[Additional parameters](http://elasticsearch-py.readthedocs.org/en/master/api.html#elasticsearch) like `use_ssl` may be provided as supported by the ElasticSearch Python library.


### Prefixes

A series of namespace prefixes and their URIs should be defined, 
these will be used both within the generated SPARQL queries 
and in the generated JSON-LD `@context`.

    prefixes:
      rdfs: http://www.w3.org/2000/01/rdf-schema#
      owl: http://www.w3o.rg/2002/07/owl#
      dct: http://purl.org/dc/terms/
      dc: http://purl.org/dc/elements/1.1/
      skos: http://www.w3.org/2004/02/skos/core#
      foaf: http://xmlns.com/foaf/0.1/

### SPARQL server

The `uri` of a [SPARQL endpoint])(http://www.w3.org/TR/sparql11-protocol/) to query:

    sparql:
      uri: http://localhost:8890/sparql
      timeout_s: 7200 # e.g. 2 hours

TODO: Support authentication?

### Common properties

The optional `common_properties` specifies any common properties that will
always be indexed:

    common_properties:
      - rdfs:label

Properties MUST be given as a qname using
one of the defined [prefixes](#Prefixes).


### Indexes

Each key under `indexes` specified will (re)create the corresponding [ElasticSearch index](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/docs-index_.html).

#### Types

Each index can contain multiple types. The below example creates the two indexes `customers` and `staff`, where the first index has two types, `orgs` and `people`, and the second only the type `people`. 

    indexes:
        customers:
            orgs:
                type: foaf:Organization
            people:
                graph: http://example.com/customers
                type: foaf:Person
        staff:
            people:
                graph: http://example.com/staff
                type: foaf:Person

Each index-type combination can specify multiple properties for defining what to index - as detailed below:

#### Graph

A typical configuration for indexing multiple graphs is to have one ElasticSearch index per graph, as shown in the example above.

    graph: http://example.com/customers
    
This specifies the name of the `GRAPH` to query. Note that it is always the configured `sparql` endpoint that is contacted, so
this URI is not retrieved. 

If the `graph` key is missing, the default graph of the SPARQL endpoint is searched instead. 

#### Type

    type: foaf:Person

The `rdf:type` of triples to index. Typically one ElasticSearch type corresponds to one RDF type.

If `type` is not specified, all resources in the graph with the given properties are indexed. 
In this case, all properties are required to be present.

#### Subclasses

    subclasses: owl
    
If the resources in the graph are only typed as subclasses of `type` in the graph, then specifying `subclasses` will modify the SPARQL query to select for subclasses instead. You can specify different mechanisms, which will generate corresponding SPARQL fragments:

    - direct
        ?uri a ?subClass .
        ?subClass rdfs:subClassOf ?type .
    - owl
        ?uri a ?subClass .
        ?subClass a owl:Class .
        ?subClass rdfs:subClassOf+ ?type .
    
Note that the `rdfs:subClassOf` and `owl:Class` statements (e.g. loaded from an ontology) must be within the same `graph`. 

#### Properties

The RDF properties to index:

    properties:
      - dct:title
      - rdfs:description
      - skos:prefLabel
      - skos:altLabel

Properties MUST be given as a qname using
one of the defined [prefixes](#Prefixes).

Any [common properties](#Common properties) are added to this list.

If a `type` was specified, then in the the generated query the properties are individually made `OPTIONAL`.

Within the indexed JSON documents the property will be given with the local name of the property (e.g. `title` and `altLabel`), or as in the configuration if an earlier property in the list has a conflicting name (e.g. `dc:title` `dct:title` would be indexed as `title` and `dct:title`).

