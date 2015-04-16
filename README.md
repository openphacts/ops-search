# openphacts-irs
IRS2 data loading scripts

Populates an [ElasticSearch](http://www.elasticsearch.org) instance with [JSON-LD](http://www.w3.org/TR/json-ld/)
documents with searchable labels extracted from [SPARQL queries](http://www.w3.org/TR/sparql11-query/) from
a configured [SPARQL service](http://www.w3.org/TR/sparql11-protocol/).


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
* bottle
* mimerender
* rdflib
* rdflib-jsonld

## Installation

You will need Python 3 and pip, in addition to some dependencies.

In Ubuntu 14.04, this easiest achieved using:

    sudo apt-get install git python3-pip libyajl2 python3-yaml python3-bottle
    sudo pip3 install elasticsearch ijson yajl mimerender rdflib rdflib-jsonld

You will also need an [ElasticSearch](http://www.elasticsearch.org) installation. You can test it out with [Docker](https://www.docker.com/):

    docker run --name elasticsearch -d -p 9200:9200 dockerfile/elasticsearch

You can test this at: [http://localhost:9200/_search?q=alice](http://localhost:9200/_search?q=alice)

## Running

To run using the [configuration](#Configuration) in `example.yaml`, do:

    python3 populate_elasticsearch.py example.yaml


## Configuration

You need to create a config file similar to
[example.yaml](example.yaml) to configure the ElasticSearch data loading.

A description of each element of the configuration follows below:

### ElasticSearch

The default configuration is:

```yaml
    elasticsearch:
        - host: localhost
          port: 9200
```

Multiple hosts can be given to address the cluster:


```yaml
    elasticsearch:
        - host: server1
        - host: server2
          port: 9201
        - host: server3
```

[Additional parameters](http://elasticsearch-py.readthedocs.org/en/master/api.html#elasticsearch) like `use_ssl` may be provided as supported by the ElasticSearch Python library.


### Prefixes

A series of namespace prefixes and their URIs should be defined,
these will be used both within the generated SPARQL queries
and in the generated JSON-LD `@context`.

```yaml
    prefixes:
      rdfs: http://www.w3.org/2000/01/rdf-schema#
      owl: http://www.w3o.rg/2002/07/owl#
      dct: http://purl.org/dc/terms/
      dc: http://purl.org/dc/elements/1.1/
      skos: http://www.w3.org/2004/02/skos/core#
      foaf: http://xmlns.com/foaf/0.1/
```

### SPARQL server

The `uri` of a [SPARQL endpoint])(http://www.w3.org/TR/sparql11-protocol/) to query:

```yaml
    sparql:
      uri: http://localhost:8890/sparql
      timeout_s: 7200 # e.g. 2 hours
```

TODO: Support authentication?

### Common properties

The optional `common_properties` specifies any common properties that will
always be indexed:

```yaml
    common_properties:
      - rdfs:label
```

Properties MUST be given as one of:

 - qname using one of the defined [prefixes](#Prefixes)
 - expanded [property configuration](#Property_configuration) (see below)

The JSON-LD property name will be taken from the string after `:`, unless
that name is already used, in which case an auto-generated name is used.
You can use a [property configuration](#Property_configuration) to give a
better name.

### Property configuration

Instead of the qname string, a property can be specified as a
nested object with the keys `sparql`, `variable` and `jsonld`:

```yaml
    common_properties:
      - rdfs:label
      - sparql: "dbprop:shortDescription"
        variable: "shortDesc"
        jsonld: "dc:description"
```

The `sparql` string is inserted verbatim into the query. The string
can be a qname using one of the [prefixes](#Prefixes) (e.g. `dc:title`)
, an absolute IRI enclosed with `<>`
(e.g. `<http://example.com/vocab#property>`), or a
[SPARQL property paths](http://www.w3.org/TR/sparql11-query/#propertypaths)
if supported by the server, e.g. `foaf:knows/foaf:name`.

The `variable` defines the SPARQL variable name that will be used in the query
(excluding the `?`). This name must be unique across properties.

The `jsonld` defines the JSON-LD string that is used for the indexed
JSON-LD document. This MUST be given as a qname
using one of the defined [prefixes](#Prefixes).


### Indexes

Each key under `indexes` specified will (re)create the corresponding [ElasticSearch index](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/docs-index_.html).

**NOTE**: Each index will be deleted before populating with new results.

#### Types

Each index can contain multiple types. The below example creates the two indexes `customers` and `staff`, where the first index has two types, `orgs` and `people`, and the second only the type `people`.

```yaml
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
```

Each index-type combination can specify multiple properties for defining what to index - as detailed below:

#### Graph

A typical configuration for indexing multiple graphs is to have one ElasticSearch index per graph, as shown in the example above.

```yaml
    graph: http://example.com/customers
```

This specifies the name of the `GRAPH` to query. Note that it is always the configured `sparql` endpoint that is contacted, so
this URI is not retrieved.

If the `graph` key is missing, the default graph of the SPARQL endpoint is searched instead.

#### Type

```yaml
    type: foaf:Person
```

The `rdf:type` of triples to index. Typically one ElasticSearch type corresponds to one RDF type.

If `type` is not specified, all resources in the graph with the given properties are indexed.
In this case, all properties are required to be present.

#### Subclasses

```yaml
    subclasses: owl
```

If the resources in the graph are only typed as subclasses of `type` in the graph, then specifying `subclasses` will modify the SPARQL query to select for subclasses instead. You can specify different mechanisms, which will generate corresponding SPARQL fragments:

`subclasses: direct`: will generate:
```
        ?uri a ?subClass .
        ?subClass rdfs:subClassOf ?type .
```

`subclasses: owl` will generate something like:
```
        ?uri a ?subClass .
        ?subClass a owl:Class .
        ?subClass rdfs:subClassOf+ ?type .
```

Note that the `rdfs:subClassOf` and `owl:Class` statements (e.g. loaded from an ontology) must be within the same `graph`.

#### Properties

The RDF properties to index:

```yaml
    properties:
      - dct:title
      - rdfs:description
      - skos:prefLabel
      - skos:altLabel
```

Properties MUST be given as one of:

- qname using one of the defined [prefixes](#Prefixes)
- expanded [property configuration](#Property_configuration) (see below)

Any [common properties](#Common properties) are inserted first at the top of
this list.

Within the indexed JSON documents the property will be given with the local
name of the property (e.g. `title` and `altLabel`), or with `prefix_localname`
if an earlier property in the property list *of this index-type* has a
conflicting name (e.g. `dc:title` `dct:title` would be indexed as `title` and
`dct_title`).

If a `type` was specified (match by type), then in the the generated query the
properties are individually made `OPTIONAL`, if not, all the specified
properties must be present in the graph (match by pattern).
