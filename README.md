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


## Configuration
