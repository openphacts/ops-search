FROM grahamdumpleton/mod-wsgi-docker:python-3.5-onbuild
#FROM pablosan/bottle-py3
RUN apt-get update && apt-get -y install git libyajl2 python3-yaml
RUN pip3 install elasticsearch elasticsearch-dsl ijson yajl mimerender rdflib rdflib-jsonld

RUN mkdir /ops-search
ADD src /ops-search
ADD conf/openphacts.yaml /ops-search/config.yaml
WORKDIR /ops-search
EXPOSE 8839
CMD ["search.wsgi", "config.yaml"]
