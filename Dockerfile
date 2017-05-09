FROM pablosan/bottle-py3
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get -y install git libyajl2 python3-yaml
RUN pip3 install elasticsearch elasticsearch_dsl ijson yajl mimerender rdflib rdflib-jsonld

RUN mkdir /ops-search
ADD src /ops-search
ADD conf/openphacts.yaml /ops-search/config.yaml
WORKDIR /ops-search
EXPOSE 8839
CMD ["/ops-search/api.py", "config.yaml", "8839"]
