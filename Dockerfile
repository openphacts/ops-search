FROM pablosan/bottle-py3
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get -y install git libyajl2 python3-yaml 
RUN pip3 install elasticsearch ijson yajl mimerender rdflib rdflib-jsonld

RUN mkdir -p /ops-search/src /ops-search/conf
ADD src /ops-search/src
ADD conf /ops-search/conf
WORKDIR /ops-search
EXPOSE 8839
CMD ["src/api.py", "conf/openphacts.yaml", "8839"]
