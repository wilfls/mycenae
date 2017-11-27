#!/bin/bash

POD_NAME="mycenae${1}"
docker rm -f "${POD_NAME}"

if ! make -C "${GOPATH}/src/github.com/uol/mycenae/" build ; then
    exit 1
fi

scyllaIPs=$(docker inspect --format "{{ .NetworkSettings.IPAddress }}" scylla1 scylla2 scylla3 | sed 's/^.*$/"&"/' | tr '\n' ',' | sed 's/.$//')
elasticIP=$(docker inspect --format "{{ .NetworkSettings.IPAddress }}" elasticsearch)

sed -i 's/nodes = \[[^]]*\]/nodes = \['$scyllaIPs'\]/' ../config-scylla.toml
sed -i 's/"[^:]*:9200"/"'$elasticIP':9200"/' ../config-scylla.toml

pod_arguments=(
	'-it'
    '--detach'
    '--name' "${POD_NAME}"
    '--volume' "${GOPATH}/src/github.com/uol/mycenae/mycenae:/tmp/mycenae"
    '--volume' "${GOPATH}/src/github.com/uol/mycenae/config-scylla.toml:/config.toml"
    '--entrypoint' '/tmp/mycenae'
)

dockerCmd="docker run ${pod_arguments[@]} ubuntu:xenial"
eval "$dockerCmd"
echo "$dockerCmd"

echo 'Mycenae OK'

docker logs "${POD_NAME}"