#!/bin/bash

PACKAGE='github.com/uol/mycenae'
CONSUL_POD_NAME="consulMycenae${1}"
POD_NAME="mycenae${1}"

#if ! make -C "${GOPATH}/src/${PACKAGE}" build ; then
#    exit 1
#fi

docker rm -f "${CONSUL_POD_NAME}"
docker rm -f ${POD_NAME}

arguments=(
    '--detach'
    '--hostname' "${CONSUL_POD_NAME}"
    '--name' "${CONSUL_POD_NAME}"
    '--dns' "127.0.0.1"
    '--volume' "${GOPATH}/src/${PACKAGE}/scripts/ssl:/ssl:ro"
    '--volume' "${GOPATH}/src/${PACKAGE}/scripts/ssl.json:/config/ssl.json:ro"
    '--volume' "${GOPATH}/src/${PACKAGE}/scripts/consul-mycenae.json:/consul/config/service.json"
    '--publish' '8787:8080'
    '--publish' '6666:6666'
)

consul_arguments=(
    'agent'
    '--join' "${CONSUL_HOST}"
    '--retry-join' "${CONSUL_HOST}"
    '-disable-host-node-id'
)

docker run "${arguments[@]}" "progrium/consul" "${consul_arguments[@]}"

CONSUL_HOST=$(docker inspect --format "{{ .NetworkSettings.IPAddress }}" consulServer)
SCYLLA_HOST=$(docker inspect --format "{{ .NetworkSettings.IPAddress }}" consulScylla1)
ELASTIC_HOST=$(docker inspect --format "{{ .NetworkSettings.IPAddress }}" elastic)

pod_arguments=(
    '--detach'
    '--name' "${POD_NAME}"
    '--network' "container:${CONSUL_POD_NAME}"
    '--volume' "${GOPATH}/src/${PACKAGE}/mycenae:/tmp/mycenae"
    '--volume' "${GOPATH}/src/${PACKAGE}/config-scylla.toml:/config.toml"
    '--volume' "${GOPATH}/src/${PACKAGE}/scripts/ssl:/ssl:ro"
    '--entrypoint' '/tmp/mycenae'
)

docker run "${pod_arguments[@]}" "ubuntu:xenial"
docker exec "${CONSUL_POD_NAME}" curl --silent -XPUT -d '{"name":"mycenae","port":8989}' --header "Content-type: application/json" "http://localhost:8500/v1/agent/service/register"