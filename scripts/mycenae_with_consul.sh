#!/bin/bash

PACKAGE='github.com/uol/mycenae'
CONSUL_POD_NAME="consulMycenae${1}"
POD_NAME="mycenae${1}"

if ! make -C "${GOPATH}/src/${PACKAGE}" build ; then
    exit 1
fi

docker rm -f "${CONSUL_POD_NAME}"
docker rm -f ${POD_NAME}

arguments=(
    '--detach'
    '--hostname' "${CONSUL_POD_NAME}"
    '--name' "${CONSUL_POD_NAME}"
    '--dns' "127.0.0.1"
    '--publish' '8787:8787'
    '--publish' '6666:6666'
)

CONSUL_HOST=$(docker inspect --format "{{ .NetworkSettings.IPAddress }}" consulServer)

consul_arguments=(
    'agent'
    '--join' "${CONSUL_HOST}"
    '--retry-join' "${CONSUL_HOST}"
    '-recursor' "192.168.206.8"
)

docker run "${arguments[@]}" "progrium/consul" "${consul_arguments[@]}"

SCYLLA_HOST=$(docker inspect --format "{{ .NetworkSettings.IPAddress }}" consulScylla1)
ELASTIC_HOST=$(docker inspect --format "{{ .NetworkSettings.IPAddress }}" elastic)

pod_arguments=(
    #'--detach'
    '--name' "${POD_NAME}"
    '--network' "container:${CONSUL_POD_NAME}"
    '--volume' "${GOPATH}/src/${PACKAGE}/mycenae:/tmp/mycenae"
    '--volume' "${GOPATH}/src/${PACKAGE}/config-scylla.toml:/config.toml"
    '--volume' "${GOPATH}/src/${PACKAGE}/scripts/ssl:/ssl:ro"
    '--entrypoint' '/tmp/mycenae'
)

docker run "${pod_arguments[@]}" "ubuntu:xenial"

sleep 5

curl -XPUT -d '{"name":"mycenae1","port":8787}' --header "Content-type: application/json" "http://localhost:8500/v1/agent/service/register"
curl -H "Content-Type: application/json" -X POST \
-d '{
  "ID": "${POD_NAME}",
  "Name": "${POD_NAME}",
  "Address": "127.0.0.1",
  "Port": 8787,
  "EnableTagOverride": false,
  "Check": {
    "DeregisterCriticalServiceAfter": "1m",
    "HTTP": "http://localhost:8787/probe",
    "Interval": "10s",
    "TTL": "15s"
  }
}' "http://127.0.0.1:8500/v1/health/node/${POD_NAME}"