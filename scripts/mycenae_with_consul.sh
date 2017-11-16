#!/bin/bash

#CONSUL_POD_NAME="consulMycenae${1}"
POD_NAME="mycenae${1}"

#if ! make -C "${GOPATH}/src/github.com/uol/mycenae/" build ; then
#    exit 1
#fi

#docker rm -f "${CONSUL_POD_NAME}"
docker rm -f "${POD_NAME}"

#arguments=(
#    '--detach'
#    '--hostname' "${CONSUL_POD_NAME}"
#    '--name' "${CONSUL_POD_NAME}"
#    '-e' "CONSUL_BIND_INTERFACE=l0"
#    '--net=host'
#)

#CONSUL_HOST=$(docker inspect --format "{{ .NetworkSettings.IPAddress }}" consulServer)

#consul_arguments=(
#    'agent'
#    '-dev'
#    '--join' "${CONSUL_HOST}"
#)

#docker run "${arguments[@]}" "consul:1.0.0" "${consul_arguments[@]}"

#SCYLLA_HOST=$(docker inspect --format "{{ .NetworkSettings.IPAddress }}" consulScylla1)
#ELASTIC_HOST=$(docker inspect --format "{{ .NetworkSettings.IPAddress }}" elastic)

pod_arguments=(
    '--detach'
    '--name' "${POD_NAME}"
    '--net=host'
    '--hostname' "${POD_NAME}"
    '--volume' "${GOPATH}/src/github.com/uol/mycenae/mycenae:/tmp/mycenae"
    '--volume' "${GOPATH}/src/github.com/uol/mycenae/config-scylla.toml:/config.toml"
    '--entrypoint' '/tmp/mycenae'
)

docker run "${pod_arguments[@]}" "ubuntu:xenial"
#echo "docker run ${pod_arguments[@]} ubuntu:xenial"

#curl --silent -XPUT --header "Content-type: application/json" "http://127.0.0.1:8500/v1/agent/service/register" \
#-d '{
#        "ID": "mycenae",
#        "Name": "mycenae",
#        "Tags": [
#            "http"
#        ],
#        "Port": 8787,
#        "Token": "your-token-here",
#        "Check": {
#            "ID": "mycenae-probe",
#            "Name": "mycenae-probe",
#            "HTTP": "http://127.0.0.1:8787/probe",
#            "Interval": "1s"
#        }
#    }'

#curl --silent -XPUT --header "Content-type: application/json" "http://127.0.0.1:8500/v1/agent/service/register" \
#-d '{
#        "ID": "mycenae-udp",
#        "Name": "mycenae",
#        "Tags": [
#           "udpv2"
#        ],
#        "Port": 4243,
#        "Token": "your-token-here"
#    }'