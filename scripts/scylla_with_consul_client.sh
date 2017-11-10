#!/bin/bash

name="consulScylla$1"
pod_name="scylla$1"

arguments=(
    '--hostname' "${name}"
    '--name' "${name}"
    '--dns' "127.0.0.1"
)

server=$(docker inspect --format "{{ .NetworkSettings.IPAddress }}" consulServer)

consul_arguments=(
    '--join' "${server}"
    '--retry-join' "${server}"
    '-recursor' "192.168.206.8" 
)

sleep 10

docker run --detach "${arguments[@]}" "progrium/consul" "${consul_arguments[@]}"

pod_arguments=(
    '--network' "container:${name}"
    '--name' "${pod_name}"
)

scylla_arguments=(
    '--smp=2'
    '--memory=2G'
    '--developer-mode=1'
)

if [ $1 -gt 1 ]
    then
        seedIP="$(docker inspect --format='{{ .NetworkSettings.IPAddress }}' consulScylla1)"
        scylla_arguments[${#scylla_arguments[@]}]="--seeds=$seedIP"
fi

docker run "${pod_arguments[@]}" -d "jenkins.macs.intranet:5000/mycenae/scylla:1.0" "${scylla_arguments[@]}"

client=$(docker inspect --format "{{ .NetworkSettings.IPAddress }}" "$name")

docker exec $name curl --silent -XPUT -d '{"name":"scylla","port":9042}' --header "Content-type: application/json" "http://localhost:8500/v1/agent/service/register"