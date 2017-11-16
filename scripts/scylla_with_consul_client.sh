#!/bin/bash

#name="consulScylla$1"
pod_name="scylla$1"

#server=$(docker inspect --format "{{ .NetworkSettings.IPAddress }}" consulServer)

#arguments=(
#    '--detach'
#    '--name' "${name}"
#    '--hostname' "${name}"
#    '--net=host'
#)

#if [ $1 -eq 1 ]
#    then
#        arguments[${#arguments[@]}]='-p9042:9042'
#fi

#consul_arguments=(
#    'agent'
#    '-dev'
#    '-bind=127.0.0.1'
#    '--join' "${server}"
#)

#docker run "${arguments[@]}" "consul:1.0.0" "${consul_arguments[@]}"

#pod_arguments=(
    #'-d'
    #'--name' "${pod_name}"
#    '--net=host'
    ###
#    '--name' "${pod_name}"
#    '--hostname' "${pod_name}"
#)

pod_arguments=(
    '-d'
    '-it'
    '--name' "${pod_name}"
    '-e' 'CONFIG=/opt/scylla/conf/scylla.yaml'
)

if [ $1 -gt 1 ]
    then
        seedIP="$(docker inspect --format='{{ .NetworkSettings.IPAddress }}' scylla1)"
        pod_arguments[${#pod_arguments[@]}]="-e SEEDS=$seedIP"
else
    pod_arguments[${#pod_arguments[@]}]="-e SEEDS=127.0.0.1"
fi

eval "docker run ${pod_arguments[@]} jenkins.macs.intranet:5000/mycenae/scylla:v1"
echo "docker run ${pod_arguments[@]} jenkins.macs.intranet:5000/mycenae/scylla:v1"

#curl --silent -XPUT -d '{"name":"scylla","port":9042}' --header "Content-type: application/json" "http://localhost:8500/v1/agent/service/register"
