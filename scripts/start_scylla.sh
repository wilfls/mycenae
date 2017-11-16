#!/bin/bash

pod_name="scylla$1"

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