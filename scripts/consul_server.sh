#!/bin/bash

## Start consul server

arguments=(
    '--hostname' 'server'
    '--name' "consulServer"
    '-p' "8500:8500"
)

consul_arguments=(
    '--server'
    '--bootstrap'
    '-recursor' "192.168.206.8"
)

docker run -d "${arguments[@]}" "progrium/consul" "${consul_arguments[@]}"
