#!/bin/bash

## Start consul server

arguments=(
    '--hostname' 'server'
    '--name' "consulServer"
    '-p' "8500:8500"
)

consul_arguments=(
    'agent'
    '-ui'
    '-server'
    '-bootstrap'
    '-recursor' "192.168.206.8"
    '-client' '0.0.0.0'
)

docker run -d "${arguments[@]}" consul:0.7.3 "${consul_arguments[@]}"
