#!/bin/bash

## Start consul server

arguments=(
    '--hostname' 'consulServer'
    '--name' "consulServer"
    '-d'
    '-p8500:8500'
    '--net=host'
)

consul_arguments=(
    'agent'
    '-server'
    '-ui'
    '-bootstrap'
)

docker run "${arguments[@]}" 'consul:1.0.0' "${consul_arguments[@]}"