#!/bin/bash

docker rm -f scylla1 scylla2 scylla3

checkScyllaUpNodes () {
    upnodes=$(docker exec -it scylla1 sh -c "/opt/scylla-tools/bin/nodetool status" | grep UN | wc -l)
    while [ "$upnodes" != "$1" ]
    do
        sleep 1
        upnodes=$(docker exec -it scylla1 sh -c "/opt/scylla-tools/bin/nodetool status" | grep UN | wc -l)
        echo "Upnodes: $upnodes"
    done
}

./start_scylla.sh 1
./start_scylla.sh 2
./start_scylla.sh 3
checkScyllaUpNodes 3

docker cp $GOPATH/src/github.com/uol/mycenae/docs/scylladb.cql scylla1:/tmp/
scyllaIP=$(docker inspect --format "{{ .NetworkSettings.IPAddress }}" scylla1)
docker exec -it scylla1 sh -c "cqlsh ${scyllaIP} -u cassandra -p cassandra < /tmp/scylladb.cql"
