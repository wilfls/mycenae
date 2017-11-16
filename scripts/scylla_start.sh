#!/bin/bash

docker rm -f scylla1 scylla2 scylla3 consulScylla1 consulScylla2 consulScylla3 consulServer elastic grafana_mycenae
#docker network rm mycenaeNetwork
#docker network create -d bridge mycenaeNetwork --attachable

checkScyllaUpNodes () {
    upnodes=$(docker exec -it scylla1 sh -c "/opt/scylla-tools/bin/nodetool status" | grep UN | wc -l)
    while [ "$upnodes" != "$1" ]
    do
        sleep 1
        upnodes=$(docker exec -it scylla1 sh -c "/opt/scylla-tools/bin/nodetool status" | grep UN | wc -l)
        echo "Upnodes: $upnodes"
    done
}

#./consul_server.sh
./scylla_with_consul_client.sh 1
./scylla_with_consul_client.sh 2
./scylla_with_consul_client.sh 3
checkScyllaUpNodes 3

docker run -d --name elastic --net=host -v $GOPATH/src/github.com/uol/mycenae/docs/elasticsearch.yml:/etc/elasticsearch/elasticsearch.yml elasticsearch:2.4.1
docker run -d --name grafana_mycenae --net=host -p 3000:3000  grafana/grafana:4.2.0

sleep 5

docker cp $GOPATH/src/github.com/uol/mycenae/docs/scylladb.cql scylla1:/tmp/

scyllaIP=$(docker inspect --format "{{ .NetworkSettings.IPAddress }}" scylla1)
docker exec -it scylla1 sh -c "cqlsh ${scyllaIP} -u cassandra -p cassandra < /tmp/scylladb.cql"

docker exec elastic curl --silent -H "Content-Type: application/json" -X POST \
-d '{
	"mappings": {
		"meta": {
			"properties": {
				"tagsNested": {
					"type": "nested",
					"properties": {
						"tagKey": {
							"type": "string"
						},
						"tagValue": {
							"type": "string"
						}
					}
				}
			}
		},
		"metatext": {
			"properties": {
				"tagsNested": {
					"type": "nested",
					"properties": {
						"tagKey": {
							"type": "string"
						},
						"tagValue": {
							"type": "string"
						}
					}
				}
			}
		}
	}
}' http://127.0.0.1:9200/stats

curl --silent -POST -H "Content-Type: application/json" -u admin:admin -d '{"name": "stats","type": "opentsdb","access": "proxy","url": "http://localhost:8787/keyspaces/stats","basicAuth": false}' http://localhost:3000/api/datasources
curl --silent -POST -H "Content-Type: application/json" -u admin:admin -d @../docs/mycenae_dashboard http://localhost:3000/api/dashboards/db

scyllaIPs=$(docker inspect --format "{{ .NetworkSettings.IPAddress }}" scylla1 scylla2 scylla3 | sed 's/^.*$/"&"/' | tr '\n' ',' | sed 's/.$//')
#scyllaIPs=$(docker inspect --format "{{ .NetworkSettings.IPAddress }}" scylla1 | sed 's/^.*$/"&"/' | tr '\n' ',' | sed 's/.$//')
elasticIP=$(docker inspect --format "{{ .NetworkSettings.IPAddress }}" elastic)

sed -i 's/nodes = \[[^]]*\]/nodes = \['$scyllaIPs'\]/' ../config-scylla.toml
sed -i 's/"[^:]*:9200"/"'$elasticIP':9200"/' ../config-scylla.toml

#./mycenae_with_consul.sh 1