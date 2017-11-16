#!/bin/bash

./start_scylla_cluster.sh

docker rm -f elastic grafana_mycenae

docker run -d --name elastic -v $GOPATH/src/github.com/uol/mycenae/docs/elasticsearch.yml:/etc/elasticsearch/elasticsearch.yml elasticsearch:2.4.1
docker run -d --name grafana_mycenae --net=host -p 3000:3000  grafana/grafana:4.2.0

sleep 3

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

./start_mycenae.sh 1