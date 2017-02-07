package config

import (
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/uol/gobol/rip"
)

func Aggregators(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	if keyspace := ps.ByName("keyspace"); keyspace == "" {
		rip.AddStatsMap(r, map[string]string{"path": "/keyspaces/#keyspace/api/aggregators", "keyspace": "empty"})
	} else {
		rip.AddStatsMap(r, map[string]string{"path": "/keyspaces/#keyspace/api/aggregators", "keyspace": keyspace})
	}
	rip.SuccessJSON(w, http.StatusOK, GetAggregators())
}

func Filters(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	if keyspace := ps.ByName("keyspace"); keyspace == "" {
		rip.AddStatsMap(r, map[string]string{"path": "/keyspaces/#keyspace/api/config/filters", "keyspace": "empty"})
	} else {
		rip.AddStatsMap(r, map[string]string{"path": "/keyspaces/#keyspace/api/config/filters", "keyspace": keyspace})
	}
	rip.SuccessJSON(w, http.StatusOK, GetFiltersFull())
}
