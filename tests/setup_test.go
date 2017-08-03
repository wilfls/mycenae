package main

import (
	"flag"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/uol/mycenae/tests/tools"
)

var setup = flag.Bool("setup", true, "flag used to skip setup when set to false")
var mycenaeTools tools.Tool
var ksMycenae, ksMycenaeMeta, ksMycenaeTsdb string

func TestMain(m *testing.M) {

	mycenaeTools.InitCass(tools.CassandraSettings{
		Keyspace:    "mycenae",
		Consistency: "quorum",
		Nodes:       []string{"cassandra1", "cassandra2", "cassandra3"},

		Username:    "cassandra",
		Password:    "cassandra",
		Connections: 3,
		Retry:       5,
		PageSize:    1000,

		DiscoverHosts:  true,
		DiscoverySleep: 10,
		Timeout:        "10s",
		ProtoVersion:   4,
	})

	mycenaeTools.InitHTTP("http://mycenae", "8080", time.Minute)

	mycenaeTools.InitUDP("mycenae", "4243")

	mycenaeTools.InitMycenae(tools.MycenaeSettings{
		Node:    "http://mycenae",
		Port:    "8080",
		Timeout: time.Minute,
	})

	mycenaeTools.InitEs(tools.ElasticsearchSettings{
		Node:    "http://elastic",
		Port:    "9200",
		Timeout: 20 * time.Second,
	})

	flag.Parse()

	ksMycenae = mycenaeTools.Mycenae.CreateKeyspace("datacenter1", fmt.Sprint(time.Now().Unix()), "mycenae@mycenae.com", 90, 3)

	if *setup {

		var wg sync.WaitGroup

		ksMycenaeMeta = mycenaeTools.Mycenae.CreateKeyspace("datacenter1", fmt.Sprint(time.Now().Unix()), "mycenae@mycenae.com", 90, 3)
		ksMycenaeTsdb = mycenaeTools.Mycenae.CreateKeyspace("datacenter1", fmt.Sprint(time.Now().Unix()), "mycenae@mycenae.com", 90, 3)

		wg.Add(8)

		go func() { sendPointsExpandExp(ksMycenae); wg.Done() }()
		go func() { sendPointsMetadata(ksMycenaeMeta); wg.Done() }()
		go func() { sendPointsParseExp(ksMycenae); wg.Done() }()
		go func() { sendPointsPointsGrafana(ksMycenae); wg.Done() }()
		go func() { sendPointsPointsGrafanaMem(ksMycenae); wg.Done() }()
		go func() { sendPointsTsdbAggAndSugAndLookup(ksMycenaeTsdb); wg.Done() }()
		go func() { sendPointsV2(ksMycenae); wg.Done() }()
		go func() { sendPointsV2Text(ksMycenae); wg.Done() }()

		wg.Wait()

		time.Sleep(time.Second * 5)
	}

	os.Exit(m.Run())
}
