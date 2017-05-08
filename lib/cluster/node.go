package cluster

import (
	"fmt"
	"net/http"

	"github.com/uol/gobol"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/uol/mycenae/lib/gorilla"
	pb "github.com/uol/mycenae/lib/proto"
)

type node struct {
	address string
	port    int

	conn *grpc.ClientConn
	c    pb.TimeseriesClient
}

func newNode(address string, port int) (*node, gobol.Error) {

	conn, err := grpc.Dial(fmt.Sprintf("%v:%d", address, port), grpc.WithInsecure())
	if err != nil {
		return nil, errInit("newNode", err)
	}

	return &node{
		address: address,
		port:    port,
		conn:    conn,
		c:       pb.NewTimeseriesClient(conn),
	}, nil

}

func (n *node) write(p *gorilla.Point) gobol.Error {
	_, err := n.c.SavePoint(context.Background(), &pb.Point{Ksid: p.KsID, Tsid: p.ID, Value: *p.Message.Value, Timestamp: p.Message.Timestamp})
	return errRequest("savePoint", http.StatusInternalServerError, err)
}

func (n *node) read(ksid, tsid string, start, end int64) (gorilla.Pnts, gobol.Error) {

	pts, err := n.c.GetTS(context.Background(), &pb.Query{Ksid: ksid, Tsid: tsid, Start: start, End: end})
	if err != nil {
		return nil, errRequest("getTs", http.StatusInternalServerError, err)
	}

	tss := pts.GetTss()

	ts := make(gorilla.Pnts, len(tss))

	for i, p := range tss {
		ts[i] = gorilla.Pnt{Value: p.GetValue(), Date: p.GetTimestamp()}
	}

	return ts, nil
}

func (n *node) close() gobol.Error {
	err := n.conn.Close()
	if err != nil {
		errInit("close", err)
	}
	return nil
}
