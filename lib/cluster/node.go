package cluster

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"

	"golang.org/x/time/rate"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/uol/gobol"
	pb "github.com/uol/mycenae/lib/proto"
	"go.uber.org/zap"
)

type node struct {
	address string
	port    int
	conf    Config
	mtx     sync.RWMutex
	ptsCh   chan []*pb.Point
	pts     []*pb.Point

	conn     *grpc.ClientConn
	client   pb.TimeseriesClient
	wLimiter *rate.Limiter
	rLimiter *rate.Limiter
}

func newNode(address string, port int, conf Config) (*node, gobol.Error) {

	//cred, err := newClientTLSFromFile(conf.Consul.CA, conf.Consul.Cert, conf.Consul.Key, "*")
	cred, err := credentials.NewClientTLSFromFile(conf.Consul.Cert, "localhost.consul.macs.intranet")
	if err != nil {
		return nil, errInit("newNode", err)
	}

	conn, err := grpc.Dial(fmt.Sprintf("%v:%d", address, port), grpc.WithTransportCredentials(cred))
	if err != nil {
		return nil, errInit("newNode", err)
	}

	logger.Debug(
		"new node",
		zap.String("package", "cluster"),
		zap.String("func", "newNode"),
		zap.String("addr", address),
		zap.Int("port", port),
	)

	node := &node{
		address:  address,
		port:     port,
		conf:     conf,
		conn:     conn,
		ptsCh:    make(chan []*pb.Point, 5),
		wLimiter: rate.NewLimiter(rate.Limit(conf.GrpcMaxServerConn)*0.9, conf.GrpcBurstServerConn),
		rLimiter: rate.NewLimiter(rate.Limit(conf.GrpcMaxServerConn)*0.1, conf.GrpcBurstServerConn),
		client:   pb.NewTimeseriesClient(conn),
	}

	for i := 0; i < 5; i++ {
		node.worker()
	}

	return node, nil
}

func (n *node) write(pts []*pb.Point) {

	n.ptsCh <- pts

}

func (n *node) read(ksid, tsid string, start, end int64) ([]*pb.Point, gobol.Error) {

	ctx, cancel := context.WithTimeout(context.Background(), n.conf.gRPCtimeout)
	defer cancel()

	if err := n.rLimiter.Wait(ctx); err != nil {
		return []*pb.Point{}, errRequest("node/read", http.StatusInternalServerError, err)
	}

	stream, err := n.client.Read(ctx, &pb.Query{Ksid: ksid, Tsid: tsid, Start: start, End: end})
	if err != nil {
		return []*pb.Point{}, errRequest("node/read", http.StatusInternalServerError, err)
	}

	var pts []*pb.Point
	for {
		p, err := stream.Recv()
		if err == io.EOF {
			// read done.
			return pts, nil
		}
		if err != nil {
			return pts, errRequest("node/write", http.StatusInternalServerError, err)
		}

		pts = append(pts, p)
	}

}

func (n *node) worker() {

	go func() {
		for {

			pts := <-n.ptsCh

			ctx, cancel := context.WithTimeout(context.Background(), n.conf.gRPCtimeout)

			if err := n.wLimiter.Wait(ctx); err != nil {
				logger.Error(
					"write limit",
					zap.String("package", "cluster"),
					zap.String("func", "worker"),
					zap.Error(err),
				)
				cancel()
				continue
			}

			stream, err := n.client.Write(ctx)
			if err != nil {
				logger.Error(
					"failed to get stream from server",
					zap.String("package", "cluster"),
					zap.String("func", "worker"),
					zap.Error(err),
				)
				cancel()
				continue
			}

			for _, p := range pts {
				attempts := 1
				var err error
				for {
					err = stream.Send(p)
					if err == io.EOF {
						break
					}
					if err != nil {
						logger.Error(
							"retry write stream",
							zap.String("package", "cluster"),
							zap.String("func", "write"),
							zap.Error(err),
						)
						if attempts >= 5 {
							break
						}
						attempts++
						continue
					}
					break
				}
				if err != nil && err != io.EOF {
					logger.Error(
						"too many attempts, unable to write to stream",
						zap.String("package", "cluster"),
						zap.String("func", "write"),
						zap.Error(err),
					)
				}
			}
			_, err = stream.CloseAndRecv()
			if err != nil && err != io.EOF {
				logger.Error(
					"problem writing to stream",
					zap.String("package", "cluster"),
					zap.String("func", "write"),
					zap.Error(err),
				)
			}

			cancel()
		}

	}()

}

func (n *node) meta(m *pb.Meta) (bool, gobol.Error) {
	mf, err := n.client.GetMeta(context.Background(), m)

	if err != nil {
		return false, errRequest("node/meta", http.StatusInternalServerError, err)
	}

	return mf.Ok, nil
}

func (n *node) close() {
	err := n.conn.Close()
	if err != nil {
		logger.Error(
			"closing connection",
			zap.String("package", "cluster"),
			zap.String("func", "node/close"),
			zap.Error(err),
		)
	}
}
