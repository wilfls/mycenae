package cluster

import (
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/billhathaway/consistentHash"
	"github.com/uol/gobol"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"sync"

	pb "github.com/uol/mycenae/lib/proto"
	"github.com/uol/mycenae/lib/storage"
)

var logger *logrus.Logger

type Config struct {
	Consul ConsulConfig
	//gRPC port
	Port int
	//Ticker interval to check cluster changes
	CheckInterval string
	//Time wait before apply cluster changes to consistency hashing
	ApplyWait string
}

func New(log *logrus.Logger, sto *storage.Storage, conf Config) (*Cluster, gobol.Error) {

	ci, err := time.ParseDuration(conf.CheckInterval)
	if err != nil {
		return nil, errInit("New", err)
	}

	aw, err := time.ParseDuration(conf.ApplyWait)
	if err != nil {
		return nil, errInit("New", err)
	}

	c, gerr := newConsul(conf.Consul)
	if gerr != nil {
		return nil, gerr
	}

	s, gerr := c.getSelf()
	if gerr != nil {
		return nil, gerr
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", conf.Port))
	if err != nil {
		return nil, errInit("New", err)
	}

	logger = log

	clr := &Cluster{
		c:     c,
		s:     sto,
		ch:    consistentHash.New(),
		apply: aw,
		nodes: map[string]*node{},
		tag:   conf.Consul.Tag,
		self:  s,
	}

	grpcServer := grpc.NewServer()

	pb.RegisterTimeseriesServer(grpcServer, clr)

	clr.server = grpcServer

	clr.getNodes()

	go func(lis net.Listener) {
		err := grpcServer.Serve(lis)
		if err != nil {
			logger.Error(err)
		}
	}(lis)

	go clr.checkCluster(ci)

	return clr, nil
}

type Cluster struct {
	s     *storage.Storage
	c     *consul
	ch    *consistentHash.ConsistentHash
	apply time.Duration

	server   *grpc.Server
	stopServ chan struct{}

	nodes  map[string]*node
	nMutex sync.RWMutex

	tag  string
	self string
}

func (c *Cluster) checkCluster(interval time.Duration) {

	ticker := time.NewTicker(interval)

	for {
		select {
		case <-ticker.C:
			c.getNodes()
		case <-c.stopServ:
			return
		}
	}

}

func (c *Cluster) Write(p *storage.Point) gobol.Error {

	nodeID, err := c.ch.Get([]byte(p.ID))
	if err != nil {
		return errRequest("Write", http.StatusInternalServerError, err)
	}

	if nodeID == c.self {
		c.s.Add(p.KsID, p.ID, p.Timestamp, *p.Message.Value)
		return nil
	}

	c.nMutex.RLock()
	node := c.nodes[nodeID]
	c.nMutex.RUnlock()

	return node.write(p)
}

func (c *Cluster) Read(ksid, tsid string, start, end int64) (storage.Pnts, int, gobol.Error) {

	nodeID, err := c.ch.Get([]byte(tsid))
	if err != nil {
		return nil, 0, errRequest("Read", http.StatusInternalServerError, err)
	}

	if nodeID == c.self {
		return c.s.Read(ksid, tsid, start, end)
	}

	c.nMutex.RLock()
	node := c.nodes[nodeID]
	c.nMutex.RUnlock()

	return node.read(ksid, tsid, start, end)
}

func (c *Cluster) SavePoint(ctx context.Context, p *pb.Point) (*pb.PointError, error) {

	c.s.Add(p.GetKsid(), p.GetTsid(), p.GetTimestamp(), p.GetValue())

	return nil, nil
}

func (c *Cluster) GetTS(ctx context.Context, q *pb.Query) (*pb.Tss, error) {

	ts, l, err := c.s.Read(q.GetKsid(), q.GetTsid(), q.GetStart(), q.GetEnd())
	if err != nil {
		return nil, err
	}

	tss := &pb.Tss{}

	tss.Tss = make([]*pb.Tsdata, l)

	for i, p := range ts {
		tss.Tss[i] = &pb.Tsdata{Value: p.Value, Timestamp: p.Date}
	}

	return tss, nil
}

func (c *Cluster) getNodes() {
	srvs, err := c.c.getNodes()
	if err != nil {
		logger.Error(err)
	}

	for _, srv := range srvs {

		for _, tag := range srv.Service.Tags {
			if tag == c.tag {

				for _, check := range srv.Checks {
					if check.ServiceID == srv.Service.ID && check.Status == "passing" {

						node, ok := c.nodes[srv.Node.ID]

						if ok {
							if node.port != srv.Service.Port || node.address != srv.Node.Address {
								node.close()
								n, err := newNode(srv.Node.Address, srv.Service.Port)
								if err != nil {
									logger.Error(err)
								}

								c.nMutex.Lock()
								c.nodes[srv.Node.ID] = n
								c.nMutex.Unlock()
							}
						} else {
							n, err := newNode(srv.Node.Address, srv.Service.Port)
							if err != nil {
								logger.Error(err)
							}

							c.nMutex.Lock()
							c.nodes[srv.Node.ID] = n
							c.nMutex.Unlock()

							c.addToCh(srv.Node.ID)
						}
					}
				}
			}

		}
	}

	del := []string{}

	for id := range c.nodes {
		found := false
		for _, srv := range srvs {
			for _, check := range srv.Checks {
				if check.ServiceID == srv.Service.ID && check.Status == "passing" && id == srv.Node.ID {
					found = true
				}
			}

		}
		if !found {
			del = append(del, id)
		}
	}

	for _, id := range del {
		c.removeFromCh(id)

		c.nMutex.Lock()
		c.nodes[id].close()
		delete(c.nodes, id)
		c.nMutex.Unlock()

	}

}

func (c *Cluster) addToCh(n string) {
	go func(d time.Duration) {
		time.Sleep(d)
		c.ch.Add(n)
	}(c.apply)

}

func (c *Cluster) removeFromCh(n string) {
	go func(d time.Duration) {
		time.Sleep(d)
		c.ch.Remove(n)
	}(c.apply)
}

func (c *Cluster) Stop() {
	c.stopServ <- struct{}{}
	c.server.GracefulStop()
}
