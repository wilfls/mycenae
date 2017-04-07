package cluster

import (
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/billhathaway/consistentHash"
	"github.com/uol/gobol"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"sync"

	pb "github.com/uol/mycenae/lib/proto"
	"github.com/uol/mycenae/lib/storage"
	"github.com/uol/mycenae/lib/storage/timecontrol"
)

var logger *logrus.Logger

type Config struct {
	Consul ConsulConfig
	//gRPC port
	Port int
	//Ticker interval to check cluster changes
	CheckInterval string
	//Time, in seconds, to wait before applying cluster changes to consistency hashing
	ApplyWait int64
}

type state struct {
	add  bool
	time int64
}

func New(log *logrus.Logger, sto *storage.Storage, tc *timecontrol.Timecontrol, conf Config) (*Cluster, gobol.Error) {

	ci, err := time.ParseDuration(conf.CheckInterval)
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
		apply: conf.ApplyWait,
		nodes: map[string]*node{},
		toAdd: map[string]state{},
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
	tc    *timecontrol.Timecontrol
	c     *consul
	ch    *consistentHash.ConsistentHash
	apply int64

	server   *grpc.Server
	stopServ chan struct{}

	nodes  map[string]*node
	nMutex sync.RWMutex
	toAdd  map[string]state

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

							if s, ok := c.toAdd[srv.Node.ID]; ok {
								if s.add {
									if c.tc.Now()-s.time >= c.apply {

										n, err := newNode(srv.Node.Address, srv.Service.Port)
										if err != nil {
											logger.Error(err)
										}

										c.nMutex.Lock()
										c.nodes[srv.Node.ID] = n
										c.nMutex.Unlock()

										c.ch.Add(srv.Node.ID)

										delete(c.toAdd, srv.Node.ID)
									}
								} else {
									c.toAdd[srv.Node.ID] = state{
										add:  true,
										time: c.tc.Now(),
									}
								}
							} else {
								c.toAdd[srv.Node.ID] = state{
									add:  true,
									time: c.tc.Now(),
								}
							}
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

		if s, ok := c.toAdd[id]; ok {
			if !s.add {
				if c.tc.Now()-s.time >= c.apply {

					c.ch.Remove(id)

					c.nMutex.Lock()
					c.nodes[id].close()
					delete(c.nodes, id)
					c.nMutex.Unlock()

					delete(c.toAdd, id)
				}
			} else {
				c.toAdd[id] = state{
					add:  false,
					time: c.tc.Now(),
				}
			}
		} else {
			c.toAdd[id] = state{
				add:  false,
				time: c.tc.Now(),
			}
		}

	}

}

func (c *Cluster) Stop() {
	c.stopServ <- struct{}{}
	c.server.GracefulStop()
}
