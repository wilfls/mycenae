package cluster

import (
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/billhathaway/consistentHash"
	"github.com/uol/gobol"

	"github.com/uol/mycenae/lib/gorilla"
	pb "github.com/uol/mycenae/lib/proto"
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

func New(log *logrus.Logger, sto *gorilla.Storage, conf Config) (*Cluster, gobol.Error) {

	if sto == nil {
		return nil, errInit("New", errors.New("storage can't be nil"))
	}

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

	logger = log

	server, err := newServer(conf, sto)
	if gerr != nil {
		return nil, errInit("New", err)
	}

	clr := &Cluster{
		c:      c,
		s:      sto,
		ch:     consistentHash.New(),
		cfg:    &conf,
		apply:  conf.ApplyWait,
		nodes:  map[string]*node{},
		toAdd:  map[string]state{},
		tag:    conf.Consul.Tag,
		self:   s,
		port:   conf.Port,
		server: server,
	}

	clr.ch.Add(s)
	clr.getNodes()
	go clr.checkCluster(ci)

	return clr, nil
}

type Cluster struct {
	s     *gorilla.Storage
	c     *consul
	ch    *consistentHash.ConsistentHash
	cfg   *Config
	apply int64

	server   *server
	stopServ chan struct{}

	nodes  map[string]*node
	nMutex sync.RWMutex
	toAdd  map[string]state

	tag  string
	self string
	port int
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

func (c *Cluster) Write(p *gorilla.Point) gobol.Error {
	nodeID, err := c.ch.Get([]byte(p.ID))
	if err != nil {
		return errRequest("Write", http.StatusInternalServerError, err)
	}

	if nodeID == c.self {
		err := c.s.Write(p.KsID, p.ID, p.Timestamp, *p.Message.Value)
		if err != nil {
			errRequest("Write", http.StatusInternalServerError, err)
		}
		logger.WithFields(logrus.Fields{
			"package": "cluster",
			"func":    "cluster/Write",
		}).Debugf("point wrote in local node id=%v", nodeID)
		return nil
	}

	c.nMutex.RLock()
	node := c.nodes[nodeID]
	c.nMutex.RUnlock()

	logger.WithFields(logrus.Fields{
		"package": "cluster",
		"func":    "cluster/Write",
	}).Debugf("forwarding point to addr=%v port=%v", node.address, node.port)

	if p != nil {
		return node.write(&pb.TSPoint{
			Ksid:  p.KsID,
			Tsid:  p.ID,
			Date:  p.Timestamp,
			Value: *p.Message.Value,
		})
	}

	logger.WithFields(logrus.Fields{
		"package": "cluster",
		"func":    "cluster/Write",
	}).Debugf("nil pointer, not forwarding point to addr=%v port=%v", node.address, node.port)
	return nil

}

func (c *Cluster) Read(ksid, tsid string, start, end int64) ([]*pb.Point, gobol.Error) {

	nodeID, err := c.ch.Get([]byte(tsid))
	if err != nil {
		return nil, errRequest("Read", http.StatusInternalServerError, err)
	}

	if nodeID == c.self {
		logger.WithFields(logrus.Fields{
			"package": "cluster",
			"func":    "cluster/Read",
		}).Debugf("reading from local node id=%v", nodeID)
		return c.s.Read(ksid, tsid, start, end)
	}

	c.nMutex.RLock()
	node := c.nodes[nodeID]
	c.nMutex.RUnlock()

	logger.WithFields(logrus.Fields{
		"package": "cluster",
		"func":    "cluster/Read",
	}).Debugf("forwarding read to addr=%v port=%v", node.address, node.port)

	return node.read(ksid, tsid, start, end)
}

func (c *Cluster) shard() {
	series := c.s.ListSeries()

	for _, s := range series {
		n, err := c.ch.Get([]byte(s.TSID))
		if err != nil {
			logger.WithFields(logrus.Fields{
				"package": "cluster",
				"func":    "cluster/shard",
			}).Error(err)
			continue
		}
		if len(n) > 0 && n != c.self {
			c.nMutex.RLock()
			node := c.nodes[n]
			c.nMutex.RUnlock()

			ptsC := c.s.Delete(s)
			for pts := range ptsC {
				for _, p := range pts {
					node.write(&pb.TSPoint{
						Tsid:  s.TSID,
						Ksid:  s.KSID,
						Date:  p.Date,
						Value: p.Value,
					})
				}
			}
		}
	}

}

func (c *Cluster) getNodes() {
	srvs, err := c.c.getNodes()
	if err != nil {
		logger.Error(err)
	}

	now := time.Now().Unix()
	reShard := false

	for _, srv := range srvs {

		if c.self == srv.Node.ID {
			continue
		}

		for _, tag := range srv.Service.Tags {
			if tag == c.tag {

				for _, check := range srv.Checks {
					if check.ServiceID == srv.Service.ID && check.Status == "passing" {

						_, ok := c.nodes[srv.Node.ID]

						if !ok {

							if s, ok := c.toAdd[srv.Node.ID]; ok {
								if s.add {
									if now-s.time >= c.apply {

										if c.self != srv.Node.ID {

											n, err := newNode(srv.Node.Address, c.port, *c.cfg)
											if err != nil {
												logger.Error(err)
												continue
											}

											c.ch.Add(srv.Node.ID)

											c.nMutex.Lock()
											c.nodes[srv.Node.ID] = n
											c.nMutex.Unlock()

											delete(c.toAdd, srv.Node.ID)
											reShard = true

											logger.WithFields(logrus.Fields{
												"package": "cluster",
												"func":    "cluster/getNodes",
											}).Debugf("added node address=%v port=%v", n.address, n.port)
										}

									}
								} else {
									c.toAdd[srv.Node.ID] = state{
										add:  true,
										time: now,
									}
								}
							} else {
								c.toAdd[srv.Node.ID] = state{
									add:  true,
									time: now,
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
				if now-s.time >= c.apply {

					c.ch.Remove(id)

					c.nMutex.Lock()

					delete(c.nodes, id)
					c.nMutex.Unlock()

					delete(c.toAdd, id)
					reShard = true

					logger.WithFields(logrus.Fields{
						"package": "cluster",
						"func":    "cluster/getNodes",
					}).Debugf("removed node %v", id)

				}
			} else {
				c.toAdd[id] = state{
					add:  false,
					time: now,
				}
			}
		} else {
			c.toAdd[id] = state{
				add:  false,
				time: now,
			}
		}
	}

	if reShard {
		go c.shard()
	}

}

//Stop cluster
func (c *Cluster) Stop() {
	c.stopServ <- struct{}{}
	c.server.grpcServer.GracefulStop()
}
