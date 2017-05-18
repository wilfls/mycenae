package cluster

import (
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/billhathaway/consistentHash"
	"github.com/uol/gobol"

	"github.com/uol/mycenae/lib/gorilla"
	pb "github.com/uol/mycenae/lib/proto"
	"go.uber.org/zap"
)

var logger *zap.Logger

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

func New(log *zap.Logger, sto *gorilla.Storage, conf Config) (*Cluster, gobol.Error) {

	if sto == nil {
		return nil, errInit("New", errors.New("storage can't be nil"))
	}

	ci, err := time.ParseDuration(conf.CheckInterval)
	if err != nil {
		log.Error("", zap.Error(err))
		return nil, errInit("New", err)
	}

	c, gerr := newConsul(conf.Consul)
	if gerr != nil {
		log.Error("", zap.Error(gerr))
		return nil, gerr
	}

	s, gerr := c.getSelf()
	if gerr != nil {
		log.Error("", zap.Error(gerr))
		return nil, gerr
	}

	logger = log

	server, err := newServer(conf, sto)
	if err != nil {
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

		logger.Debug(
			"point written in local node",
			zap.String("package", "cluster"),
			zap.String("func", "Write"),
			zap.String("id", c.self),
		)
		return nil
	}

	c.nMutex.RLock()
	node := c.nodes[nodeID]
	c.nMutex.RUnlock()

	logger.Debug(
		"forwarding point",
		zap.String("package", "cluster"),
		zap.String("func", "Write"),
		zap.String("addr", node.address),
		zap.Int("port", node.port),
	)

	if p != nil {
		return node.write(&pb.TSPoint{
			Ksid:  p.KsID,
			Tsid:  p.ID,
			Date:  p.Timestamp,
			Value: *p.Message.Value,
		})
	}

	return nil
}

func (c *Cluster) Read(ksid, tsid string, start, end int64) ([]*pb.Point, gobol.Error) {

	ctxt := logger.With(
		zap.String("package", "cluster"),
		zap.String("func", "Read"),
	)

	nodeID, err := c.ch.Get([]byte(tsid))
	if err != nil {
		return nil, errRequest("Read", http.StatusInternalServerError, err)
	}

	if nodeID == c.self {
		ctxt.Debug("reading from local node")
		return c.s.Read(ksid, tsid, start, end)
	}

	c.nMutex.RLock()
	node := c.nodes[nodeID]
	c.nMutex.RUnlock()

	ctxt.Debug(
		"forwarding read",
		zap.String("addr", node.address),
		zap.Int("port", node.port),
	)

	return node.read(ksid, tsid, start, end)
}

func (c *Cluster) shard() {
	series := c.s.ListSeries()

	for _, s := range series {
		n, err := c.ch.Get([]byte(s.TSID))
		if err != nil {

			logger.Error(
				err.Error(),
				zap.String("package", "cluster"),
				zap.String("func", "shard"),
			)
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
		logger.Error("", zap.Error(err))
	}

	now := time.Now().Unix()
	reShard := false

	for _, srv := range srvs {

		logger.Debug(
			"updating nodes list",
			zap.String("package", "cluster"),
			zap.String("func", "getNodes"),
			zap.String("nodeIP", srv.Node.Address),
			zap.String("nodeID", srv.Node.ID),
		)

		if c.self == srv.Node.ID {
			continue
		}

		for _, tag := range srv.Service.Tags {
			if tag == c.tag {

				for _, check := range srv.Checks {
					if check.ServiceID == srv.Service.ID && check.Status == "passing" {

						node, ok := c.nodes[srv.Node.ID]
						if ok {
							if node.port != srv.Service.Port || node.address != srv.Node.Address {
								//node.close()
								n, err := newNode(srv.Node.Address, c.port, *c.cfg)
								if err != nil {
									logger.Error("", zap.Error(err))
								}

								c.nMutex.Lock()
								c.nodes[srv.Node.ID] = n
								c.nMutex.Unlock()
							}
						} else {

							if s, ok := c.toAdd[srv.Node.ID]; ok {
								if s.add {
									if now-s.time >= c.apply {

										n, err := newNode(srv.Node.Address, c.port, *c.cfg)
										if err != nil {
											logger.Error("", zap.Error(err))
											continue
										}

										c.ch.Add(srv.Node.ID)

										c.nMutex.Lock()
										c.nodes[srv.Node.ID] = n
										c.nMutex.Unlock()

										delete(c.toAdd, srv.Node.ID)
										reShard = true

										logger.Debug(
											"added node",
											zap.String("package", "cluster"),
											zap.String("func", "getNodes"),
											zap.String("address", n.address),
											zap.Int("port", n.port),
										)

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

					logger.Debug(
						"removed node",
						zap.String("package", "cluster"),
						zap.String("func", "getNodes"),
					)
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
