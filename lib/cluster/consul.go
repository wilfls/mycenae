package cluster

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/uol/gobol"
)

type ConsulConfig struct {
	//Consul agent adrress without the scheme
	Address string
	//Consul agent port
	Port int
	//Location of consul agent cert file
	Cert string
	//Location of consul agent key file
	Key string
	//Location of consul agent CA file
	CA string
	//Name of the service to be probed on consul
	Service string
	//Tag of the service
	Tag string
	// Token of the service
	Token string
}

type Health struct {
	Node    Node    `json:"Node"`
	Service Service `json:"Service"`
	Checks  []Check `json:"Checks"`
}

type Node struct {
	ID              string            `json:"ID"`
	Node            string            `json:"Node"`
	Address         string            `json:"Address"`
	TaggedAddresses TagAddr           `json:"TaggedAddresses"`
	Meta            map[string]string `json:"Meta"`
	CreateIndex     int               `json:"CreateIndex"`
	ModifyIndex     int               `json:"ModifyIndex"`
}

type TagAddr struct {
	Lan string `json:"lan"`
	Wan string `json:"wan"`
}

type Service struct {
	ID                string   `json:"ID"`
	Service           string   `json:"Service"`
	Tags              []string `json:"Tags"`
	Address           string   `json:"Address"`
	Port              int      `json:"Port"`
	EnableTagOverride bool     `json:"EnableTagOverride"`
	CreateIndex       int      `json:"CreateIndex"`
	ModifyIndex       int      `json:"ModifyIndex"`
}

type Check struct {
	Node        string `json:"Node"`
	CheckID     string `json:"CheckID"`
	Name        string `json:"Name"`
	Status      string `json:"Status"`
	Notes       string `json:"Notes"`
	Output      string `json:"Output"`
	ServiceID   string `json:"ServiceID"`
	ServiceName string `json:"ServiceName"`
	CreateIndex int    `json:"CreateIndex"`
	ModifyIndex int    `json:"ModifyIndex"`
}

type Addresses struct {
	Lan string `json:"lan"`
	Wan string `json:"wan"`
}

type Local struct {
	Config Conf `json:"Config"`
}

type Conf struct {
	NodeID string `json:"NodeID"`
}

func newConsul(conf ConsulConfig) (*consul, gobol.Error) {

	cert, err := tls.LoadX509KeyPair(conf.Cert, conf.Key)
	if err != nil {
		return nil, errInit("newConsul", err)
	}

	caCert, err := ioutil.ReadFile(conf.CA)
	if err != nil {
		return nil, errInit("newConsul", err)
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			Certificates: []tls.Certificate{cert},
			RootCAs:      caCertPool,
		},
	}

	return &consul{
		c: &http.Client{
			Transport: tr,
		},
		serviceAPI: fmt.Sprintf("https://%s:%d/v1/catalog/service/%s", conf.Address, conf.Port, conf.Service),
		agentAPI:   fmt.Sprintf("https://%s:%d/v1/agent/self", conf.Address, conf.Port),
		token:      conf.Token,
	}, nil
}

type consul struct {
	c          *http.Client
	token      string
	serviceAPI string
	agentAPI   string
}

func (c *consul) getNodes() ([]Health, gobol.Error) {

	req, err := http.NewRequest("GET", c.serviceAPI, nil)
	if err != nil {
		return nil, errRequest("getNodes", http.StatusInternalServerError, err)
	}
	req.Header.Add("X-Consul-Token", c.token)

	resp, err := c.c.Do(req)
	if err != nil {
		return nil, errRequest("getNodes", resp.StatusCode, err)
	}

	dec := json.NewDecoder(resp.Body)

	srvs := []Health{}

	err = dec.Decode(srvs)
	if err != nil {
		return nil, errRequest("getNodes", http.StatusInternalServerError, err)
	}

	return srvs, nil
}

func (c *consul) getSelf() (string, gobol.Error) {

	req, err := http.NewRequest("GET", c.agentAPI, nil)
	if err != nil {
		return "", errRequest("getSelf", http.StatusInternalServerError, err)
	}
	req.Header.Add("X-Consul-Token", c.token)

	resp, err := c.c.Do(req)
	if err != nil {
		return "", errRequest("getSelf", resp.StatusCode, err)
	}

	if resp.StatusCode >= 300 {
		return "", errRequest("getSelf", resp.StatusCode, err)
	}

	dec := json.NewDecoder(resp.Body)

	self := Local{}

	err = dec.Decode(&self)
	if err != nil {
		return "", errRequest("getSelf", http.StatusInternalServerError, err)
	}

	return self.Config.NodeID, nil
}
