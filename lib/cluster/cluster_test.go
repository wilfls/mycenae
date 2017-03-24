package cluster

import (
	"fmt"
	"testing"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestCluster(t *testing.T) {
	var (
		size     = 5
		basePort = 9000

		seed = []string{fmt.Sprintf("127.0.0.1:%d", basePort)}

		nodes []*Cluster
	)

	for i := 0; i < size; i++ {
		node, err := New(Settings{
			Name: fmt.Sprintf("node-%d", i),

			AdvertisePort: basePort + i,
			BindPort:      basePort + i,

			Seed: seed,
		}, logrus.StandardLogger(), nil)
		if !assert.NoError(t, err) {
			return
		}
		nodes = append(nodes, node)
	}
	time.Sleep(time.Second * 5)
	assert.Equal(t, size, len(nodes[0].Members()))

	for _, node := range nodes {
		if !assert.NoError(t, node.Stop()) {
			return
		}
		time.Sleep(time.Second * 5)
	}
}
