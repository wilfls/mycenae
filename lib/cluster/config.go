package cluster

import (
	"time"

	"github.com/hashicorp/serf/serf"
)

const (
	// DefaultPort is the default port for the serf protocol
	DefaultPort = 9875
	// DefaultSnapshot is the default value for the snapshot file
	DefaultSnapshot = "/tmp/snapshot.db"
	// DefaultKeyring is the default value for the keyring file
	DefaultKeyring = "/tmp/keyring.db"
)

// Settings are the configuration for the cluster
type Settings struct {
	Name string `toml:"nodeName,omitempty"`

	BindAddrress string `toml:"bindAddress,omitempty"`
	BindPort     int    `toml:"bindPort,omitempty"`

	AdvertiseAddress string `toml:"advertiseAddress,omitempty"`
	AdvertisePort    int    `toml:"advertisePort,omitempty"`

	Seed      []string `toml:"seedNodes,omitempty"`
	Frequency string   `toml:"frequency"`

	Snapshot string `toml:"snapshot,omitempty"`
	Keyring  string `toml:"keyring,omitempty"`
	// Encrypt  string `json:"encrypt,omitempty"`
}

func makeConfig(settings Settings) *serf.Config {
	serfConfig := serf.DefaultConfig()

	// Define the node name
	if settings.Name != "" {
		serfConfig.NodeName = settings.Name
	}

	// Define the bind address
	if settings.BindAddrress != "" {
		serfConfig.MemberlistConfig.BindAddr = settings.BindAddrress
	}

	// Define the advertise address
	if settings.AdvertiseAddress != "" {
		serfConfig.MemberlistConfig.AdvertiseAddr = settings.AdvertiseAddress
	}

	// Define the bind port
	bindPort := DefaultPort
	if settings.BindPort > 0 {
		bindPort = settings.BindPort
	}
	serfConfig.MemberlistConfig.BindPort = bindPort

	// Define the advertise port
	advertisePort := DefaultPort
	if settings.AdvertisePort > 0 {
		advertisePort = settings.AdvertisePort
	}
	serfConfig.MemberlistConfig.AdvertisePort = advertisePort

	snapshot := DefaultSnapshot
	if settings.Snapshot != "" {
		snapshot = settings.Snapshot
	}
	serfConfig.SnapshotPath = snapshot

	keyring := DefaultKeyring
	if settings.Keyring != "" {
		keyring = settings.Keyring
	}
	serfConfig.KeyringFile = keyring

	return serfConfig
}

func (settings Settings) getFrenquency() (time.Duration, error) {
	if settings.Frequency == "" {
		return time.Minute, nil
	}
	return time.ParseDuration(settings.Frequency)
}
