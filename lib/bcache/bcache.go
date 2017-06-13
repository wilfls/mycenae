package bcache

import (
	"net/http"
	"sync"

	"github.com/uol/gobol"

	"github.com/uol/mycenae/lib/keyspace"
	"github.com/uol/mycenae/lib/tsstats"
)

var (
	stats *tsstats.StatsTS
)

//New creates a a struct that "caches" timeseries keys. It uses boltdb as persistence
func New(sts *tsstats.StatsTS, ks *keyspace.Keyspace, path string) (*Bcache, gobol.Error) {

	stats = sts

	persist, gerr := newBolt(path)
	if gerr != nil {
		return nil, gerr
	}

	b := &Bcache{
		kspace:  ks,
		persist: persist,
		mcache:  make(map[string]uint),
	}

	go b.load()

	return b, nil
}

//Bcache is responsible for caching timeseries keys from elasticsearch
type Bcache struct {
	kspace  *keyspace.Keyspace
	persist *persistence
	mcache  map[string]uint
	mtx     sync.RWMutex
}

func (bc *Bcache) load() {
	bc.mtx.Lock()
	defer bc.mtx.Unlock()

	for _, kv := range bc.persist.Load([]byte("number")) {
		bc.mcache[string(kv.K)] = 0
	}

}

//GetKeyspace returns a keyspace key, a boolean that tells if the key was found or not and an error.
//If the key isn't in boltdb GetKeyspace tries to fetch the key from cassandra, and if found, puts it in boltdb.
func (bc *Bcache) GetKeyspace(key string) (string, bool, gobol.Error) {

	v, gerr := bc.persist.Get([]byte("keyspace"), []byte(key))
	if gerr != nil {
		return "", false, gerr
	}
	if v != nil {
		return string(v), true, nil
	}

	ks, found, gerr := bc.kspace.GetKeyspace(key)
	if gerr != nil {
		if gerr.StatusCode() == http.StatusNotFound {
			return "", false, nil
		}
		return "", false, gerr
	}
	if !found {
		return "", false, nil
	}

	value := "false"

	if ks.TUUID {
		value = "true"
	}

	gerr = bc.persist.Put([]byte("keyspace"), []byte(key), []byte(value))
	if gerr != nil {
		return "", false, gerr
	}

	return value, true, nil
}

func (bc *Bcache) GetTsNumber(key string, CheckTSID func(esType, id string) (bool, gobol.Error)) (bool, gobol.Error) {
	bc.mtx.RLock()
	if _, ok := bc.mcache[key]; ok {
		return true, nil
	}
	bc.mtx.RUnlock()

	return bc.getTSID("meta", "number", key, CheckTSID)
}

func (bc *Bcache) GetTsText(key string, CheckTSID func(esType, id string) (bool, gobol.Error)) (bool, gobol.Error) {
	return bc.getTSID("metatext", "text", key, CheckTSID)
}

func (bc *Bcache) getTSID(esType, bucket, key string, CheckTSID func(esType, id string) (bool, gobol.Error)) (bool, gobol.Error) {

	v, gerr := bc.persist.Get([]byte(bucket), []byte(key))
	if gerr != nil {
		return false, gerr
	}
	if v != nil {
		return true, nil
	}

	found, gerr := CheckTSID(esType, key)
	if gerr != nil {
		return false, gerr
	}
	if !found {
		return false, nil
	}

	gerr = bc.persist.Put([]byte(bucket), []byte(key), []byte{})
	if gerr != nil {
		return false, gerr
	}
	bc.mtx.Lock()
	bc.mcache[key] = 0
	bc.mtx.Unlock()

	return true, nil
}
