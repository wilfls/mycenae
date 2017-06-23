package bcache

import (
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
		tsmap:   make(map[string]interface{}),
		ksmap:   make(map[string]interface{}),
	}

	go b.load()

	return b, nil

}

//Bcache is responsible for caching timeseries keys from elasticsearch
type Bcache struct {
	kspace  *keyspace.Keyspace
	persist *persistence
	tsmap   map[string]interface{}
	ksmap   map[string]interface{}
	ksmtx   sync.RWMutex
	tsmtx   sync.RWMutex
}

func (bc *Bcache) load() {
	bc.tsmtx.Lock()
	defer bc.tsmtx.Unlock()

	for _, kv := range bc.persist.Load([]byte("number")) {
		bc.tsmap[string(kv.K)] = nil
	}

}

//GetKeyspace returns a keyspace key, a boolean that tells if the key was found or not and an error.
//If the key isn't in boltdb GetKeyspace tries to fetch the key from cassandra, and if found, puts it in boltdb.
func (bc *Bcache) GetKeyspace(key string) (string, bool, gobol.Error) {

	bc.ksmtx.RLock()
	_, ok := bc.ksmap[key]
	bc.ksmtx.RUnlock()
	if ok {
		return string(key), true, nil
	}

	go func(key string) {
		v, gerr := bc.persist.Get([]byte("keyspace"), []byte(key))
		if gerr != nil {
			return
		}
		if v != nil {
			bc.ksmtx.Lock()
			bc.ksmap[key] = nil
			bc.ksmtx.Unlock()
			return
		}

		_, found, gerr := bc.kspace.GetKeyspace(key)
		if gerr != nil {
			return
		}
		if !found {
			return
		}

		gerr = bc.persist.Put([]byte("keyspace"), []byte(key), []byte("false"))
		if gerr != nil {
			return
		}

		bc.ksmtx.Lock()
		bc.ksmap[key] = nil
		bc.ksmtx.Unlock()
	}(key)

	return "", false, nil
}

func (bc *Bcache) GetTsNumber(key string, CheckTSID func(esType, id string) (bool, gobol.Error)) (bool, gobol.Error) {

	return bc.getTSID("meta", "number", key, CheckTSID)
}

func (bc *Bcache) GetTsText(key string, CheckTSID func(esType, id string) (bool, gobol.Error)) (bool, gobol.Error) {
	return bc.getTSID("metatext", "text", key, CheckTSID)
}

func (bc *Bcache) getTSID(esType, bucket, key string, CheckTSID func(esType, id string) (bool, gobol.Error)) (bool, gobol.Error) {

	bc.tsmtx.RLock()
	_, ok := bc.tsmap[key]
	bc.tsmtx.RUnlock()
	if ok {
		return true, nil
	}

	go func() {
		v, gerr := bc.persist.Get([]byte(bucket), []byte(key))
		if gerr != nil {
			return
		}
		if v != nil {
			bc.tsmtx.Lock()
			bc.tsmap[key] = nil
			bc.tsmtx.Unlock()
			return
		}

		found, gerr := CheckTSID(esType, key)
		if gerr != nil {
			return
		}
		if !found {
			return
		}

		gerr = bc.persist.Put([]byte(bucket), []byte(key), []byte{})
		if gerr != nil {
			return
		}

		bc.tsmtx.Lock()
		bc.tsmap[key] = nil
		bc.tsmtx.Unlock()
		return
	}()

	return false, nil
}
