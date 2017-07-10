package bcache

import (
	"net/http"
	"sync"
	"time"

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
		tsmap:   make(map[string]int64),
		ksmap:   make(map[string]int64),
	}

	go b.load()
	b.start()

	return b, nil
}

//Bcache is responsible for caching timeseries keys from elasticsearch
type Bcache struct {
	kspace  *keyspace.Keyspace
	persist *persistence
	tsmap   map[string]int64
	ksmap   map[string]int64
	ksmtx   sync.RWMutex
	tsmtx   sync.RWMutex
}

func (bc *Bcache) tsIter() []string {
	var tsKeys []string
	now := time.Now().Unix()
	bc.tsmtx.RLock()
	for k, t := range bc.tsmap {
		bc.tsmtx.RUnlock()
		if t-now >= 86400 {
			tsKeys = append(tsKeys, k)
		}
		bc.tsmtx.RLock()
	}
	bc.tsmtx.RUnlock()

	return tsKeys
}

func (bc *Bcache) ksIter() []string {
	var ksKeys []string
	now := time.Now().Unix()
	bc.ksmtx.RLock()
	for k, t := range bc.ksmap {
		bc.ksmtx.RUnlock()
		if t-now >= 86400 {
			ksKeys = append(ksKeys, k)
		}
		bc.ksmtx.RLock()
	}
	bc.ksmtx.RUnlock()

	return ksKeys
}

func (bc *Bcache) load() {
	bc.tsmtx.Lock()
	defer bc.tsmtx.Unlock()

	bc.tsmtx.Lock()
	defer bc.tsmtx.Unlock()
	for _, kv := range bc.persist.Load([]byte("number")) {

		bc.tsmap[string(kv.K)] = time.Now().Unix()
	}

}

func (bc *Bcache) start() {
	go func() {
		ticker := time.NewTicker(1 * time.Hour)
		for {
			select {

			case <-ticker.C:
				for _, v := range bc.ksIter() {
					bc.ksmtx.Lock()
					delete(bc.ksmap, v)
					bc.ksmtx.Unlock()
				}
				for _, v := range bc.tsIter() {
					bc.tsmtx.Lock()
					delete(bc.tsmap, v)
					bc.tsmtx.Unlock()
				}

			}
		}
	}()
}

//GetKeyspace returns a keyspace key, a boolean that tells if the key was found or not and an error.
//If the key isn't in boltdb GetKeyspace tries to fetch the key from cassandra, and if found, puts it in boltdb.
func (bc *Bcache) GetKeyspace(key string) (string, bool, gobol.Error) {

	bc.ksmtx.RLock()
	_, ok := bc.ksmap[key]
	bc.ksmtx.RUnlock()

	if ok {
		bc.ksmtx.Lock()
		defer bc.ksmtx.Unlock()
		bc.ksmap[key] = time.Now().Unix()
		return string(key), true, nil
	}

	v, gerr := bc.persist.Get([]byte("keyspace"), []byte(key))
	if gerr != nil {
		return "", false, gerr
	}
	if v != nil {
		bc.ksmtx.Lock()
		defer bc.ksmtx.Unlock()
		bc.ksmap[key] = time.Now().Unix()

		return key, true, nil
	}

	_, found, gerr := bc.kspace.GetKeyspace(key)
	if gerr != nil {
		if gerr.StatusCode() == http.StatusNotFound {
			return "", false, nil
		}
		return "", false, gerr
	}
	if !found {
		return "", false, nil
	}

	gerr = bc.persist.Put([]byte("keyspace"), []byte(key), []byte("false"))
	if gerr != nil {
		return "", false, gerr
	}

	bc.ksmtx.Lock()
	defer bc.ksmtx.Unlock()
	bc.ksmap[key] = time.Now().Unix()

	return key, true, nil
}

func (bc *Bcache) GetTsNumber(key string, CheckTSID func(esType, id string) (bool, gobol.Error)) (bool, gobol.Error) {

	return bc.getTSID("meta", "number", key, CheckTSID)
}

func (bc *Bcache) GetTsText(key string, CheckTSID func(esType, id string) (bool, gobol.Error)) (bool, gobol.Error) {
	return bc.getTSID("metatext", "text", key, CheckTSID)
}

func (bc *Bcache) Get(key *string) bool {

	bc.tsmtx.RLock()
	_, ok := bc.tsmap[*key]
	bc.tsmtx.RUnlock()

	if ok {
		bc.tsmtx.Lock()
		defer bc.tsmtx.Unlock()
		bc.tsmap[*key] = time.Now().Unix()
	}

	return ok

}

func (bc *Bcache) Set(key string) {

	gerr := bc.persist.Put([]byte("number"), []byte(key), []byte{})
	if gerr != nil {
		return
	}

	bc.tsmtx.Lock()
	defer bc.tsmtx.Unlock()
	bc.tsmap[key] = time.Now().Unix()

}

func (bc *Bcache) getTSID(esType, bucket, key string, CheckTSID func(esType, id string) (bool, gobol.Error)) (bool, gobol.Error) {

	bc.tsmtx.Lock()
	_, ok := bc.tsmap[key]
	if ok {
		bc.tsmap[key] = time.Now().Unix()
		bc.tsmtx.Unlock()
		return ok, nil
	}
	bc.tsmtx.Unlock()

	go func() {
		v, gerr := bc.persist.Get([]byte(bucket), []byte(key))
		if gerr != nil {
			return
		}
		if v != nil {
			bc.tsmtx.Lock()
			defer bc.tsmtx.Unlock()
			bc.tsmap[key] = time.Now().Unix()
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
		defer bc.tsmtx.Unlock()
		bc.tsmap[key] = time.Now().Unix()

		return
	}()

	return false, nil
}
