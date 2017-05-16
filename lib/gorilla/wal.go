package gorilla

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/snappy"
	"go.uber.org/zap"
)

const (
	maxFileSize    = 10 * 1024 * 1024
	maxBufferSize  = 10000
	fileSuffixName = "waf.log"
)

// Point must be exported to satisfy gob.Encode
type walPoint struct {
	KSID string
	TSID string
	T    int64
	V    float32
}

// WAL - Write-Ahead-Log
// Mycenae uses write-after-log, we save the point in memory
// and after a couple seconds at the log file.
type WAL struct {
	path       string
	id         int64
	created    int64
	stopCh     chan struct{}
	stopSyncCh chan struct{}
	writeCh    chan walPoint
	syncCh     chan []walPoint
	fd         *os.File
	mtx        sync.Mutex
	get        chan []walPoint
	give       chan []walPoint
}

// NewWAL returns a WAL
func NewWAL(path string) (*WAL, error) {

	wal := &WAL{
		path:       path,
		stopCh:     make(chan struct{}),
		stopSyncCh: make(chan struct{}),
		writeCh:    make(chan walPoint, 10000),
		syncCh:     make(chan []walPoint, maxBufferSize),
	}

	wal.get, wal.give = wal.recycler()

	if err := os.MkdirAll(path, 0777); err != nil {
		return nil, err
	}

	names, err := wal.listFiles()

	if len(names) > 0 {
		lastWal := names[len(names)-1]
		id, err := idFromFileName(lastWal)
		if err != nil {
			return nil, err
		}

		wal.id = id
		stat, err := os.Stat(lastWal)
		if err != nil {
			return nil, err
		}

		if stat.Size() == 0 {
			os.Remove(lastWal)
			names = names[:len(names)-1]
		}
	}

	if err := wal.newFile(); err != nil {
		return nil, err
	}

	return wal, err

}

// Start dispatchs a goroutine with a ticker
// to save and sync points in disk
func (wal *WAL) Start() {

	go func() {
		ticker := time.NewTicker(time.Hour)
		for {
			select {
			case <-ticker.C:
				go wal.cleanup()
			}
		}

	}()

	go func() {

		ticker := time.NewTicker(time.Second)
		buffer := [maxBufferSize]walPoint{}
		buffTimer := time.Now()
		index := 0

		for {
			select {
			case pt := <-wal.writeCh:

				buffer[index] = pt
				index++

				if index == maxBufferSize-1 {
					wal.write(buffer[:index])
					index = 0
				}

			case <-ticker.C:
				if time.Now().Sub(buffTimer) >= time.Second {
					if index > 0 {
						wal.write(buffer[:index])
						index = 0
					}
				}
			case <-wal.stopCh:
				wal.stopSyncCh <- struct{}{}
				for pt := range wal.writeCh {
					buffer[index] = pt
					index++
					if index == maxBufferSize-1 {
						wal.write(buffer[:index])
						index = 0
					}
				}
				wal.write(buffer[:index])

				return

			}

		}
	}()

}

// Add append point at the end of the file
func (wal *WAL) Add(ksid, tsid string, date int64, value float32) {

	wal.writeCh <- walPoint{
		KSID: ksid,
		TSID: tsid,
		T:    date,
		V:    value,
	}

}

func (wal *WAL) makeBuffer() []walPoint {

	return make([]walPoint, maxBufferSize)

}

type queued struct {
	when  time.Time
	slice []walPoint
}

func (wal *WAL) recycler() (get, give chan []walPoint) {

	get = make(chan []walPoint)
	give = make(chan []walPoint)

	go func() {
		q := new(list.List)
		for {
			if q.Len() == 0 {
				q.PushFront(queued{when: time.Now(), slice: wal.makeBuffer()})
			}

			e := q.Front()

			timeout := time.NewTimer(time.Minute)
			select {
			case b := <-give:
				timeout.Stop()
				q.PushFront(queued{when: time.Now(), slice: b})

			case get <- e.Value.(queued).slice:
				timeout.Stop()
				q.Remove(e)

			case <-timeout.C:
				e := q.Front()
				for e != nil {
					n := e.Next()
					if time.Since(e.Value.(queued).when) > time.Minute {
						q.Remove(e)
						e.Value = nil
					}
					e = n
				}
			}
		}

	}()

	return

}

func (wal *WAL) write(pts []walPoint) {

	buffer := <-wal.get
	copy(buffer[:len(pts)], pts)

	go func() {

		b := new(bytes.Buffer)
		encoder := gob.NewEncoder(b)

		err := encoder.Encode(buffer)
		if err != nil {
			gblog.Sugar().Errorf("error creating buffer to be saved at commitlog: %v", err)
			wal.give <- buffer
			return
		}

		encodePts := make([]byte, len(buffer))
		compressed := snappy.Encode(encodePts, b.Bytes())
		b.Reset()

		size := make([]byte, 4)
		binary.BigEndian.PutUint32(size, uint32(len(compressed)))

		wal.mtx.Lock()
		defer wal.mtx.Unlock()
		if _, err := wal.fd.Write(size); err != nil {
			gblog.Sugar().Errorf("error writing header to commitlog: %v", err)
			wal.give <- buffer
			return
		}

		if _, err := wal.fd.Write(compressed); err != nil {
			gblog.Sugar().Errorf("error writing data to commitlog: %v", err)
			wal.give <- buffer
			return
		}

		stat, err := wal.fd.Stat()
		if err != nil {
			gblog.Sugar().Errorf("error doing stat at commitlog: %v", err)
			wal.give <- buffer
			return
		}

		err = wal.fd.Sync()
		if err != nil {
			gblog.Sugar().Errorf("error sycing data to commitlog: %v", err)
			wal.give <- buffer
			return
		}

		gblog.Sugar().Debugf("%05d-%s synced", wal.id, fileSuffixName)
		if stat.Size() > maxFileSize {
			err = wal.newFile()
			if err != nil {
				gblog.Sugar().Errorf("error creating new commitlog: %v", err)
			}
		}
		wal.give <- buffer
	}()
}

// idFromFileName parses the file ID from its name.
func idFromFileName(name string) (int64, error) {
	fileNameParts := strings.Split(filepath.Base(name), "-")
	if len(fileNameParts) < 2 {
		return 0, fmt.Errorf("%s has wrong format name", name)
	}
	return strconv.ParseInt(fileNameParts[0], 10, 32)
}

// newFile will close the current file and open a new one
func (wal *WAL) newFile() error {
	if wal.fd != nil {
		if err := wal.fd.Close(); err != nil {
			return err
		}
	}

	wal.id++
	fileName := filepath.Join(wal.path, fmt.Sprintf("%05d-%s", wal.id, fileSuffixName))
	fd, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}

	wal.created = time.Now().Unix()
	wal.fd = fd

	return nil
}

func (wal *WAL) listFiles() ([]string, error) {

	names, err := filepath.Glob(
		filepath.Join(
			wal.path,
			fmt.Sprintf("*-%s", fileSuffixName),
		))

	sort.Strings(names)

	return names, err

}
func (wal *WAL) load() <-chan []walPoint {

	ptsChan := make(chan []walPoint)

	go func() {

		defer close(ptsChan)

		names, err := wal.listFiles()
		if err != nil {
			gblog.Sugar().Errorf("error getting list of files: %v", err)
			return
		}

		fCount := len(names) - 1

		//lastWal := names[len(names)-1]

		var lastTimestamp int64
		for {
			if fCount < 0 {
				break
			}

			filepath := names[fCount]

			gblog.Sugar().Infof("loading %v", filepath)
			fileData, err := ioutil.ReadFile(filepath)
			if err != nil {
				gblog.Sugar().Errorf("error reading %v: %v", filepath, err)
				return
			}

			size := 4
			for len(fileData) >= size {

				length := binary.BigEndian.Uint32(fileData[:size])

				fileData = fileData[size:]

				if len(fileData) < int(length) {
					gblog.Error("unable to read data from file, sizes don't match")
					break
				}

				decLen, err := snappy.DecodedLen(fileData[:length])
				if err != nil {
					gblog.Sugar().Errorf("decode header %v bytes from file: %v", length, err)
					return
				}
				buf := make([]byte, decLen)

				data, err := snappy.Decode(buf, fileData[:length])
				if err != nil {
					gblog.Sugar().Errorf("decode data %v bytes from file: %v", length, err)
					return
				}

				fileData = fileData[length:]

				buffer := bytes.NewBuffer(data)

				decoder := gob.NewDecoder(buffer)

				pts := []walPoint{}

				if err := decoder.Decode(&pts); err != nil {
					gblog.Sugar().Errorf("unable to decode points from file %v: %v", filepath, err)
					return
				}

				rp := []walPoint{}
				for _, p := range pts {
					if len(p.KSID) > 0 && len(p.TSID) > 0 && p.T > 0 {

						if p.T > lastTimestamp {
							lastTimestamp = p.T
						}
						delta := lastTimestamp - p.T
						if delta <= int64(2*hour) {
							rp = append(rp, p)
						}
					}
				}

				ptsChan <- rp
			}
			fCount--
		}
		return
	}()

	return ptsChan
}

func (wal *WAL) cleanup() {

	timeout := time.Now().UTC().Add(-4 * time.Hour)

	names, err := wal.listFiles()
	if err != nil {
		gblog.Error("Error getting list of files", zap.Error(err))
	}

	for _, f := range names {

		stat, err := os.Stat(f)
		if err != nil {
			gblog.Sugar().Errorf("error to stat file %v: %v", f, err)
		}

		if !stat.ModTime().UTC().After(timeout) {
			gblog.Sugar().Infof("removing write-ahead file %v", f)
			err = os.Remove(f)
			if err != nil {
				gblog.Sugar().Errorf("error to remove file %v: %v", f, err)
			}
		}

	}

}
