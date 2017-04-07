package storage

import (
	"bytes"
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
)

const (
	maxFileSize    = 1 * 1024 * 1024
	maxBufferSize  = 10000
	fileSuffixName = "write-after.log"
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
	pts        [][]walPoint
	buffer     []walPoint
	pool       sync.Pool
	mtx        sync.Mutex
}

// NewWAL returns a WAL
func NewWAL(path string) (*WAL, error) {

	wal := &WAL{
		path:       path,
		stopCh:     make(chan struct{}),
		stopSyncCh: make(chan struct{}),
		writeCh:    make(chan walPoint, 10000),
		syncCh:     make(chan []walPoint, maxBufferSize),
		pts:        [][]walPoint{},
		buffer:     []walPoint{},
	}

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

	wal.pool = sync.Pool{
		New: func() interface{} {
			buff := make([]Point, maxBufferSize)
			return buff
		},
	}

	return wal, err

}

// Start dispatchs a goroutine with a ticker
// to save and sync points in disk
func (wal *WAL) Start() {

	go func() {

		for {
			select {
			case buffer := <-wal.syncCh:
				err := wal.write(buffer)
				if err != nil {
					panic(err)
				}
				wal.sync()

			case <-wal.stopSyncCh:
				return
			}

		}
	}()

	go func() {
		ticker := time.NewTicker(time.Second)
		wal.buffer = wal.pool.Get().([]walPoint)
		buffTimer := time.Now()
		index := 0
		for {
			select {
			case pt := <-wal.writeCh:
				if index >= maxBufferSize {
					wal.syncCh <- wal.buffer[:index-1]
					wal.buffer = wal.pool.Get().([]walPoint)
					index = 0
					buffTimer = time.Now()
				}
				wal.buffer = append(wal.buffer, pt)
				index++

			case <-ticker.C:
				if time.Now().Sub(buffTimer) >= time.Second {
					wal.syncCh <- wal.buffer[:index-1]
					wal.buffer = wal.pool.Get().([]walPoint)
					index = 0
					buffTimer = time.Now()
				}

			case <-wal.stopCh:
				wal.stopSyncCh <- struct{}{}
				for pt := range wal.writeCh {
					wal.buffer = append(wal.buffer, pt)
					index++
				}
				wal.write(wal.buffer[:index])
				// LOG ERROR
				wal.sync()

				return

			}

		}
	}()

}

// Add append point at the of the file
func (wal *WAL) Add(ksid, tsid string, date int64, value float32) {

	wal.writeCh <- walPoint{
		KSID: ksid,
		TSID: tsid,
		T:    date,
		V:    value,
	}

}

func (wal *WAL) sync() {
	err := wal.fd.Sync()
	if err != nil {
		panic(err)
	}
}

func (wal *WAL) write(buffer []walPoint) error {

	if len(buffer) == 0 {
		return nil
	}
	b := new(bytes.Buffer)
	encoder := gob.NewEncoder(b)

	err := encoder.Encode(buffer)
	if err != nil {
		return err
	}

	wal.pool.Put(buffer)

	encodePts := make([]byte, len(buffer))
	compressed := snappy.Encode(encodePts, b.Bytes())
	b.Reset()

	size := make([]byte, 4)
	binary.BigEndian.PutUint32(size, uint32(len(compressed)))

	if _, err := wal.fd.Write(size); err != nil {
		return err
	}

	if _, err := wal.fd.Write(compressed); err != nil {
		return err
	}

	stat, err := wal.fd.Stat()
	if err != nil {
		return err
	}

	if stat.Size() > maxFileSize {
		return wal.newFile()
	}

	return nil
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
func (wal *WAL) load(s *Storage) error {
	names, err := wal.listFiles()
	if err != nil {
		return err
	}

	for _, filepath := range names {
		fileData, err := ioutil.ReadFile(filepath)
		if err != nil {
			return err
		}

		size := 4
		for len(fileData) >= size {

			length := binary.BigEndian.Uint32(fileData[:size])

			fileData = fileData[size:]

			if len(fileData) < int(length) {
				// Add log here
				break
			}

			decLen, err := snappy.DecodedLen(fileData[:length])
			if err != nil {
				return err
			}
			buf := make([]byte, decLen)

			data, err := snappy.Decode(buf, fileData[:length])
			if err != nil {
				return err
			}

			fileData = fileData[length:]

			buffer := bytes.NewBuffer(data)

			decoder := gob.NewDecoder(buffer)

			pts := []walPoint{}

			if err := decoder.Decode(pts); err != nil {
				return err
			}

			for _, pt := range pts {
				s.getSerie(pt.KSID, pt.TSID).addPoint(s.Cassandra, pt.KSID, pt.TSID, pt.T, pt.V)
			}

		}
	}
	return nil
}
