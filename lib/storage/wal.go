package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/golang/snappy"
)

const (
	maxFileSize = 20 * 1024 * 1024
)

// Point must be exported to satisfy gob.Encode
type Point struct {
	ID string
	T  int64
	V  float64
}

// WAL - Write-Ahead-Log
// Mycenae uses write-after-log, we save the point in memory
// and after a couple seconds at the log file.
type WAL struct {
	path     string
	index    int
	StopCh   chan struct{}
	WriteCh  chan Point
	w        io.WriteCloser
	pts      *[]Point
	ptsIndex int
}

// NewWAL returns a WAL
func (s *Storage) NewWAL(path string) (*WAL, error) {

	l := &WAL{
		path:    path,
		StopCh:  make(chan struct{}),
		WriteCh: make(chan Point, 10000),
		pts:     &[]Point{},
	}

	if err := os.MkdirAll(l.path, 0777); err != nil {
		return nil, err
	}

	names, err := l.listFiles()

	if len(names) > 0 {
		lastWal := names[len(names)-1]
		id, err := idFromFileName(lastWal)
		if err != nil {
			return nil, err
		}

		l.index = id
		stat, err := os.Stat(lastWal)
		if err != nil {
			return nil, err
		}

		if stat.Size() == 0 {
			os.Remove(lastWal)
			names = names[:len(names)-1]
		}
	}

	if err := l.newFile(); err != nil {
		return nil, err
	}

	return l, err

}

// Start dispatchs a goroutine with a ticker
// to save and sync points in disk
func (l *WAL) Start() {
	ticker := time.NewTicker(time.Second * 2)

	for {
		select {
		case <-ticker.C:
			err := l.write()
			if err != nil {
				panic(err)
			}
			l.sync()
		case pt := <-l.WriteCh:

			*l.pts = append(*l.pts, pt)

		case <-l.StopCh:
			// write to log and return
			return
		}
	}

}

func (l *WAL) sync() {
	err := l.w.(*os.File).Sync()
	if err != nil {
		panic(err)
	}
}

func (l *WAL) write() error {

	if len(*l.pts) == 0 {
		return nil
	}
	b := new(bytes.Buffer)
	encoder := gob.NewEncoder(b)

	pts := *l.pts

	err := encoder.Encode(pts)
	if err != nil {
		return err
	}

	encodePts := make([]byte, len(pts))

	compressed := snappy.Encode(encodePts, b.Bytes())

	size := make([]byte, 4)
	binary.BigEndian.PutUint32(size, uint32(len(compressed)))

	fd := l.w.(*os.File)

	if _, err := fd.Write(size); err != nil {
		return err
	}

	if _, err := fd.Write(compressed); err != nil {
		return err
	}

	stat, err := fd.Stat()
	if err != nil {
		return err
	}

	l.pts = &[]Point{}
	if stat.Size() > maxFileSize {
		return l.newFile()
	}

	return nil
}

// idFromFileName parses the file ID from its name.
func idFromFileName(name string) (int, error) {
	parts := strings.Split(filepath.Base(name), ".")
	if len(parts) != 2 {
		return 0, fmt.Errorf("file %s has wrong name format to have an id", name)
	}

	id, err := strconv.ParseUint(parts[0][1:], 10, 32)

	return int(id), err
}

// newFile will close the current file and open a new one
func (l *WAL) newFile() error {
	l.index++
	if l.w != nil {
		if err := l.w.Close(); err != nil {
			return err
		}
	}

	fileName := filepath.Join(l.path, fmt.Sprintf("%s%05d.%s", "_", l.index, "wal"))
	fd, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	l.w = fd

	return nil
}

func (l *WAL) listFiles() ([]string, error) {

	names, err := filepath.Glob(
		filepath.Join(
			l.path,
			fmt.Sprintf("%s*.%s", "_", "wal"),
		))

	sort.Strings(names)

	return names, err

}
func (l *WAL) load(s *Storage) error {
	names, err := l.listFiles()
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

			pts := &[]Point{}

			if err := decoder.Decode(pts); err != nil {
				return err
			}

			for _, pt := range *pts {
				s.getSerie(pt.ID).addPoint(pt.T, pt.V)
			}

		}
	}
	return nil
}
