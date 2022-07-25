package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	encoding = binary.BigEndian
)

const (
	lenWidth = 8
)

type store struct {
	*os.File
	mutex  sync.Mutex
	buffer *bufio.Writer
	size   uint64
}

func newStore(file *os.File) (*store, error) {
	fi, err := os.Stat(file.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fi.Size())
	return &store{
		File:   file,
		size:   size,
		buffer: bufio.NewWriter(file),
	}, nil
}

func (store *store) Append(data []byte) (n uint64, position uint64, err error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	position = store.size
	if err := binary.Write(store.buffer, encoding, uint64(len(data))); err != nil {
		return 0, 0, err
	}
	write, err := store.buffer.Write(data)
	if err != nil {
		return 0, 0, err
	}
	write += lenWidth
	store.size += uint64(write)
	return uint64(write), position, nil
}

func (store *store) Read(position uint64) ([]byte, error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	if err := store.buffer.Flush(); err != nil {
		return nil, err
	}
	size := make([]byte, lenWidth)
	if _, err := store.File.ReadAt(size, int64(position)); err != nil {
		return nil, err
	}
	data := make([]byte, encoding.Uint64(size))
	if _, err := store.File.ReadAt(data, int64(position+lenWidth)); err != nil {
		return nil, err
	}
	return data, nil
}

func (store *store) ReadAt(data []byte, offset int64) (int, error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	if err := store.buffer.Flush(); err != nil {
		return 0, err
	}
	return store.File.ReadAt(data, offset)
}

func (store *store) Close() error {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	err := store.buffer.Flush()
	if err != nil {
		return err
	}
	return store.File.Close()
}
