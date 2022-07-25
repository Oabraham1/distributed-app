package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

var (
	offsetWidth   uint64 = 4
	positionWidth uint64 = 8
	entryWidth           = offsetWidth + positionWidth
)

type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

func newIndex(file *os.File, config Config) (*index, error) {
	idx := &index{
		file: file,
	}
	fi, err := os.Stat(file.Name())
	if err != nil {
		return nil, err
	}
	idx.size = uint64(fi.Size())
	if err = os.Truncate(
		file.Name(), int64(config.Segment.MaxIndexBytes),
	); err != nil {
		return nil, err
	}
	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}
	return idx, nil
}

func (idx *index) Close() error {
	if err := idx.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	if err := idx.file.Sync(); err != nil {
		return err
	}
	if err := idx.file.Truncate(int64(idx.size)); err != nil {
		return err
	}
	return idx.file.Close()
}

func (idx *index) Read(input int64) (output uint32, position uint64, err error) {
	if idx.size == 0 {
		return 0, 0, io.EOF
	}
	if input == -1 {
		output = uint32((idx.size / entryWidth) - 1)
	} else {
		output = uint32(input)
	}
	position = uint64(output) * entryWidth
	if idx.size < position+entryWidth {
		return 0, 0, io.EOF
	}
	output = encoding.Uint32(idx.mmap[position : position+offsetWidth])
	position = encoding.Uint64(idx.mmap[position+offsetWidth : position+entryWidth])
	return output, position, nil
}

func (idx *index) Write(offset uint32, position uint64) error {
	if uint64(len(idx.mmap)) < idx.size+entryWidth {
		return io.EOF
	}
	encoding.PutUint32(idx.mmap[idx.size:idx.size+offsetWidth], offset)
	encoding.PutUint64(idx.mmap[idx.size+offsetWidth:idx.size+entryWidth], position)
	idx.size += uint64(entryWidth)
	return nil
}

func (idx *index) Name() string {
	return idx.file.Name()
}
