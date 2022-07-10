package log

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	write = []byte("Project Logger Test")
	width = uint64(len(write)) + lenWidth
)

func openFile(filename string) (osFile *os.File, size int64, err error) {
	file, err := os.OpenFile(
		filename,
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, 0, err
	}
	fi, err := file.Stat()
	if err != nil {
		return nil, 0, err
	}
	return file, fi.Size(), nil
}

func TestStoreAppend(t *testing.T) {
	file, err := ioutil.TempFile("", "store_append_read_test")
	require.NoError(t, err)
	defer os.Remove(file.Name())

	store, err := newStore(file)
	require.NoError(t, err)

	for i := uint64(1); i < 4; i++ {
		n, position, err := store.Append((write))
		require.NoError(t, err)
		require.Equal(t, position+n, width*i)
	}
}

func TestStoreRead(t *testing.T) {
	file, err := ioutil.TempFile("", "store_append_read_test")
	require.NoError(t, err)
	defer os.Remove(file.Name())

	store, err := newStore(file)
	require.NoError(t, err)

	for i := uint64(1); i < 4; i++ {
		n, position, err := store.Append((write))
		require.NoError(t, err)
		require.Equal(t, position+n, width*i)
	}

	var position uint64
	for i := uint64(1); i < 4; i++ {
		read, err := store.Read(position)
		require.NoError(t, err)
		require.Equal(t, write, read)
		position += width
	}

	store, err = newStore(file)
	require.NoError(t, err)

	position = 0
	for i := uint64(1); i < 4; i++ {
		read, err := store.Read(position)
		require.NoError(t, err)
		require.Equal(t, write, read)
		position += width
	}
}

func TestStoreReadAt(t *testing.T) {
	file, err := ioutil.TempFile("", "store_append_read_test")
	require.NoError(t, err)
	defer os.Remove(file.Name())

	store, err := newStore(file)
	require.NoError(t, err)

	for i := uint64(1); i < 4; i++ {
		n, position, err := store.Append((write))
		require.NoError(t, err)
		require.Equal(t, position+n, width*i)
	}

	for i, offset := uint64(1), int64(0); i < 4; i++ {
		bytes := make([]byte, lenWidth)
		nonce, err := store.ReadAt(bytes, offset)
		require.NoError(t, err)
		require.Equal(t, lenWidth, nonce)
		offset := int64(nonce)

		size := encoding.Uint64(bytes)
		bytes = make([]byte, size)
		nonce, err = store.ReadAt(bytes, offset)
		require.NoError(t, err)
		require.Equal(t, write, bytes)
		require.Equal(t, int(size), nonce)
		offset += int64(nonce)
	}

	store, err = newStore(file)
	require.NoError(t, err)
}

func TestStoreClose(t *testing.T) {
	file, err := ioutil.TempFile("", "store_append_read_test")
	require.NoError(t, err)
	defer os.Remove(file.Name())

	store, err := newStore(file)
	require.NoError(t, err)
	_, _, err = store.Append((write))
	require.NoError(t, err)

	file, sizeBefore, err := openFile(file.Name())
	require.NoError(t, err)

	err = store.Close()
	require.NoError(t, err)

	_, sizeAfter, err := openFile(file.Name())
	require.NoError(t, err)
	require.True(t, sizeAfter > sizeBefore)
}
