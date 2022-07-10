package log

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	file, err := ioutil.TempFile(os.TempDir(), "Testing the index")
	require.NoError(t, err)
	defer os.Remove(file.Name())

	config := Config{}
	config.Segment.MaxIndexBytes = 1024
	idx, err := newIndex(file, config)
	require.NoError(t, err)
	_, _, err = idx.Read(-1)
	require.Error(t, err)
	require.Equal(t, file.Name(), idx.Name())
	entries := []struct {
		Offset   uint32
		Position uint64
	}{
		{Offset: 0, Position: 0},
		{Offset: 1, Position: 10},
	}

	for _, want := range entries {
		err = idx.Write(want.Offset, want.Position)
		require.NoError(t, err)

		_, position, err := idx.Read(int64(want.Offset))
		require.NoError(t, err)
		require.Equal(t, want.Position, position)
	}

	_, _, err = idx.Read(int64(len(entries)))
	require.Equal(t, io.EOF, err)
	_ = idx.Close()
}
