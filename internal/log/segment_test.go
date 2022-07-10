package log

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/oabraham1/distributed-app/api/v1"
	"github.com/stretchr/testify/require"
)

func TestSegment(t *testing.T) {
	directory, _ := ioutil.TempDir("", "Testing Segment file")
	defer os.Remove(directory)

	want := &api.Record{Value: []byte("Segment Test")}

	config := Config{}
	config.Segment.MaxStoreBytes = 1024
	config.Segment.MaxIndexBytes = entryWidth * 3

	segment, err := newSegment(directory, 16, config)
	require.NoError(t, err)
	require.Equal(t, uint64(16), segment.nextOffset, segment.nextOffset)
	require.False(t, segment.IsMaxed())

	for i := uint64(0); i < 3; i++ {
		offset, err := segment.Append(want)
		require.NoError(t, err)
		require.Equal(t, 16+i, offset)

		got, err := segment.Read(offset)
		require.NoError(t, err)
		require.Equal(t, want.Value, got.Value)
	}

	_, err = segment.Append(want)
	require.Equal(t, io.EOF, err)

	require.True(t, segment.IsMaxed())

	config.Segment.MaxStoreBytes = uint64(len(want.Value) * 3)
	config.Segment.MaxIndexBytes = 1024

	segment, err = newSegment(directory, 16, config)
	require.NoError(t, err)
	require.True(t, segment.IsMaxed())

	err = segment.Remove()
	require.NoError(t, err)
	segment, err = newSegment(directory, 16, config)
	require.NoError(t, err)
	require.False(t, segment.IsMaxed())
}
