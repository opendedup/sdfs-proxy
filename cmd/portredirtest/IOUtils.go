package test

import (
	"context"
	"fmt"
	"hash"
	"math/rand"
	"testing"
	"time"

	"github.com/opendedup/sdfs-client-go/api"
	"github.com/stretchr/testify/assert"
	"golang.org/x/crypto/blake2b"
)

const (
	letterBytes   = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

func makeFile(ctx context.Context, t *testing.T, c *TestRun, parent string, size int64) (string, []byte) {
	return makeGenericFile(ctx, t, c.connection, parent, size)
}

func makeLargeBlockFile(ctx context.Context, t *testing.T, c *TestRun, parent string, size int64, blocksize int) (string, []byte) {
	return makeLargeBlockGenericFile(ctx, t, c.connection, parent, size, blocksize)
}

func makeGenericFile(ctx context.Context, t *testing.T, connection *api.SdfsConnection, parent string, size int64) (string, []byte) {
	fn := fmt.Sprintf("%s/%s", parent, string(randBytesMaskImpr(16)))
	err := connection.MkNod(ctx, fn, 511, 0)
	assert.Nil(t, err)
	stat, err := connection.GetAttr(ctx, fn)
	assert.Nil(t, err)
	var h hash.Hash
	assert.Equal(t, stat.Mode, int32(511))
	fh, err := connection.Open(ctx, fn, 0)
	assert.Nil(t, err)
	maxoffset := size
	offset := int64(0)
	h, err = blake2b.New(32, make([]byte, 0))
	assert.Nil(t, err)
	blockSz := 1024 * 32
	for offset < maxoffset {
		if blockSz > int(maxoffset-offset) {
			blockSz = int(maxoffset - offset)
		}
		b := randBytesMaskImpr(blockSz)
		err = connection.Write(ctx, fh, b, offset, int32(len(b)))
		h.Write(b)
		assert.Nil(t, err)
		offset += int64(len(b))
		b = nil
	}

	err = connection.Release(ctx, fh)
	assert.Nil(t, err)
	stat, _ = connection.GetAttr(ctx, fn)
	assert.Equal(t, size, stat.Size)
	return fn, h.Sum(nil)
}

func makeLargeBlockGenericFile(ctx context.Context, t *testing.T, connection *api.SdfsConnection, parent string, size int64, blocksize int) (string, []byte) {
	fn := fmt.Sprintf("%s/%s", parent, string(randBytesMaskImpr(16)))
	err := connection.MkNod(ctx, fn, 511, 0)
	assert.Nil(t, err)
	stat, err := connection.GetAttr(ctx, fn)
	assert.Nil(t, err)
	assert.Equal(t, stat.Mode, int32(511))
	fh, err := connection.Open(ctx, fn, 0)
	assert.Nil(t, err)
	maxoffset := size
	offset := int64(0)
	h, err := blake2b.New(32, make([]byte, 0))
	assert.Nil(t, err)
	blockSz := 1024 * blocksize
	for offset < maxoffset {
		if blockSz > int(maxoffset-offset) {
			blockSz = int(maxoffset - offset)
		}
		b := randBytesMaskImpr(blockSz)

		err = connection.Write(ctx, fh, b, offset, int32(len(b)))
		h.Write(b)
		//t.Logf("Wrote blocksize %d", len(b))
		assert.Nil(t, err)
		offset += int64(len(b))
		b = nil
	}
	stat, _ = connection.GetAttr(ctx, fn)
	assert.Equal(t, stat.Size, maxoffset)
	_ = connection.Release(ctx, fh)
	return fn, h.Sum(nil)
}

func readFile(ctx context.Context, t *testing.T, c *TestRun, filenm string, delete bool) (data []byte, err error) {
	stat, err := c.connection.GetAttr(ctx, filenm)
	assert.Nil(t, err)
	if err != nil {
		return data, err
	}
	assert.Greater(t, stat.Size, int64(0))
	fh, err := c.connection.Open(ctx, filenm, 0)
	assert.Nil(t, err)
	maxoffset := stat.Size
	offset := int64(0)
	b := make([]byte, 0)
	h, err := blake2b.New(32, b)
	assert.Nil(t, err)
	readSize := int32(1024 * 1024)
	for offset < maxoffset {
		if readSize > int32(maxoffset-offset) {
			readSize = int32(maxoffset - offset)
		}
		b, err := c.connection.Read(ctx, fh, offset, int32(readSize))
		h.Write(b)
		assert.Nil(t, err)
		offset += int64(len(b))
		b = nil
	}
	err = c.connection.Release(ctx, fh)
	assert.Nil(t, err)

	if delete {
		err = c.connection.DeleteFile(ctx, filenm)
		assert.Nil(t, err)
		_, err = c.connection.GetAttr(ctx, filenm)
		assert.NotNil(t, err)
	}
	bs := h.Sum(nil)
	return bs, nil
}

func deleteFile(t *testing.T, c *TestRun, fn string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := c.connection.DeleteFile(ctx, fn)
	assert.Nil(t, err)
	_, err = c.connection.GetAttr(ctx, fn)
	assert.NotNil(t, err)
}

func randBytesMaskImpr(n int) []byte {
	rand.Seed(time.Now().UTC().UnixNano())
	b := make([]byte, n)
	// A rand.Int63() generates 63 random bits, enough for letterIdxMax letters!
	for i, cache, remain := n-1, rand.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = rand.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}
	return b
}
