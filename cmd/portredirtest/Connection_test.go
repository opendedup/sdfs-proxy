package test

import (
	"context"
	"encoding/json"
	"hash"
	"net/url"
	"runtime"

	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/Azure/azure-storage-blob-go/azblob"

	api "github.com/opendedup/sdfs-client-go/api"
	spb "github.com/opendedup/sdfs-client-go/sdfs"
	paip "github.com/opendedup/sdfs-proxy/api"
	"github.com/stretchr/testify/assert"
	"golang.org/x/crypto/blake2b"
	"google.golang.org/grpc"
)

type testRun struct {
	name             string
	volume           int64
	clientsidededupe bool
	url              string
	connection       *api.SdfsConnection
	inputEnv         []string
	containerName    string
	containerPort    string
	cloudVol         bool
	fe               *paip.ForwardEntry
	direct           bool
}

var maddress []*testRun

var tls = false
var mtls = false
var lport = "localhost:16442-16445"
var imagename = "gcr.io/hybrics/hybrics:dp3"
var password = "admin"

const (
	letterBytes   = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
	tb            = int64(1099511627776)
	gb            = int64(1073741824)
)

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

func runMatix(t *testing.T, testType string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, c := range maddress {
		testNewProxyConnection(t, c)
		t.Run(fmt.Sprintf("%s/%s/testConnection", testType, c.name), func(t *testing.T) {
			assert.NotNil(t, c.connection)
		})
		t.Run(fmt.Sprintf("%s/%s/testChown", testType, c.name), func(t *testing.T) {
			testChow(t, c)

		})
		t.Run(fmt.Sprintf("%s/%s/testMkDir", testType, c.name), func(t *testing.T) {
			testMkDir(t, c)
		})
		t.Run(fmt.Sprintf("%s/%s/testListDir", testType, c.name), func(t *testing.T) {
			testListDir(t, c)
		})
		t.Run(fmt.Sprintf("%s/%s/testMkNod", testType, c.name), func(t *testing.T) {
			testMkNod(t, c)
		})
		t.Run(fmt.Sprintf("%s/%s/testMkDirAll", testType, c.name), func(t *testing.T) {
			testMkDirAll(t, c)
		})
		t.Run(fmt.Sprintf("%s/%s/testCleanStore", testType, c.name), func(t *testing.T) {
			testCleanStore(t, c)
		})
		t.Run(fmt.Sprintf("%s/%s/testStatFS", testType, c.name), func(t *testing.T) {
			testStatFS(t, c)
		})
		t.Run(fmt.Sprintf("%s/%s/testRename", testType, c.name), func(t *testing.T) {
			testRename(t, c)
		})
		t.Run(fmt.Sprintf("%s/%s/testCopyFile", testType, c.name), func(t *testing.T) {
			testCopyFile(t, c)
		})
		t.Run(fmt.Sprintf("%s/%s/testEvents", testType, c.name), func(t *testing.T) {
			testEvents(t, c)
		})
		t.Run(fmt.Sprintf("%s/%s/testXAttrs", testType, c.name), func(t *testing.T) {
			testXAttrs(t, c)
		})
		t.Run(fmt.Sprintf("%s/%s/testSetUtime", testType, c.name), func(t *testing.T) {
			testSetUtime(t, c)
		})
		t.Run(fmt.Sprintf("%s/%s/testTuncate", testType, c.name), func(t *testing.T) {
			testTuncate(t, c)
		})
		t.Run(fmt.Sprintf("%s/%s/testWriteLargeBlock", testType, c.name), func(t *testing.T) {
			testWriteLargeBlock(t, c)
		})
		t.Run(fmt.Sprintf("%s/%s/testCopyExtent", testType, c.name), func(t *testing.T) {
			testCopyExtent(t, c)
		})
		t.Run(fmt.Sprintf("%s/%s/testSymLink", testType, c.name), func(t *testing.T) {
			testSymLink(t, c)
		})
		t.Run(fmt.Sprintf("%s/%s/testSync", testType, c.name), func(t *testing.T) {
			testSync(t, c)
		})
		t.Run(fmt.Sprintf("%s/%s/testMaxAge", testType, c.name), func(t *testing.T) {
			testMaxAge(t, c)
		})
		t.Run(fmt.Sprintf("%s/%s/testInfo", testType, c.name), func(t *testing.T) {
			testInfo(t, c)
		})
		t.Run(fmt.Sprintf("%s/%s/testGCSchedule", testType, c.name), func(t *testing.T) {
			testGCSchedule(t, c)
		})
		t.Run(fmt.Sprintf("%s/%s/testUpload", testType, c.name), func(t *testing.T) {
			testUpload(t, c)
		})
		t.Run(fmt.Sprintf("%s/%s/testShutdown", testType, c.name), func(t *testing.T) {
			testShutdown(t, c)
		})
		t.Run(fmt.Sprintf("%s/%s/testSetVolumeSize", testType, c.name), func(t *testing.T) {
			testSetVolumeSize(t, c)
		})
		if c.cloudVol {
			t.Run(fmt.Sprintf("%s/%s/testSetRWSpeed", testType, c.name), func(t *testing.T) {
				testSetRWSpeed(t, c)
			})
			t.Run(fmt.Sprintf("%s/%s/testCache", testType, c.name), func(t *testing.T) {
				testCache(t, c)
			})
			t.Run(fmt.Sprintf("%s/%s/testCloudSync", testType, c.name), func(t *testing.T) {
				testCloudSync(t, c)
			})
		}
		c.connection.CloseConnection(ctx)
	}
}

func TestMatrix(t *testing.T) {
	runMatix(t, "PROXY")
	for _, c := range maddress {
		c.clientsidededupe = true
	}
	runMatix(t, "PROXYDEDUPE")
	for _, c := range maddress {
		c.direct = true
	}
	runMatix(t, "DIRECTDEDUPE")
}

func testNewProxyConnection(t *testing.T, c *testRun) {
	t.Logf("Creating connection for %d\n", c.volume)
	c.connection = dconnect(t, c)
	assert.NotNil(t, c.connection)

}

func testChow(t *testing.T, c *testRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fn, _ := makeFile(ctx, t, c, "", 128)
	err := c.connection.Chown(ctx, fn, int32(100), int32(100))
	assert.Nil(t, err)
	stat, err := c.connection.GetAttr(ctx, fn)
	assert.Nil(t, err)
	assert.Equal(t, stat.Gid, int32(100))
	assert.Equal(t, stat.Uid, int32(100))
	deleteFile(t, c, fn)
}

func testMkNod(t *testing.T, c *testRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fn, _ := makeFile(ctx, t, c, "", 128)
	exists, err := c.connection.FileExists(ctx, fn)
	assert.Nil(t, err)
	assert.True(t, exists)
	deleteFile(t, c, fn)
}

func testMkDir(t *testing.T, c *testRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := c.connection.MkDir(ctx, "testdir", 511)
	assert.Nil(t, err)
	stat, err := c.connection.GetAttr(ctx, "testdir")
	assert.Nil(t, err)
	if runtime.GOOS != "windows" {
		assert.Equal(t, stat.Mode, int32(16895))
	}
	err = c.connection.RmDir(ctx, "testdir")
	assert.Nil(t, err)
	_, err = c.connection.GetAttr(ctx, "testdir")
	assert.NotNil(t, err)
}

func testMkDirAll(t *testing.T, c *testRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := c.connection.MkDirAll(ctx, "testdir/t")
	assert.Nil(t, err)
	stat, err := c.connection.GetAttr(ctx, "testdir/t")
	assert.Nil(t, err)
	if runtime.GOOS != "windows" {
		assert.Equal(t, stat.Mode, int32(16832))
	}
	err = c.connection.RmDir(ctx, "testdir/t")
	assert.Nil(t, err)
	_, err = c.connection.GetAttr(ctx, "testdir/t")
	assert.NotNil(t, err)
	err = c.connection.RmDir(ctx, "testdir")
	assert.Nil(t, err)
	_, err = c.connection.GetAttr(ctx, "testdir")
	assert.NotNil(t, err)
}

func testListDir(t *testing.T, c *testRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dn := string(randBytesMaskImpr(16))
	err := c.connection.MkDir(ctx, dn, 511)
	assert.Nil(t, err)
	var files []string
	for i := 0; i < 10; i++ {
		fn, _ := makeFile(ctx, t, c, dn, 1024)
		files = append(files, fn)
	}
	_, list, err := c.connection.ListDir(ctx, dn, "", false, 20)
	assert.Nil(t, err)
	var afiles []string
	for _, l := range list {
		afiles = append(afiles, l.FilePath)
		c.connection.DeleteFile(ctx, l.FilePath)
	}
	if runtime.GOOS != "windows" {
		assert.ElementsMatch(t, files, afiles)
	} else {
		assert.Equal(t, len(files), len(afiles))
	}
	err = c.connection.RmDir(ctx, dn)
	assert.Nil(t, err)
	_, err = c.connection.GetAttr(ctx, dn)
	assert.NotNil(t, err)
}

func testCleanStore(t *testing.T, c *testRun) {
	cleanStore(t, c)

}

func testStatFS(t *testing.T, c *testRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := c.connection.StatFS(ctx)
	assert.Nil(t, err)
}

func testRename(t *testing.T, c *testRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fn, _ := makeFile(ctx, t, c, "", 1024)
	nfn := string(randBytesMaskImpr(16))

	err := c.connection.Rename(ctx, fn, nfn)
	assert.Nil(t, err)
	_, err = c.connection.Stat(ctx, fn)
	assert.NotNil(t, err)
	_, err = c.connection.Stat(ctx, nfn)
	assert.Nil(t, err)
	err = c.connection.DeleteFile(ctx, nfn)
	assert.Nil(t, err)
}

func testCopyFile(t *testing.T, c *testRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fn, hash := makeFile(ctx, t, c, "", 1024)
	nfn := string(randBytesMaskImpr(16))
	_, err := c.connection.CopyFile(ctx, fn, nfn, false)
	assert.Nil(t, err)
	nhash, err := readFile(ctx, t, c, nfn, false)
	assert.Nil(t, err)
	if err != nil {
		assert.Equal(t, hash, nhash)
		err = c.connection.DeleteFile(ctx, nfn)
		assert.Nil(t, err)
		err = c.connection.DeleteFile(ctx, fn)
		assert.Nil(t, err)
	}
}

func testEvents(t *testing.T, c *testRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fn, hash := makeFile(ctx, t, c, "", 1024)
	nfn := string(randBytesMaskImpr(16))
	evt, err := c.connection.CopyFile(ctx, fn, nfn, true)
	assert.Nil(t, err)
	assert.NotNil(t, evt)
	if evt != nil {
		_, err = c.connection.WaitForEvent(ctx, evt.Uuid)
		assert.Nil(t, err)
	}
	nhash, err := readFile(ctx, t, c, nfn, false)
	assert.Nil(t, err)
	if err != nil {
		assert.Equal(t, hash, nhash)
		err = c.connection.DeleteFile(ctx, nfn)
		assert.Nil(t, err)
		err = c.connection.DeleteFile(ctx, fn)
		assert.Nil(t, err)
		_, err = c.connection.GetEvent(ctx, evt.Uuid)
		assert.Nil(t, err)
		_, err = c.connection.ListEvents(ctx)
		assert.Nil(t, err)
	}
}

func testXAttrs(t *testing.T, c *testRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fn, _ := makeFile(ctx, t, c, "", 1024)
	_, err := c.connection.GetXAttrSize(ctx, fn, "key")
	assert.NotNil(t, err)
	err = c.connection.SetXAttr(ctx, "key", "value", fn)
	assert.Nil(t, err)
	val, err := c.connection.GetXAttr(ctx, "key", fn)
	assert.Equal(t, val, "value")
	assert.Nil(t, err)
	_, err = c.connection.GetXAttrSize(ctx, fn, "key")
	assert.Nil(t, err)
	err = c.connection.RemoveXAttr(ctx, "key", fn)
	assert.Nil(t, err)
	_, err = c.connection.GetXAttrSize(ctx, fn, "key")
	assert.NotNil(t, err)
	fa := []*spb.FileAttributes{{Key: "key1", Value: "value1"}, {Key: "key2", Value: "value2"}}
	err = c.connection.SetUserMetaData(ctx, fn, fa)
	assert.Nil(t, err)
	_, fal, err := c.connection.ListDir(ctx, fn, "", false, int32(1000))
	assert.Nil(t, err)
	for _, attrs := range fal {
		if attrs.FileAttributes[0].Key == "key1" {
			assert.Equal(t, attrs.FileAttributes[0].Value, "value1")
		} else {
			assert.Equal(t, attrs.FileAttributes[0].Key, "key2")
			assert.Equal(t, attrs.FileAttributes[0].Value, "value2")
		}
	}
	err = c.connection.DeleteFile(ctx, fn)
	assert.Nil(t, err)
}

func testSetUtime(t *testing.T, c *testRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fn, _ := makeFile(ctx, t, c, "", 1024)
	err := c.connection.Utime(ctx, fn, int64(0), int64(0))
	assert.Nil(t, err)
	stat, err := c.connection.GetAttr(ctx, fn)
	assert.Nil(t, err)
	assert.Equal(t, stat.Atime, int64(0))
	assert.Equal(t, stat.Mtim, int64(0))
	err = c.connection.DeleteFile(ctx, fn)
	assert.Nil(t, err)
}

func testTuncate(t *testing.T, c *testRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fn, _ := makeFile(ctx, t, c, "", 1024*1024*10)
	err := c.connection.Truncate(ctx, fn, int64(0))
	assert.Nil(t, err)
	stat, err := c.connection.GetAttr(ctx, fn)
	assert.Nil(t, err)
	assert.Equal(t, stat.Size, int64(0))
	err = c.connection.DeleteFile(ctx, fn)
	assert.Nil(t, err)
}

func testWriteLargeBlock(t *testing.T, c *testRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tMb := int64(1024 * 1024 * 10)
	fMb := 1024 * 5
	fn, _ := makeLargeBlockFile(ctx, t, c, "", tMb, fMb)
	err := c.connection.DeleteFile(ctx, fn)
	assert.Nil(t, err)
}

func testSymLink(t *testing.T, c *testRun) {
	if runtime.GOOS != "windows" {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		fn, _ := makeFile(ctx, t, c, "", 1024)
		sfn := string(randBytesMaskImpr(16))
		_, fls, err := c.connection.ListDir(ctx, "/", "", false, int32(100))
		assert.Nil(t, err)
		err = c.connection.SymLink(ctx, fn, sfn)
		assert.Nil(t, err)
		_sfn, err := c.connection.ReadLink(ctx, sfn)
		assert.Nil(t, err)
		assert.Equal(t, fn, _sfn)
		_, err = c.connection.GetAttr(ctx, sfn)
		assert.Nil(t, err)
		_, nfls, err := c.connection.ListDir(ctx, "/", "", false, int32(100))
		assert.Equal(t, len(fls), len(nfls)-1)
		assert.Nil(t, err)
		err = c.connection.Unlink(ctx, sfn)
		assert.Nil(t, err)
		_, err = c.connection.GetAttr(ctx, sfn)
		assert.NotNil(t, err)
		err = c.connection.DeleteFile(ctx, fn)
		assert.Nil(t, err)
	}
}

func testSync(t *testing.T, c *testRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fn, _ := makeFile(ctx, t, c, "", 1024)
	fh, err := c.connection.Open(ctx, fn, int32(-1))
	assert.Nil(t, err)
	b := randBytesMaskImpr(16)
	err = c.connection.Write(ctx, fh, b, 0, int32(len(b)))
	assert.Nil(t, err)
	err = c.connection.Flush(ctx, fn, fh)
	assert.Nil(t, err)
	err = c.connection.Release(ctx, fh)
	assert.Nil(t, err)
	err = c.connection.DeleteFile(ctx, fn)
	assert.Nil(t, err)
}

func testMaxAge(t *testing.T, c *testRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	info, err := c.connection.DSEInfo(ctx)
	assert.Nil(t, err)
	t.Logf("max age : %d", info.MaxAge)
	err = c.connection.SetMaxAge(ctx, 1000)
	assert.Nil(t, err)
	info, err = c.connection.DSEInfo(ctx)
	assert.Nil(t, err)
	assert.Equal(t, int64(1000), info.MaxAge)
	t.Logf("new max age : %d", info.MaxAge)
	fsz := int64(1024 * 1024)
	_nfn, _ := makeFile(ctx, t, c, "", fsz)
	time.Sleep(15 * time.Second)
	info, err = c.connection.DSEInfo(ctx)
	assert.Nil(t, err)

	sz := info.CurrentSize
	nfn := string(randBytesMaskImpr(16))
	time.Sleep(10 * time.Second)

	_, err = c.connection.Download(ctx, _nfn, nfn, 1024)
	defer os.Remove(nfn)
	assert.Nil(t, err)
	_, err = c.connection.Upload(ctx, nfn, nfn, 1024)
	assert.Nil(t, err)
	os.Remove(_nfn)
	time.Sleep(15 * time.Second)
	info, err = c.connection.DSEInfo(ctx)
	assert.Nil(t, err)
	nsz := info.CurrentSize
	t.Logf("sz = %d nsz =%d", sz, nsz)
	assert.Less(t, sz, nsz)
	err = c.connection.DeleteFile(ctx, nfn)
	assert.Nil(t, err)
	err = c.connection.DeleteFile(ctx, _nfn)
	assert.Nil(t, err)
	time.Sleep(15 * time.Second)
	c.connection.CleanStore(ctx, true, true)
	tm := time.Duration(60 * int(time.Second))
	time.Sleep(tm)
	info, err = c.connection.DSEInfo(ctx)
	assert.Nil(t, err)
	fnsz := info.CurrentSize
	t.Logf("sz = %d nsz =%d, fnsz=%d", sz, nsz, fnsz)
	assert.Greater(t, sz, fnsz)
	_nfn, hs := makeFile(ctx, t, c, "", fsz)
	nfn = string(randBytesMaskImpr(16))
	time.Sleep(10 * time.Second)
	c.connection.CopyFile(ctx, _nfn, nfn, false)
	c.connection.DeleteFile(ctx, _nfn)
	time.Sleep(10 * time.Second)
	c.connection.CleanStore(ctx, true, true)
	tm = time.Duration(60 * int(time.Second))
	time.Sleep(tm)
	info, err = c.connection.DSEInfo(ctx)
	assert.Nil(t, err)
	fnsz = info.CurrentSize
	t.Logf("sz = %d, fnsz=%d", sz, fnsz)
	nhs, err := readFile(ctx, t, c, nfn, true)
	assert.Nil(t, err)
	assert.Equal(t, hs, nhs)
	c.connection.DeleteFile(ctx, _nfn)
	time.Sleep(10 * time.Second)
	c.connection.CleanStore(ctx, true, true)
	tm = time.Duration(60 * int(time.Second))
	time.Sleep(tm)
	info, err = c.connection.DSEInfo(ctx)
	assert.Nil(t, err)
	fnsz = info.CurrentSize
	t.Logf("sz = %d, fnsz=%d", sz, fnsz)
	_nfn, _ = makeFile(ctx, t, c, "", fsz)
	nfn = string(randBytesMaskImpr(16))
	os.Remove(nfn)
	time.Sleep(10 * time.Second)
	_, err = c.connection.Download(ctx, _nfn, nfn, 1024)
	assert.Nil(t, err)
	for i := 0; i < 10; i++ {
		_, err = c.connection.Upload(ctx, nfn, fmt.Sprintf("file%d", i), 1024)
		if err != nil {
			t.Logf("upload error %v", err)
		}
		info, err := c.connection.Stat(ctx, fmt.Sprintf("file%d", i))
		assert.GreaterOrEqual(t, info.IoMonitor.ActualBytesWritten, int64(0))
		assert.Nil(t, err)
		time.Sleep(10 * time.Second)
	}
	time.Sleep(15 * time.Second)
	info, _ = c.connection.DSEInfo(ctx)
	sz = info.CurrentSize
	c.connection.DeleteFile(ctx, _nfn)
	time.Sleep(10 * time.Second)
	c.connection.CleanStore(ctx, true, true)
	tm = time.Duration(60 * int(time.Second))
	time.Sleep(tm)
	info, _ = c.connection.DSEInfo(ctx)
	nsz = info.CurrentSize
	t.Logf("sz = %d, nsz=%d", sz, nsz)
	assert.Less(t, nsz, sz)
	for i := 0; i < 10; i++ {
		err = c.connection.DeleteFile(ctx, fmt.Sprintf("file%d", i))
		if err != nil {
			t.Logf("upload error %v", err)
		}
	}
	time.Sleep(10 * time.Second)
	c.connection.CleanStore(ctx, true, true)
	tm = time.Duration(60 * int(time.Second))
	time.Sleep(tm)
	info, _ = c.connection.DSEInfo(ctx)
	sz = info.CurrentSize
	t.Logf("sz = %d, nsz=%d", sz, nsz)
	assert.Less(t, sz, nsz)
	os.Remove(nfn)
	err = c.connection.SetMaxAge(ctx, -1)
	assert.Nil(t, err)
}

func testCopyExtent(t *testing.T, c *testRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fn, _ := makeFile(ctx, t, c, "", 1024)
	sfn, _ := makeFile(ctx, t, c, "", 1024)
	fh, err := c.connection.Open(ctx, fn, int32(-1))
	assert.Nil(t, err)
	sfh, err := c.connection.Open(ctx, sfn, int32(-1))
	assert.Nil(t, err)
	b := randBytesMaskImpr(16)
	err = c.connection.Write(ctx, fh, b, 0, int32(len(b)))
	assert.Nil(t, err)
	err = c.connection.Flush(ctx, fn, fh)
	assert.Nil(t, err)
	err = c.connection.Release(ctx, fh)
	assert.Nil(t, err)
	fh, err = c.connection.Open(ctx, fn, int32(-1))
	assert.Nil(t, err)
	_, err = c.connection.CopyExtent(ctx, fn, sfn, 0, 0, int64(len(b)))
	assert.Nil(t, err)
	err = c.connection.Flush(ctx, sfn, sfh)
	assert.Nil(t, err)
	err = c.connection.Release(ctx, sfh)
	assert.Nil(t, err)
	sfh, err = c.connection.Open(ctx, sfn, int32(-1))
	assert.Nil(t, err)
	nb, err := c.connection.Read(ctx, sfh, 0, int32(len(b)))
	assert.Nil(t, err)
	assert.Equal(t, nb, b)
	err = c.connection.Release(ctx, fh)
	assert.Nil(t, err)
	err = c.connection.Release(ctx, sfh)
	assert.Nil(t, err)
	err = c.connection.DeleteFile(ctx, fn)
	assert.Nil(t, err)
	err = c.connection.DeleteFile(ctx, sfn)
	assert.Nil(t, err)
}

func testInfo(t *testing.T, c *testRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := c.connection.GetVolumeInfo(ctx)
	assert.Nil(t, err)
	_, err = c.connection.DSEInfo(ctx)
	assert.Nil(t, err)
	_, err = c.connection.SystemInfo(ctx)
	assert.Nil(t, err)
}

func testGCSchedule(t *testing.T, c *testRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gc, err := c.connection.GetGCSchedule(ctx)
	assert.Nil(t, err)
	t.Logf("GC Sched = %s", gc)
}

func testCache(t *testing.T, c *testRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err := c.connection.SetCacheSize(ctx, int64(1)*tb, true)
	assert.NotNil(t, err)
	dse, err := c.connection.DSEInfo(ctx)
	assert.Nil(t, err)
	assert.Equal(t, int64(10)*gb, dse.MaxCacheSize)
	_, err = c.connection.SetCacheSize(ctx, int64(5)*gb, true)
	assert.Nil(t, err)
	dse, err = c.connection.DSEInfo(ctx)
	assert.Nil(t, err)
	assert.Equal(t, int64(5)*gb, dse.MaxCacheSize)
	_, err = c.connection.SetCacheSize(ctx, int64(10)*gb, true)
	assert.Nil(t, err)
	dse, err = c.connection.DSEInfo(ctx)
	assert.Nil(t, err)
	assert.Equal(t, int64(10)*gb, dse.MaxCacheSize)
}

func testSetRWSpeed(t *testing.T, c *testRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := c.connection.SetReadSpeed(ctx, int32(1000))
	assert.Nil(t, err)
	err = c.connection.SetWriteSpeed(ctx, int32(2000))
	assert.Nil(t, err)
	dse, err := c.connection.DSEInfo(ctx)
	assert.Nil(t, err)
	assert.Equal(t, int32(1000), dse.ReadSpeed)
	assert.Equal(t, int32(2000), dse.WriteSpeed)
}

func testSetVolumeSize(t *testing.T, c *testRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := c.connection.SetVolumeCapacity(ctx, int64(100)*tb)
	assert.Nil(t, err)
	vol, err := c.connection.GetVolumeInfo(ctx)
	assert.Nil(t, err)
	assert.Equal(t, int64(100)*tb, vol.Capactity)
}

func cleanStore(t *testing.T, c *testRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var files []string
	for i := 0; i < 10; i++ {
		fn, _ := makeFile(ctx, t, c, "", 1024*1024)
		files = append(files, fn)
	}
	_nfn, nh := makeFile(ctx, t, c, "", 1024*1024)
	time.Sleep(60 * time.Second)
	info, err := c.connection.DSEInfo(ctx)
	assert.Nil(t, err)
	sz := info.CurrentSize
	for _, l := range files {
		c.connection.DeleteFile(ctx, l)
	}
	time.Sleep(65 * time.Second)
	c.connection.CleanStore(ctx, true, true)
	time.Sleep(65 * time.Second)
	info, err = c.connection.DSEInfo(ctx)
	assert.Nil(t, err)
	nsz := info.CurrentSize
	assert.Greater(t, sz, nsz)
	nhn, err := readFile(ctx, t, c, _nfn, true)
	assert.Nil(t, err)
	assert.Equal(t, nh, nhn)
	t.Logf("orig = %d new = %d", sz, nsz)
}

/*
func TestCert(t *testing.T) {
	api.DisableTrust = false
	connection, err := api.NewConnection(address, false)
	assert.NotNil(t, err)
	assert.Nil(t, connection)
	err = api.AddTrustedCert(address)
	assert.Nil(t, err)
	api.DisableTrust = false
	connection, err = api.NewConnection(address, false)
	assert.NotNil(t, connection)
	assert.Nil(t, err)
	user, err := user.Current()
	assert.Nil(t, err)
	configPath := user.HomeDir + "/.sdfs/keys/"
	addr, _, _, _ := api.ParseURL(address)
	fn := fmt.Sprintf("%s%s.pem", configPath, addr)
	err = os.Remove(fn)

	assert.Nil(t, err)

}*/

func testShutdown(t *testing.T, c *testRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := c.connection.DSEInfo(ctx)
	assert.Nil(t, err)
	err = c.connection.ShutdownVolume(ctx)
	assert.Nil(t, err)
	time.Sleep(20 * time.Second)
	//because the volume is not actually shutdown during debug
	_, err = c.connection.DSEInfo(ctx)
	assert.Nil(t, err)

}

func testUpload(t *testing.T, c *testRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fn := string(randBytesMaskImpr(16))
	data := randBytesMaskImpr(1024)
	h, err := blake2b.New(32, make([]byte, 0))
	defer os.Remove(fn)
	assert.Nil(t, err)
	err = ioutil.WriteFile(fn, data, 0777)
	assert.Nil(t, err)
	h.Write(data)
	bs := h.Sum(nil)
	wr, err := c.connection.Upload(ctx, fn, fn, 1024)
	assert.Nil(t, err)
	assert.Equal(t, int64(len(data)), wr)
	nhs, err := readFile(ctx, t, c, fn, false)
	assert.Nil(t, err)
	assert.Equal(t, bs, nhs)
	nfn := string(randBytesMaskImpr(16))
	rr, err := c.connection.Download(ctx, fn, nfn, 1024)
	assert.Equal(t, int64(len(data)), rr)
	assert.Nil(t, err)
	ndata, err := ioutil.ReadFile(nfn)
	assert.Nil(t, err)
	h, err = blake2b.New(32, make([]byte, 0))
	assert.Nil(t, err)
	h.Write(ndata)
	nbs := h.Sum(nil)
	assert.Equal(t, bs, nbs)
	os.Remove(nfn)
	os.Remove(fn)
	c.connection.DeleteFile(ctx, fn)
}

func testCloudSync(t *testing.T, c *testRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	containername := fmt.Sprintf("second-%s", c.containerName)
	portopening := "6442"
	nport := fmt.Sprintf("2%s", c.containerPort)
	cmd := []string{}
	_, err := RunContainer(ctx, imagename, containername, nport, portopening, c.inputEnv, cmd, true)
	if err != nil {
		fmt.Printf("Unable to create docker client %v", err)
	}
	durl := fmt.Sprintf("sdfs://localhost:%s", nport)
	tr := testRun{url: durl, name: fmt.Sprintf("second-%s", c.name), inputEnv: c.inputEnv, containerName: containername, containerPort: portopening, cloudVol: true, clientsidededupe: c.clientsidededupe}
	fe := paip.ForwardEntry{
		Address:       tr.url,
		Dedupe:        false,
		DedupeThreads: 1,
		DedupeBuffer:  4,
	}
	portR := &paip.PortRedirectors{}
	for _, c := range maddress {
		portR.ForwardEntrys = append(portR.ForwardEntrys, *c.fe)
	}

	portR.ForwardEntrys = append(portR.ForwardEntrys, fe)
	connection, err := api.NewConnection(durl, false, true, -1, 0, 0)
	retrys := 0
	for err != nil {
		t.Logf("retries = %d\n", retrys)
		time.Sleep(20 * time.Second)
		connection, err = api.NewConnection(durl, false, true, -1, 0, 0)
		if retrys > 10 {
			t.Errorf("SDFS Server connection timed out %s\n", durl)
			os.Exit(-1)
		} else {
			retrys++
		}
	}
	assert.Nil(t, err)

	tr.volume = connection.Volumeid
	t.Logf("connected to volume = %d\n", connection.Volumeid)
	b, err := json.Marshal(*portR)
	assert.Nil(t, err)
	err = ioutil.WriteFile("testpf.json", b, 0644)
	assert.Nil(t, err)

	_, err = c.connection.ReloadProxyConfig(ctx)
	assert.Nil(t, err)
	connection = connect(t, false, connection.Volumeid)
	assert.NotNil(t, connection)
	tr.connection = connection
	defer connection.CloseConnection(ctx)
	defer StopAndRemoveContainer(ctx, containername)
	vis, err := connection.GetProxyVolumes(ctx)
	if err != nil {
		t.Logf("error %v", err)
	}
	assert.Nil(t, err)
	assert.Equal(t, len(portR.ForwardEntrys), len(vis.VolumeInfoResponse))
	tr.connection = connect(t, tr.clientsidededupe, tr.volume)
	assert.NotNil(t, tr.connection)

	info, err := c.connection.GetVolumeInfo(ctx)
	assert.Nil(t, err)
	time.Sleep(35 * time.Second)
	cinfo, err := tr.connection.GetConnectedVolumes(ctx)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(cinfo))
	fn, sh := makeGenericFile(ctx, t, c.connection, "", 1024)
	fi, err := c.connection.Stat(ctx, fn)
	assert.Nil(t, err)
	_, err = tr.connection.SyncFromCloudVolume(ctx, info.SerialNumber, true)
	assert.Nil(t, err)
	nfi, err := tr.connection.Stat(ctx, fn)
	assert.Nil(t, err)
	dh, err := readFile(ctx, t, &tr, fn, false)
	assert.Nil(t, err)
	assert.Equal(t, dh, sh)
	assert.Equal(t, fi.Mode, nfi.Mode)
	err = c.connection.DeleteFile(ctx, fn)
	assert.Nil(t, err)
	time.Sleep(15 * time.Second)
	c.connection.CleanStore(ctx, true, true)
	time.Sleep(65 * time.Second)
	dh, err = readFile(ctx, t, &tr, fn, false)
	assert.Nil(t, err)
	assert.Equal(t, dh, sh)
	fn, sh = makeGenericFile(ctx, t, c.connection, "", 1024)
	_, err = c.connection.Stat(ctx, fn)
	assert.Nil(t, err)
	_, err = tr.connection.GetCloudFile(ctx, fn, fmt.Sprintf("nf%s", fn), true, true)
	assert.Nil(t, err)
	time.Sleep(35 * time.Second)
	dh, err = readFile(ctx, t, &tr, fn, false)
	assert.Nil(t, err)
	assert.Equal(t, dh, sh)
	assert.Equal(t, fi.Mode, nfi.Mode)
	err = c.connection.DeleteFile(ctx, fn)
	assert.Nil(t, err)
	time.Sleep(15 * time.Second)
	c.connection.CleanStore(ctx, true, true)
	time.Sleep(65 * time.Second)
	dh, err = readFile(ctx, t, &tr, fn, false)
	assert.Nil(t, err)
	assert.Equal(t, dh, sh)
	StopAndRemoveContainer(ctx, containername)
	portR = &paip.PortRedirectors{}
	for _, c := range maddress {
		portR.ForwardEntrys = append(portR.ForwardEntrys, *c.fe)
	}
	err = ioutil.WriteFile("testpf.json", b, 0644)
	assert.Nil(t, err)
	_, err = c.connection.ReloadProxyConfig(ctx)
	assert.Nil(t, err)
	vis, err = c.connection.GetProxyVolumes(ctx)
	if err != nil {
		t.Logf("error %v", err)
	}
	assert.Nil(t, err)
	assert.Equal(t, len(portR.ForwardEntrys), len(vis.VolumeInfoResponse))

}

func TestProxyVolumeInfo(t *testing.T) {
	var volumeIds []int64
	for _, c := range maddress {
		volumeIds = append(volumeIds, c.volume)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connection := connect(t, false, -1)
	vis, err := connection.GetProxyVolumes(ctx)
	if err != nil {
		t.Logf("error %v", err)
	}
	assert.Nil(t, err)
	var vids []int64
	for _, vi := range vis.VolumeInfoResponse {
		vids = append(vids, vi.SerialNumber)
		t.Logf("serial = %d", vi.SerialNumber)
	}
	assert.ElementsMatch(t, vids, volumeIds)
}

func TestReloadProxyVolume(t *testing.T) {
	var volumeIds []int64
	for _, c := range maddress {
		volumeIds = append(volumeIds, c.volume)
	}

	portR := &paip.PortRedirectors{}
	for i := 2; i < 4; i++ {
		fe := paip.ForwardEntry{Address: fmt.Sprintf("sdfs://localhost:644%d", i)}
		portR.ForwardEntrys = append(portR.ForwardEntrys, fe)
	}
	b, err := json.Marshal(*portR)
	assert.Nil(t, err)
	err = ioutil.WriteFile("testpf.json", b, 0644)
	assert.Nil(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connection := connect(t, false, -1)
	_, err = connection.ReloadProxyConfig(ctx)
	assert.Nil(t, err)
	vis, err := connection.GetProxyVolumes(ctx)
	if err != nil {
		t.Logf("error %v", err)
	}
	assert.Nil(t, err)
	for _, vi := range vis.VolumeInfoResponse {
		t.Logf("serial = %d", vi.SerialNumber)
	}
	assert.Equal(t, 2, len(vis.VolumeInfoResponse)-2)
	portR = &paip.PortRedirectors{}
	for i := 2; i < 5; i++ {
		fe := paip.ForwardEntry{Address: fmt.Sprintf("sdfs://localhost:644%d", i)}
		portR.ForwardEntrys = append(portR.ForwardEntrys, fe)
	}
	b, err = json.Marshal(*portR)
	assert.Nil(t, err)
	err = ioutil.WriteFile("testpf.json", b, 0644)
	assert.Nil(t, err)
	_, err = connection.ReloadProxyConfig(ctx)
	assert.Nil(t, err)
	vis, err = connection.GetProxyVolumes(ctx)
	if err != nil {
		t.Logf("error %v", err)
	}
	assert.Nil(t, err)
	var vids []int64
	for _, vi := range vis.VolumeInfoResponse {
		vids = append(vids, vi.SerialNumber)
		t.Logf("serial = %d", vi.SerialNumber)
	}
	assert.ElementsMatch(t, vids, volumeIds)

}

func TestMain(m *testing.M) {

	rand.Seed(time.Now().UTC().UnixNano())
	var err error
	var port = 2

	var containernames []string
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if runtime.GOOS != "windows" {

		//create local s3 storage
		mimagename := "docker.io/minio/minio:latest"
		containername := "minio"
		portopening := "9000"
		s3bucket := string(randBytesMaskImpr(16))
		inputEnv := []string{fmt.Sprintf("MINIO_ROOT_USER=%s", "MINIO"), fmt.Sprintf("MINIO_ROOT_PASSWORD=%s", "MINIO1234")}
		cmd := []string{"server", "/data"}
		_, err = RunContainer(ctx, mimagename, containername, portopening, portopening, inputEnv, cmd, false)
		if err != nil {
			fmt.Printf("Unable to create minio service %v", err)
		}
		// Create Block storage
		portopening = fmt.Sprintf("644%d", port)
		containername = fmt.Sprintf("portredirsdfs-%s", portopening)

		inputEnv = []string{"BACKUP_VOLUME=true", fmt.Sprintf("CAPACITY=%s", "1TB"), "EXTENDED_CMD=--hashtable-rm-threshold=1000"}

		inputEnv = append(inputEnv, "DISABLE_TLS=true")
		cmd = []string{}
		_, err = RunContainer(ctx, imagename, containername, portopening, "6442", inputEnv, cmd, true)
		if err != nil {
			fmt.Printf("Unable to create docker client %v", err)
		}
		btr := testRun{url: fmt.Sprintf("sdfs://localhost:644%d", port), name: "blockstorage", inputEnv: inputEnv, containerName: containername, containerPort: portopening, cloudVol: false}
		maddress = append(maddress, &btr)
		containernames = append(containernames, containername)
		port++
		// Create S3 storage
		portopening = fmt.Sprintf("644%d", port)
		containername = fmt.Sprintf("s3portredirsdfs-%s", portopening)

		inputEnv = []string{fmt.Sprintf("CAPACITY=%s", "1TB"), "EXTENDED_CMD=--hashtable-rm-threshold=1000 --aws-disable-dns-bucket=true --minio-enabled",
			fmt.Sprintf("TYPE=%s", "AWS"), fmt.Sprintf("URL=%s", "http://minio:9000"), fmt.Sprintf("BUCKET_NAME=%s", s3bucket),
			fmt.Sprintf("ACCESS_KEY=%s", "MINIO"), fmt.Sprintf("SECRET_KEY=%s", "MINIO1234")}
		inputEnv = append(inputEnv, "DISABLE_TLS=true")
		cmd = []string{}
		_, err = RunContainer(ctx, imagename, containername, portopening, "6442", inputEnv, cmd, true)
		if err != nil {
			fmt.Printf("Unable to create docker client %v", err)
		}
		s3tr := testRun{url: fmt.Sprintf("sdfs://localhost:644%d", port), name: "s3storage", inputEnv: inputEnv, containerName: containername, containerPort: portopening, cloudVol: true}
		maddress = append(maddress, &s3tr)
		containernames = append(containernames, containername)
		port++
		// Create Azure storage
		// Cleanup Azure Storage
		credential, err := azblob.NewSharedKeyCredential(os.Getenv("AZURE_ACCESS_KEY"), os.Getenv("AZURE_SECRET_KEY"))
		p := azblob.NewPipeline(credential, azblob.PipelineOptions{})
		u, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net", os.Getenv("AZURE_ACCESS_KEY")))
		serviceURL := azblob.NewServiceURL(*u, p)
		containerURL := serviceURL.NewContainerURL(os.Getenv("AZURE_BUCKET_NAME"))
		for marker := (azblob.Marker{}); marker.NotDone(); { // The parens around Marker{} are required to avoid compiler error.
			// Get a result segment starting with the blob indicated by the current Marker.
			listBlob, err := containerURL.ListBlobsFlatSegment(ctx, marker, azblob.ListBlobsSegmentOptions{})
			if err != nil {
				log.Fatal(err)
			}
			// IMPORTANT: ListBlobs returns the start of the next segment; you MUST use this to get
			// the next segment (after processing the current result segment).
			marker = listBlob.NextMarker

			// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
			for _, blobInfo := range listBlob.Segment.BlobItems {
				blobURL := containerURL.NewBlockBlobURL(blobInfo.Name)
				_, err = blobURL.Delete(ctx, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
				if err != nil {
					log.Fatal(err)
				}
			}
		}

		if err != nil {
			log.Fatal(err)
		}
		portopening = fmt.Sprintf("644%d", port)
		containername = fmt.Sprintf("azureportredirsdfs-%s", portopening)

		inputEnv = []string{"TYPE=AZURE", fmt.Sprintf("ACCESS_KEY=%s", os.Getenv("AZURE_ACCESS_KEY")), fmt.Sprintf("BUCKET_NAME=%s", os.Getenv("AZURE_BUCKET_NAME")), fmt.Sprintf("ACCESS_KEY=%s", os.Getenv("AZURE_ACCESS_KEY")), fmt.Sprintf("SECRET_KEY=%s", os.Getenv("AZURE_SECRET_KEY")), fmt.Sprintf("CAPACITY=%s", "1TB"), "EXTENDED_CMD=--hashtable-rm-threshold=1000"}
		inputEnv = append(inputEnv, "DISABLE_TLS=true")
		cmd = []string{}
		_, err = RunContainer(ctx, imagename, containername, portopening, "6442", inputEnv, cmd, true)
		if err != nil {
			fmt.Printf("Unable to create docker client %v", err)
		}
		aztr := testRun{url: fmt.Sprintf("sdfs://localhost:644%d", port), name: "azurestorage", inputEnv: inputEnv, containerName: containername, containerPort: portopening, cloudVol: true}
		maddress = append(maddress, &aztr)
		containernames = append(containernames, containername)
		port++
	}
	tls = true
	api.DisableTrust = true
	paip.ServerCACert = "out/signer_key.crt"
	paip.ServerCert = "out/tls_key.crt"
	paip.ServerKey = "out/tls_key.key"
	paip.ServerTls = true

	cmp := make(map[int64]*grpc.ClientConn)
	dd := make(map[int64]paip.ForwardEntry)
	portR := &paip.PortRedirectors{}
	for _, m := range maddress {
		fe := paip.ForwardEntry{Address: m.url}
		portR.ForwardEntrys = append(portR.ForwardEntrys, fe)
		connection, err := api.NewConnection(m.url, false, true, -1, 0, 0)
		retrys := 0
		for err != nil {
			log.Printf("retries = %d", retrys)
			time.Sleep(20 * time.Second)
			connection, err = api.NewConnection(m.url, false, true, -1, 0, 0)
			if retrys > 10 {
				fmt.Printf("SDFS Server connection timed out %s\n", m.url)
				os.Exit(-1)
			} else {
				retrys++
			}
		}
		if err != nil {
			fmt.Printf("Unable to create connection %v", err)
		}
		log.Printf("connected to volume = %d for %s", connection.Volumeid, m.containerName)
		m.volume = connection.Volumeid
		cmp[connection.Volumeid] = connection.Clnt
		fe = paip.ForwardEntry{
			Address:       m.url,
			Dedupe:        false,
			DedupeThreads: 1,
			DedupeBuffer:  4,
		}
		dd[connection.Volumeid] = fe
		m.fe = &fe
	}
	paip.ServerMtls = true
	mtls = true
	paip.NOSHUTDOWN = true
	b, err := json.Marshal(*portR)
	if err != nil {
		fmt.Printf("Unable to serialize portredirectors %v", err)
	}
	err = ioutil.WriteFile("testpf.json", b, 0644)
	defer os.Remove("testpf.json")
	if err != nil {
		fmt.Printf("Unable to write portredirectors to file %v", err)
	}
	pf := paip.NewPortRedirector("testpf.json", lport, false, nil, false)
	pf.Cmp = cmp
	pf.Dd = dd
	paip.ServerMtls = true
	paip.AnyCert = true
	mtls = true

	go paip.StartServer(cmp, lport, false, dd, false, false, password, pf, false)
	fmt.Printf("Server initialized at %s\n", lport)
	code := m.Run()
	fmt.Printf("AnyCert MTLS Testing code is %d\n", code)
	paip.StopServer()
	if runtime.GOOS != "windows" {
		for _, containername := range containernames {
			StopAndRemoveContainer(ctx, containername)
		}
		StopAndRemoveContainer(ctx, "minio")
	}
	os.Exit(code)
}

func makeFile(ctx context.Context, t *testing.T, c *testRun, parent string, size int64) (string, []byte) {
	return makeGenericFile(ctx, t, c.connection, parent, size)
}

func makeLargeBlockFile(ctx context.Context, t *testing.T, c *testRun, parent string, size int64, blocksize int) (string, []byte) {
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
		t.Logf("Wrote blocksize %d", len(b))
		assert.Nil(t, err)
		offset += int64(len(b))
		b = nil
	}
	stat, _ = connection.GetAttr(ctx, fn)
	assert.Equal(t, stat.Size, maxoffset)
	_ = connection.Release(ctx, fh)
	return fn, h.Sum(nil)
}

func readFile(ctx context.Context, t *testing.T, c *testRun, filenm string, delete bool) (data []byte, err error) {
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

func deleteFile(t *testing.T, c *testRun, fn string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := c.connection.DeleteFile(ctx, fn)
	assert.Nil(t, err)
	_, err = c.connection.GetAttr(ctx, fn)
	assert.NotNil(t, err)
}

func connect(t *testing.T, dedupe bool, volumeid int64) *api.SdfsConnection {

	//api.DisableTrust = true
	api.Debug = false
	api.UserName = "admin"
	api.Password = "admin"
	api.Mtls = false
	var address = "sdfs://localhost:16442"
	if tls {
		address = "sdfss://localhost:16442"
	}
	if mtls {
		api.Mtls = true
		api.DisableTrust = true
		api.MtlsCACert = "out/signer_key.crt"
		api.MtlsCert = "out/client_key.crt"
		api.MtlsKey = "out/client_key.key"
	}

	connection, err := api.NewConnection(address, dedupe, true, volumeid, 40000, 60)
	if err != nil {
		t.Errorf("Unable to connect to %s error: %v\n", address, err)
		return nil
	}
	t.Logf("Connection state %s", connection.Clnt.GetState())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	volinfo, err := connection.GetVolumeInfo(ctx)
	assert.Nil(t, err)
	if err != nil {

		t.Errorf("Unable to get volume info for %s error: %v\n", address, err)
		return nil
	}
	assert.Equal(t, volinfo.SerialNumber, volumeid)
	if volinfo.SerialNumber != volumeid {
		t.Errorf("Volume serial numbers don't match expected %d got %d\n", volumeid, volinfo.SerialNumber)
		return nil
	}
	return connection
}

func dconnect(t *testing.T, c *testRun) *api.SdfsConnection {

	//api.DisableTrust = true
	api.Debug = false
	api.UserName = "admin"
	api.Password = "admin"
	api.Mtls = false
	var address = c.url
	vid := int64(-1)
	if !c.direct {
		address = "sdfss://localhost:16442"
		if tls {
			address = "sdfss://localhost:16442"
		}
		if mtls {
			api.Mtls = true
			api.DisableTrust = true
			api.MtlsCACert = "out/signer_key.crt"
			api.MtlsCert = "out/client_key.crt"
			api.MtlsKey = "out/client_key.key"
		}
		vid = c.volume
	}
	t.Logf("Connecting to %s dedupe is %v", address, c.clientsidededupe)

	connection, err := api.NewConnection(address, c.clientsidededupe, true, vid, 40000, 60)
	if err != nil {
		t.Errorf("Unable to connect to %s error: %v\n", address, err)
		return nil
	}
	t.Logf("Connection state %s", connection.Clnt.GetState())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	volinfo, err := connection.GetVolumeInfo(ctx)
	assert.Nil(t, err)
	if err != nil {

		t.Errorf("Unable to get volume info for %s error: %v\n", address, err)
		return nil
	}
	assert.Equal(t, volinfo.SerialNumber, c.volume)
	if volinfo.SerialNumber != c.volume {
		t.Errorf("Volume serial numbers don't match expected %d got %d\n", c.volume, volinfo.SerialNumber)
		return nil
	}
	return connection
}
