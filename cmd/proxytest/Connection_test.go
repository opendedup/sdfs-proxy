package test

import (
	"context"
	"runtime"

	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	network "github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	natting "github.com/docker/go-connections/nat"
	api "github.com/opendedup/sdfs-client-go/api"
	spb "github.com/opendedup/sdfs-client-go/sdfs"
	paip "github.com/opendedup/sdfs-proxy/api"
	"github.com/stretchr/testify/assert"
	"golang.org/x/crypto/blake2b"
	"google.golang.org/grpc"
)

var maddress = "sdfss://localhost:6442"
var address = "sdfs://localhost:16442"
var port = "localhost:16442"
var imagename = "gcr.io/hybrics/hybrics:master"
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

func TestNewConnection(t *testing.T) {
	connection := connect(t, false)
	ctx, cancel := context.WithCancel(context.Background())
	defer connection.CloseConnection(ctx)
	defer cancel()
	assert.NotNil(t, connection)
}

func TestChow(t *testing.T) {
	if runtime.GOOS != "windows" {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		connection := connect(t, false)
		defer connection.CloseConnection(ctx)
		assert.NotNil(t, connection)
		fn, _ := makeFile(t, "", 128, false)
		err := connection.Chown(ctx, fn, int32(100), int32(100))
		assert.Nil(t, err)
		stat, err := connection.GetAttr(ctx, fn)
		assert.Nil(t, err)
		assert.Equal(t, stat.Gid, int32(100))
		assert.Equal(t, stat.Uid, int32(100))
		deleteFile(t, fn)
	}
}

func TestMkNod(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connection := connect(t, false)
	defer connection.CloseConnection(ctx)
	assert.NotNil(t, connection)
	fn, _ := makeFile(t, "", 128, false)
	exists, err := connection.FileExists(ctx, fn)
	assert.Nil(t, err)
	assert.True(t, exists)
	deleteFile(t, fn)
}

func TestMkDir(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connection := connect(t, false)
	defer connection.CloseConnection(ctx)
	assert.NotNil(t, connection)
	err := connection.MkDir(ctx, "testdir", 511)
	assert.Nil(t, err)
	stat, err := connection.GetAttr(ctx, "testdir")
	assert.Nil(t, err)
	if runtime.GOOS != "windows" {
		assert.Equal(t, stat.Mode, int32(16895))
	}
	err = connection.RmDir(ctx, "testdir")
	assert.Nil(t, err)
	_, err = connection.GetAttr(ctx, "testdir")
	assert.NotNil(t, err)
	connection.CloseConnection(ctx)
}

func TestMkDirAll(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connection := connect(t, false)
	defer connection.CloseConnection(ctx)
	assert.NotNil(t, connection)
	err := connection.MkDirAll(ctx, "testdir/t")
	assert.Nil(t, err)
	stat, err := connection.GetAttr(ctx, "testdir/t")
	assert.Nil(t, err)
	if runtime.GOOS != "windows" {
		assert.Equal(t, stat.Mode, int32(16832))
	}
	err = connection.RmDir(ctx, "testdir/t")
	assert.Nil(t, err)
	_, err = connection.GetAttr(ctx, "testdir/t")
	assert.NotNil(t, err)
	err = connection.RmDir(ctx, "testdir")
	assert.Nil(t, err)
	_, err = connection.GetAttr(ctx, "testdir")
	assert.NotNil(t, err)
	connection.CloseConnection(ctx)
}

func TestListDir(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connection := connect(t, false)
	assert.NotNil(t, connection)
	defer connection.CloseConnection(ctx)
	dn := string(randBytesMaskImpr(16))
	err := connection.MkDir(ctx, dn, 511)
	assert.Nil(t, err)
	var files []string
	for i := 0; i < 10; i++ {
		fn, _ := makeFile(t, dn, 1024, false)
		files = append(files, fn)
	}
	_, list, err := connection.ListDir(ctx, dn, "", false, 20)
	assert.Nil(t, err)
	var afiles []string
	for _, l := range list {
		afiles = append(afiles, l.FilePath)
		connection.DeleteFile(ctx, l.FilePath)
	}
	if runtime.GOOS != "windows" {
		assert.ElementsMatch(t, files, afiles)
	} else {
		assert.Equal(t, len(files), len(afiles))
	}
	err = connection.RmDir(ctx, dn)
	assert.Nil(t, err)
	_, err = connection.GetAttr(ctx, dn)
	assert.NotNil(t, err)
}

func TestCleanStore(t *testing.T) {
	cleanStore(t, 30)

}

func TestStatFS(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connection := connect(t, false)
	assert.NotNil(t, connection)
	defer connection.CloseConnection(ctx)
	_, err := connection.StatFS(ctx)
	assert.Nil(t, err)
}

func TestRename(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connection := connect(t, false)
	assert.NotNil(t, connection)
	defer connection.CloseConnection(ctx)
	fn, _ := makeFile(t, "", 1024, false)
	nfn := string(randBytesMaskImpr(16))

	err := connection.Rename(ctx, fn, nfn)
	assert.Nil(t, err)
	_, err = connection.Stat(ctx, fn)
	assert.NotNil(t, err)
	_, err = connection.Stat(ctx, nfn)
	assert.Nil(t, err)
	err = connection.DeleteFile(ctx, nfn)
	assert.Nil(t, err)
}

func TestCopyFile(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connection := connect(t, false)
	assert.NotNil(t, connection)
	defer connection.CloseConnection(ctx)
	fn, hash := makeFile(t, "", 1024, false)
	nfn := string(randBytesMaskImpr(16))
	_, err := connection.CopyFile(ctx, fn, nfn, false)
	assert.Nil(t, err)
	nhash := readFile(t, nfn, false)
	assert.Equal(t, hash, nhash)
	err = connection.DeleteFile(ctx, nfn)
	assert.Nil(t, err)
	err = connection.DeleteFile(ctx, fn)
	assert.Nil(t, err)
}

func TestEvents(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connection := connect(t, false)
	assert.NotNil(t, connection)
	defer connection.CloseConnection(ctx)
	fn, hash := makeFile(t, "", 1024, false)
	nfn := string(randBytesMaskImpr(16))
	evt, err := connection.CopyFile(ctx, fn, nfn, true)
	assert.Nil(t, err)
	_, err = connection.WaitForEvent(ctx, evt.Uuid)
	assert.Nil(t, err)
	nhash := readFile(t, nfn, false)
	assert.Equal(t, hash, nhash)
	err = connection.DeleteFile(ctx, nfn)
	assert.Nil(t, err)
	err = connection.DeleteFile(ctx, fn)
	assert.Nil(t, err)
	_, err = connection.GetEvent(ctx, evt.Uuid)
	assert.Nil(t, err)
	_, err = connection.ListEvents(ctx)
	assert.Nil(t, err)
}

func TestXAttrs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connection := connect(t, false)
	assert.NotNil(t, connection)
	defer connection.CloseConnection(ctx)
	fn, _ := makeFile(t, "", 1024, false)
	_, err := connection.GetXAttrSize(ctx, fn, "key")
	assert.NotNil(t, err)
	err = connection.SetXAttr(ctx, "key", "value", fn)
	assert.Nil(t, err)
	val, err := connection.GetXAttr(ctx, "key", fn)
	assert.Equal(t, val, "value")
	assert.Nil(t, err)
	_, err = connection.GetXAttrSize(ctx, fn, "key")
	assert.Nil(t, err)
	err = connection.RemoveXAttr(ctx, "key", fn)
	assert.Nil(t, err)
	_, err = connection.GetXAttrSize(ctx, fn, "key")
	assert.NotNil(t, err)
	fa := []*spb.FileAttributes{{Key: "key1", Value: "value1"}, {Key: "key2", Value: "value2"}}
	err = connection.SetUserMetaData(ctx, fn, fa)
	assert.Nil(t, err)
	_, fal, err := connection.ListDir(ctx, fn, "", false, int32(1000))
	assert.Nil(t, err)
	for _, attrs := range fal {
		if attrs.FileAttributes[0].Key == "key1" {
			assert.Equal(t, attrs.FileAttributes[0].Value, "value1")
		} else {
			assert.Equal(t, attrs.FileAttributes[0].Key, "key2")
			assert.Equal(t, attrs.FileAttributes[0].Value, "value2")
		}
	}
	err = connection.DeleteFile(ctx, fn)
	assert.Nil(t, err)
}

func TestSetUtime(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connection := connect(t, false)
	assert.NotNil(t, connection)
	defer connection.CloseConnection(ctx)
	fn, _ := makeFile(t, "", 1024, false)
	err := connection.Utime(ctx, fn, int64(0), int64(0))
	assert.Nil(t, err)
	stat, err := connection.GetAttr(ctx, fn)
	assert.Nil(t, err)
	assert.Equal(t, stat.Atime, int64(0))
	assert.Equal(t, stat.Mtim, int64(0))
	err = connection.DeleteFile(ctx, fn)
	assert.Nil(t, err)
}

func TestTuncate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connection := connect(t, false)
	assert.NotNil(t, connection)
	defer connection.CloseConnection(ctx)
	fn, _ := makeFile(t, "", 1024*1024*1024*10, false)
	err := connection.Truncate(ctx, fn, int64(0))
	assert.Nil(t, err)
	stat, err := connection.GetAttr(ctx, fn)
	assert.Nil(t, err)
	assert.Equal(t, stat.Size, int64(0))
	err = connection.DeleteFile(ctx, fn)
	assert.Nil(t, err)
}

func TestWriteLargeBlock(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connection := connect(t, false)
	assert.NotNil(t, connection)
	defer connection.CloseConnection(ctx)
	tMb := int64(1024 * 1024 * 10)
	fMb := 1024 * 5
	t.Logf("Writing file \n")
	fn, _ := makeLargeBlockFile(t, "", tMb, false, fMb)
	t.Logf("Wrote %s\n", fn)
	err := connection.DeleteFile(ctx, fn)
	assert.Nil(t, err)
}

func TestSymLink(t *testing.T) {
	if runtime.GOOS != "windows" {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		connection := connect(t, false)
		assert.NotNil(t, connection)
		defer connection.CloseConnection(ctx)
		fn, _ := makeFile(t, "", 1024, false)
		sfn := string(randBytesMaskImpr(16))
		err := connection.SymLink(ctx, fn, sfn)
		assert.Nil(t, err)
		_sfn, err := connection.ReadLink(ctx, sfn)
		assert.Nil(t, err)
		assert.Equal(t, fn, _sfn)
		_, err = connection.GetAttr(ctx, sfn)
		assert.Nil(t, err)
		_, fls, err := connection.ListDir(ctx, "/", "", false, int32(100))
		assert.Equal(t, len(fls), 2)
		assert.Nil(t, err)
		err = connection.Unlink(ctx, sfn)
		assert.Nil(t, err)
		_, err = connection.GetAttr(ctx, sfn)
		assert.NotNil(t, err)
		err = connection.DeleteFile(ctx, fn)
		assert.Nil(t, err)
	}
}

func TestSync(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connection := connect(t, false)
	assert.NotNil(t, connection)
	defer connection.CloseConnection(ctx)
	fn, _ := makeFile(t, "", 1024, false)
	fh, err := connection.Open(ctx, fn, int32(-1))
	assert.Nil(t, err)
	b := randBytesMaskImpr(16)
	err = connection.Write(ctx, fh, b, 0, int32(len(b)))
	assert.Nil(t, err)
	err = connection.Flush(ctx, fn, fh)
	assert.Nil(t, err)
	err = connection.Release(ctx, fh)
	assert.Nil(t, err)
	err = connection.DeleteFile(ctx, fn)
	assert.Nil(t, err)
}

func TestMaxAge(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connection := connect(t, false)
	assert.NotNil(t, connection)
	defer connection.CloseConnection(ctx)
	info, err := connection.DSEInfo(ctx)
	assert.Nil(t, err)
	t.Logf("max age : %d", info.MaxAge)
	err = connection.SetMaxAge(ctx, 1000)
	assert.Nil(t, err)
	info, err = connection.DSEInfo(ctx)
	assert.Nil(t, err)
	assert.Equal(t, int64(1000), info.MaxAge)
	t.Logf("new max age : %d", info.MaxAge)
	fsz := int64(1024 * 1024)
	_nfn, _ := makeFile(t, "", fsz, false)
	info, err = connection.DSEInfo(ctx)
	assert.Nil(t, err)

	sz := info.CurrentSize
	nfn := string(randBytesMaskImpr(16))
	time.Sleep(10 * time.Second)

	_, err = connection.Download(ctx, _nfn, nfn)
	defer os.Remove(nfn)
	assert.Nil(t, err)
	_, err = connection.Upload(ctx, nfn, nfn)
	assert.Nil(t, err)
	os.Remove(_nfn)
	info, err = connection.DSEInfo(ctx)
	assert.Nil(t, err)
	nsz := info.CurrentSize
	t.Logf("sz = %d nsz =%d", sz, nsz)
	assert.Less(t, sz, nsz)
	err = connection.DeleteFile(ctx, nfn)
	assert.Nil(t, err)
	err = connection.DeleteFile(ctx, _nfn)
	assert.Nil(t, err)
	time.Sleep(10 * time.Second)
	connection.CleanStore(ctx, true, true)
	tm := time.Duration(30 * int(time.Second))
	time.Sleep(tm)
	info, err = connection.DSEInfo(ctx)
	assert.Nil(t, err)
	fnsz := info.CurrentSize
	t.Logf("sz = %d nsz =%d, fnsz=%d", sz, nsz, fnsz)
	assert.Greater(t, sz, fnsz)
	_nfn, hs := makeFile(t, "", fsz, false)
	nfn = string(randBytesMaskImpr(16))
	time.Sleep(10 * time.Second)
	connection.CopyFile(ctx, _nfn, nfn, false)
	connection.DeleteFile(ctx, _nfn)
	time.Sleep(10 * time.Second)
	connection.CleanStore(ctx, true, true)
	tm = time.Duration(30 * int(time.Second))
	time.Sleep(tm)
	info, err = connection.DSEInfo(ctx)
	assert.Nil(t, err)
	fnsz = info.CurrentSize
	t.Logf("sz = %d, fnsz=%d", sz, fnsz)
	nhs := readFile(t, nfn, true)
	assert.Equal(t, hs, nhs)
	connection.DeleteFile(ctx, _nfn)
	time.Sleep(10 * time.Second)
	connection.CleanStore(ctx, true, true)
	tm = time.Duration(60 * int(time.Second))
	time.Sleep(tm)
	info, err = connection.DSEInfo(ctx)
	assert.Nil(t, err)
	fnsz = info.CurrentSize
	t.Logf("sz = %d, fnsz=%d", sz, fnsz)
	_nfn, _ = makeFile(t, "", fsz, false)
	nfn = string(randBytesMaskImpr(16))
	time.Sleep(10 * time.Second)
	_, err = connection.Download(ctx, _nfn, nfn)
	assert.Nil(t, err)
	for i := 0; i < 10; i++ {
		_, err = connection.Upload(ctx, nfn, fmt.Sprintf("file%d", i))
		if err != nil {
			t.Logf("upload error %v", err)
		}
		info, err := connection.Stat(ctx, fmt.Sprintf("file%d", i))
		assert.GreaterOrEqual(t, info.IoMonitor.ActualBytesWritten, int64(0))
		assert.Nil(t, err)
		time.Sleep(10 * time.Second)
	}
	info, _ = connection.DSEInfo(ctx)
	sz = info.CurrentSize
	connection.DeleteFile(ctx, _nfn)
	time.Sleep(10 * time.Second)
	connection.CleanStore(ctx, true, true)
	tm = time.Duration(60 * int(time.Second))
	time.Sleep(tm)
	info, _ = connection.DSEInfo(ctx)
	nsz = info.CurrentSize
	t.Logf("sz = %d, nsz=%d", sz, nsz)
	assert.Less(t, nsz, sz)
	for i := 0; i < 10; i++ {
		err = connection.DeleteFile(ctx, fmt.Sprintf("file%d", i))
		if err != nil {
			t.Logf("upload error %v", err)
		}
	}
	time.Sleep(10 * time.Second)
	connection.CleanStore(ctx, true, true)
	tm = time.Duration(60 * int(time.Second))
	time.Sleep(tm)
	info, _ = connection.DSEInfo(ctx)
	sz = info.CurrentSize
	t.Logf("sz = %d, nsz=%d", sz, nsz)
	assert.Less(t, sz, nsz)
	os.Remove(nfn)
	err = connection.SetMaxAge(ctx, -1)
	assert.Nil(t, err)
}

func TestCopyExtent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connection := connect(t, false)
	assert.NotNil(t, connection)
	defer connection.CloseConnection(ctx)
	fn, _ := makeFile(t, "", 1024, false)
	sfn, _ := makeFile(t, "", 1024, false)
	fh, err := connection.Open(ctx, fn, int32(-1))
	assert.Nil(t, err)
	sfh, err := connection.Open(ctx, sfn, int32(-1))
	assert.Nil(t, err)
	b := randBytesMaskImpr(16)
	err = connection.Write(ctx, fh, b, 0, int32(len(b)))
	assert.Nil(t, err)
	err = connection.Flush(ctx, fn, fh)
	assert.Nil(t, err)
	err = connection.Release(ctx, fh)
	assert.Nil(t, err)
	fh, err = connection.Open(ctx, fn, int32(-1))
	assert.Nil(t, err)
	_, err = connection.CopyExtent(ctx, fn, sfn, 0, 0, int64(len(b)))
	assert.Nil(t, err)
	err = connection.Flush(ctx, sfn, sfh)
	assert.Nil(t, err)
	err = connection.Release(ctx, sfh)
	assert.Nil(t, err)
	sfh, err = connection.Open(ctx, sfn, int32(-1))
	assert.Nil(t, err)
	nb, err := connection.Read(ctx, sfh, 0, int32(len(b)))
	assert.Nil(t, err)
	assert.Equal(t, nb, b)
	err = connection.Release(ctx, fh)
	assert.Nil(t, err)
	err = connection.Release(ctx, sfh)
	assert.Nil(t, err)
	err = connection.DeleteFile(ctx, fn)
	assert.Nil(t, err)
	err = connection.DeleteFile(ctx, sfn)
	assert.Nil(t, err)
}

func TestInfo(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connection := connect(t, false)
	assert.NotNil(t, connection)
	defer connection.CloseConnection(ctx)
	_, err := connection.GetVolumeInfo(ctx)
	assert.Nil(t, err)
	_, err = connection.DSEInfo(ctx)
	assert.Nil(t, err)
	_, err = connection.SystemInfo(ctx)
	assert.Nil(t, err)
}

func TestGCSchedule(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connection := connect(t, false)
	assert.NotNil(t, connection)
	defer connection.CloseConnection(ctx)
	gc, err := connection.GetGCSchedule(ctx)
	assert.Nil(t, err)
	t.Logf("GC Sched = %s", gc)
}

func TestCache(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connection := connect(t, false)
	assert.NotNil(t, connection)
	defer connection.CloseConnection(ctx)

	_, err := connection.SetCacheSize(ctx, tb, true)
	assert.NotNil(t, err)
	_, err = connection.SetCacheSize(ctx, int64(20)*gb, true)
	assert.Nil(t, err)
	dse, err := connection.DSEInfo(ctx)
	assert.Nil(t, err)
	assert.Equal(t, int64(20)*gb, dse.MaxCacheSize)
}

func TestSetRWSpeed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connection := connect(t, false)
	assert.NotNil(t, connection)
	defer connection.CloseConnection(ctx)
	err := connection.SetReadSpeed(ctx, int32(1000))
	assert.Nil(t, err)
	err = connection.SetWriteSpeed(ctx, int32(2000))
	assert.Nil(t, err)
	dse, err := connection.DSEInfo(ctx)
	assert.Nil(t, err)
	assert.Equal(t, int32(1000), dse.ReadSpeed)
	assert.Equal(t, int32(2000), dse.WriteSpeed)
}

func TestSetVolumeSize(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	connection := connect(t, false)
	assert.NotNil(t, connection)
	defer connection.CloseConnection(ctx)
	err := connection.SetVolumeCapacity(ctx, int64(100)*tb)
	assert.Nil(t, err)
	vol, err := connection.GetVolumeInfo(ctx)
	assert.Nil(t, err)
	assert.Equal(t, int64(100)*tb, vol.Capactity)
}

func cleanStore(t *testing.T, dur int) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connection := connect(t, false)
	assert.NotNil(t, connection)
	defer connection.CloseConnection(ctx)
	var files []string
	for i := 0; i < 10; i++ {
		fn, _ := makeFile(t, "", 1024*1024, false)
		files = append(files, fn)
	}
	_nfn, nh := makeFile(t, "", 1024*1024, false)
	info, err := connection.DSEInfo(ctx)
	assert.Nil(t, err)
	sz := info.CurrentSize
	for _, l := range files {
		connection.DeleteFile(ctx, l)
	}
	time.Sleep(10 * time.Second)
	connection.CleanStore(ctx, true, true)
	tm := time.Duration(dur * int(time.Second))
	time.Sleep(tm)
	info, err = connection.DSEInfo(ctx)
	assert.Nil(t, err)
	nsz := info.CurrentSize
	assert.Greater(t, sz, nsz)
	nhn := readFile(t, _nfn, true)
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

func TestShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connection := connect(t, false)
	assert.NotNil(t, connection)
	defer connection.CloseConnection(ctx)
	_, err := connection.DSEInfo(ctx)
	assert.Nil(t, err)
	err = connection.ShutdownVolume(ctx)
	assert.Nil(t, err)
	time.Sleep(20 * time.Second)
	//because the volume is not actually shutdown during debug
	_, err = connection.DSEInfo(ctx)
	assert.Nil(t, err)

}

func TestUpload(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connection := connect(t, false)
	assert.NotNil(t, connection)
	defer connection.CloseConnection(ctx)
	fn := string(randBytesMaskImpr(16))
	data := randBytesMaskImpr(1024)
	h, err := blake2b.New(32, make([]byte, 0))

	assert.Nil(t, err)
	err = ioutil.WriteFile(fn, data, 0777)
	assert.Nil(t, err)
	h.Write(data)
	bs := h.Sum(nil)
	wr, err := connection.Upload(ctx, fn, fn)
	assert.Nil(t, err)
	assert.Equal(t, int64(len(data)), wr)
	nhs := readFile(t, fn, false)
	assert.Equal(t, bs, nhs)
	nfn := string(randBytesMaskImpr(16))
	rr, err := connection.Download(ctx, fn, nfn)
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
	connection.DeleteFile(ctx, fn)
}

func TestMain(m *testing.M) {

	rand.Seed(time.Now().UTC().UnixNano())
	var cli *client.Client
	var containername string
	var err error
	if runtime.GOOS != "windows" {
		cli, err = client.NewClientWithOpts(client.FromEnv)
		if err != nil {
			fmt.Printf("Unable to create docker client %v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		cli.NegotiateAPIVersion(ctx)
		containername = string(randBytesMaskImpr(16))
		portopening := "6442"
		inputEnv := []string{fmt.Sprintf("CAPACITY=%s", "1TB"), "EXTENDED_CMD=--hashtable-rm-threshold=1000"}
		cmd := []string{}
		_, err = runContainer(cli, imagename, containername, portopening, portopening, inputEnv, cmd)
		if err != nil {
			fmt.Printf("Unable to create docker client %v", err)
		}
	}
	api.DisableTrust = true
	connection, err := api.NewConnection(maddress, false, -1)
	retrys := 0
	for err != nil {
		log.Printf("retries = %d", retrys)
		time.Sleep(20 * time.Second)
		connection, err = api.NewConnection(maddress, false, -1)
		if retrys > 10 {
			break
		} else {
			retrys++
		}
	}
	if err != nil {
		fmt.Printf("Unable to create connection %v", err)
	}

	log.Printf("connected to volume = %d", connection.Volumeid)
	paip.NOSHUTDOWN = true
	if connection != nil {
		cmp := make(map[int64]*grpc.ClientConn)
		cmp[connection.Volumeid] = connection.Clnt
		dd := make(map[int64]bool)
		dd[connection.Volumeid] = true
		go paip.StartServer(cmp, port, true, dd, true, false, password)
	}
	fmt.Printf("Server initialized at %s\n", port)
	code := m.Run()
	paip.StopServer()
	if runtime.GOOS != "windows" {
		stopAndRemoveContainer(cli, containername)
	}
	os.Exit(code)
}

func runContainer(client *client.Client, imagename string, containername string, hostPort, port string, inputEnv []string, cmd []string) (string, error) {
	// Define a PORT opening
	newport, err := natting.NewPort("tcp", port)
	if err != nil {
		fmt.Println("Unable to create docker port")
		return "", err
	}

	// Configured hostConfig:
	// https://godoc.org/github.com/docker/docker/api/types/container#HostConfig
	hostConfig := &container.HostConfig{
		NetworkMode: "testnw",
		PortBindings: natting.PortMap{
			newport: []natting.PortBinding{
				{
					HostIP:   "0.0.0.0",
					HostPort: hostPort,
				},
			},
		},
		RestartPolicy: container.RestartPolicy{
			Name: "always",
		},
		LogConfig: container.LogConfig{
			Type:   "json-file",
			Config: map[string]string{},
		},
	}

	// Define Network config (why isn't PORT in here...?:
	// https://godoc.org/github.com/docker/docker/api/types/network#NetworkingConfig
	networkConfig := &network.NetworkingConfig{
		EndpointsConfig: map[string]*network.EndpointSettings{},
	}
	gatewayConfig := &network.EndpointSettings{
		Gateway: "gatewayname",
	}
	networkConfig.EndpointsConfig["testnw"] = gatewayConfig

	// Define ports to be exposed (has to be same as hostconfig.portbindings.newport)
	exposedPorts := map[natting.Port]struct{}{
		newport: {},
	}

	// Configuration
	// https://godoc.org/github.com/docker/docker/api/types/container#Config
	config := &container.Config{
		Image:        imagename,
		Env:          inputEnv,
		ExposedPorts: exposedPorts,
		Hostname:     containername,
	}
	if len(cmd) > 0 {
		config.Cmd = cmd
	}

	// Creating the actual container. This is "nil,nil,nil" in every example.
	cont, err := client.ContainerCreate(
		context.Background(),
		config,
		hostConfig,
		networkConfig, nil,
		containername,
	)

	if err != nil {
		log.Println(err)
		return "", err
	}

	// Run the actual container
	err = client.ContainerStart(context.Background(), cont.ID, types.ContainerStartOptions{})
	if err != nil {
		log.Println(err)
		return "", err
	}
	log.Printf("Container %s is created", cont.ID)

	return cont.ID, nil
}

func stopAndRemoveContainer(client *client.Client, containername string) error {
	ctx := context.Background()
	log.Printf("Stopping container %s", containername)
	if err := client.ContainerStop(ctx, containername, nil); err != nil {
		log.Printf("Unable to stop container %s: %s", containername, err)
	}

	removeOptions := types.ContainerRemoveOptions{
		RemoveVolumes: true,
		Force:         true,
	}

	if err := client.ContainerRemove(ctx, containername, removeOptions); err != nil {
		log.Printf("Unable to remove container: %s", err)
		return err
	}

	return nil
}

func makeFile(t *testing.T, parent string, size int64, dedupe bool) (string, []byte) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connection := connect(t, dedupe)
	defer connection.CloseConnection(ctx)
	assert.NotNil(t, connection)
	return makeGenericFile(ctx, t, connection, parent, size)
}

func makeLargeBlockFile(t *testing.T, parent string, size int64, dedupe bool, blocksize int) (string, []byte) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connection := connect(t, dedupe)
	defer connection.CloseConnection(ctx)
	assert.NotNil(t, connection)
	return makeLargeBlockGenericFile(ctx, t, connection, parent, size, blocksize)
}

func makeGenericFile(ctx context.Context, t *testing.T, connection *api.SdfsConnection, parent string, size int64) (string, []byte) {
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
	stat, _ = connection.GetAttr(ctx, fn)
	assert.Equal(t, stat.Size, maxoffset)
	_ = connection.Release(ctx, fh)
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

func readFile(t *testing.T, filenm string, delete bool) []byte {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connection := connect(t, false)
	assert.NotNil(t, connection)
	stat, err := connection.GetAttr(ctx, filenm)
	assert.Nil(t, err)

	assert.Greater(t, stat.Size, int64(0))
	fh, err := connection.Open(ctx, filenm, 0)
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
		b, err := connection.Read(ctx, fh, offset, int32(readSize))
		h.Write(b)
		assert.Nil(t, err)
		offset += int64(len(b))
		b = nil
	}
	err = connection.Release(ctx, fh)
	assert.Nil(t, err)

	if delete {
		err = connection.DeleteFile(ctx, filenm)
		assert.Nil(t, err)
		_, err = connection.GetAttr(ctx, filenm)
		assert.NotNil(t, err)
	}

	connection.CloseConnection(ctx)
	bs := h.Sum(nil)
	return bs
}

func deleteFile(t *testing.T, fn string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connection := connect(t, false)
	defer connection.CloseConnection(ctx)
	assert.NotNil(t, connection)
	err := connection.DeleteFile(ctx, fn)
	assert.Nil(t, err)
	_, err = connection.GetAttr(ctx, fn)
	assert.NotNil(t, err)
}

func connect(t *testing.T, dedupe bool) *api.SdfsConnection {

	//api.DisableTrust = true
	api.Debug = true
	api.UserName = "admin"
	api.Password = "admin"

	connection, err := api.NewConnection(address, dedupe, -1)
	if err != nil {
		t.Errorf("Unable to connect to %s error: %v\n", address, err)
		return nil
	}
	return connection
}
