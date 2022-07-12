package test

import (
	"bytes"
	"context"
	"encoding/json"
	"os/exec"
	"reflect"
	"runtime"
	"sync"

	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"

	"net/http"
	_ "net/http/pprof"

	api "github.com/opendedup/sdfs-client-go/api"
	spb "github.com/opendedup/sdfs-client-go/sdfs"
	paip "github.com/opendedup/sdfs-proxy/api"
	"github.com/stretchr/testify/assert"
	"golang.org/x/crypto/blake2b"
	"google.golang.org/grpc"
)

type containerConfig struct {
	cpu                                               int64
	memory                                            int64
	encrypt                                           bool
	mountstorage                                      bool
	imagename, containername, hostPort, containerPort string
	inputEnv, cmd                                     []string
	copyFile                                          bool
	attachProfiler                                    bool
}

type testRun struct {
	name             string
	volume           int64
	clientsidededupe bool
	url              string
	connection       *api.SdfsConnection
	cloudVol         bool
	fe               *paip.ForwardEntry
	direct           bool
	cfg              *containerConfig
}

//var maddress []*testRun

var tls = false
var mtls = false
var lport = "localhost:16442-16445"
var password = "admin"

const (
	tb = int64(1099511627776)
	gb = int64(1073741824)
)

func runMatix(t *testing.T, testType string, tests []string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, name := range tests {
		var c *testRun
		var err error
		t.Run(fmt.Sprintf("%s/%s", testType, name), func(t *testing.T) {
			switch n := name; n {
			case "AZURE":
				cfg := &containerConfig{containername: "azure-6442", hostPort: "6442"}
				//c.cfg = cfg
				c, err = CreateAzureSetup(ctx, cfg)
				c.name = n
				c.cloudVol = true
				assert.Nil(t, err)
			case "BLOCK":
				cfg := &containerConfig{containername: "block-6442", hostPort: "6442"}
				//c.cfg = cfg
				c, err = CreateBlockSetup(ctx, cfg)
				c.name = n
				c.cloudVol = false
				assert.Nil(t, err)
			case "S3":
				cfg := &containerConfig{containername: "s3-6442", hostPort: "6442"}
				//c.cfg = cfg
				c, err = CreateS3Setup(ctx, cfg)
				c.name = n
				c.cloudVol = true
				assert.Nil(t, err)
			}
			switch z := testType; z {
			case "PROXYDEDUPE":
				c.clientsidededupe = true
			case "DIRECTDEDUPE":
				c.clientsidededupe = true
				c.direct = true
			case "PROXY":
				c.clientsidededupe = false
				c.direct = false
				c.volume = -1
			}
			trs := []*testRun{c}
			if !c.direct {
				startProxyVolume(trs)
			}
			testNewProxyConnection(t, c)
			t.Run("testConnection", func(t *testing.T) {
				assert.NotNil(t, c.connection)
			})
			t.Run("testChown", func(t *testing.T) {
				testChow(t, c)

			})
			t.Run("testMkDir", func(t *testing.T) {
				testMkDir(t, c)
			})
			t.Run("testListDir", func(t *testing.T) {
				testListDir(t, c)
			})
			t.Run("testMkNod", func(t *testing.T) {
				testMkNod(t, c)
			})
			t.Run("testMkDirAll", func(t *testing.T) {
				testMkDirAll(t, c)
			})
			t.Run("testCleanStore", func(t *testing.T) {
				testCleanStore(t, c)
			})
			t.Run("testStatFS", func(t *testing.T) {
				testStatFS(t, c)
			})
			t.Run("testRename", func(t *testing.T) {
				testRename(t, c)
			})
			t.Run("testCopyFile", func(t *testing.T) {
				testCopyFile(t, c)
			})
			t.Run("testEvents", func(t *testing.T) {
				testEvents(t, c)
			})
			t.Run("testXAttrs", func(t *testing.T) {
				testXAttrs(t, c)
			})
			t.Run("testSetUtime", func(t *testing.T) {
				testSetUtime(t, c)
			})
			t.Run("testTuncate", func(t *testing.T) {
				testTuncate(t, c)
			})
			t.Run("testWriteLargeBlock", func(t *testing.T) {
				testWriteLargeBlock(t, c)
			})
			t.Run("testCopyExtent", func(t *testing.T) {
				testCopyExtent(t, c)
			})
			t.Run("testSymLink", func(t *testing.T) {
				testSymLink(t, c)
			})
			t.Run("testSync", func(t *testing.T) {
				testSync(t, c)
			})
			t.Run("testMaxAge", func(t *testing.T) {
				testMaxAge(t, c)
			})
			t.Run("testInfo", func(t *testing.T) {
				testInfo(t, c)
			})
			t.Run("testGCSchedule", func(t *testing.T) {
				testGCSchedule(t, c)
			})
			t.Run("testUpload", func(t *testing.T) {
				testUpload(t, c)
			})
			t.Run("testShutdown", func(t *testing.T) {
				testShutdown(t, c)
			})
			t.Run("testSetVolumeSize", func(t *testing.T) {
				testSetVolumeSize(t, c)
			})
			if c.cloudVol {
				t.Run("testSetRWSpeed", func(t *testing.T) {
					testSetRWSpeed(t, c)
				})
				t.Run("testCache", func(t *testing.T) {
					testCache(t, c)
				})
				t.Run("testCloudSync", func(t *testing.T) {
					testCloudSync(t, c)
				})
			}
			c.connection.CloseConnection(ctx)
			if !c.direct {
				paip.StopServer()
			}

			StopAndRemoveContainer(ctx, c.cfg.containername)
		})

	}
}

func BenchmarkWrites(b *testing.B) {
	type ut struct {
		name string
		pu   int
	}
	type st struct {
		name string
		sz   int64
	}

	tests := []string{"AZURE", "S3", "BLOCK", "ENCRYPTBLOCK"}
	testTypes := []string{"PROXY", "PROXYDEDUPE", "DIRECTDEDUPE"}
	uTest := []ut{{name: "0PercentUnique", pu: 0}, {name: "50PercentUnique", pu: 50}, {name: "100PercentUnique", pu: 100}}
	sTest := []st{{name: "1GB", sz: int64(1) * gb}, {name: "10GB", sz: int64(10) * gb},
		{name: "100GB", sz: int64(100) * gb}, {name: "900GB", sz: int64(900) * gb}}
	tTest := []int{1, 2, 4, 8}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, testType := range testTypes {
		for _, name := range tests {
			var c *testRun
			var err error
			b.Run(fmt.Sprintf("%s/%s", testType, name), func(b *testing.B) {
				//remove old data
				cmd := exec.Command("sudo", "rm", "-rf", "/opt/sdfs/volumes")

				var out bytes.Buffer
				cmd.Stdout = &out

				err = cmd.Run()

				if err != nil {
					log.Fatal(err)
				}
				switch n := name; n {
				case "AZURE":
					cfg := &containerConfig{containername: "azure-6442", hostPort: "6442", mountstorage: true, cpu: 4}
					//c.cfg = cfg
					c, err = CreateAzureSetup(ctx, cfg)
					if err != nil {
						b.Logf("error creating container %v", err)
					}
					c.name = n
					c.cloudVol = true
				case "BLOCK":
					cfg := &containerConfig{containername: "block-6442", hostPort: "6442", mountstorage: true, memory: 16 * gb}
					//c.cfg = cfg
					c, err = CreateBlockSetup(ctx, cfg)
					if err != nil {
						b.Logf("error creating container %v", err)
					}
					c.name = n
					c.cloudVol = false
				case "ENCRYPTBLOCK":
					cfg := &containerConfig{attachProfiler: true, containername: "eblock-6442", hostPort: "6442", mountstorage: true, memory: 16 * gb, encrypt: true}
					//c.cfg = cfg
					c, err = CreateBlockSetup(ctx, cfg)
					if err != nil {
						b.Logf("error creating container %v", err)
					}
					c.name = n
					c.cloudVol = false
				case "S3":
					cfg := &containerConfig{containername: "s3-6442", hostPort: "6442", mountstorage: true, cpu: 4}
					//c.cfg = cfg
					c, err = CreateS3Setup(ctx, cfg)
					if err != nil {
						b.Logf("error creating container %v", err)
					}
					c.name = n
					c.cloudVol = true
				}
				switch z := testType; z {
				case "PROXYDEDUPE":
					c.clientsidededupe = true
				case "DIRECTDEDUPE":
					c.clientsidededupe = true
					c.direct = true
				case "PROXY":
					c.clientsidededupe = false
					c.direct = false
					c.volume = -1
				}
				trs := []*testRun{c}
				if !c.direct {
					startProxyVolume(trs)
				}
				c.connection = benchmarkConnect(b, c)
				cc := 0
				for c.connection == nil {
					time.Sleep(15 * time.Second)
					c.connection = benchmarkConnect(b, c)
					cc++
					if cc > 5 {
						b.Errorf("unable to connect to volume")
					}
				}
				for _, pu := range uTest {
					b.Run(pu.name, func(b *testing.B) {
						for _, st := range sTest {
							b.Run(st.name, func(b *testing.B) {
								for _, tt := range tTest {
									b.Run(fmt.Sprintf("WriteThreads%d", tt), func(b *testing.B) {
										//parallel write
										b.Run("parallelBenchmarkWrite32", func(b *testing.B) {
											parallelBenchmarkWrite(b, c, 32, st.sz, pu.pu, tt)
										})
										b.Run("parallelBenchmarkWrite64", func(b *testing.B) {
											parallelBenchmarkWrite(b, c, 64, st.sz, pu.pu, tt)
										})
										b.Run("parallelBenchmarkWrite128", func(b *testing.B) {
											parallelBenchmarkWrite(b, c, 128, st.sz, pu.pu, tt)
										})
										b.Run("parallelBenchmarkWrite256", func(b *testing.B) {
											parallelBenchmarkWrite(b, c, 256, st.sz, pu.pu, tt)
										})
										b.Run("parallelBenchmarkWrite512", func(b *testing.B) {
											parallelBenchmarkWrite(b, c, 512, st.sz, pu.pu, tt)
										})
										b.Run("parallelBenchmarkWrite1024", func(b *testing.B) {
											parallelBenchmarkWrite(b, c, 1024, st.sz, pu.pu, tt)
										})
										b.Run("parallelBenchmarkWrite2048", func(b *testing.B) {
											parallelBenchmarkWrite(b, c, 2048, st.sz, pu.pu, tt)
										})
									})
									b.Run(fmt.Sprintf("ReadThreads%d", tt), func(b *testing.B) {
										//parallel read
										b.Run("parallelBenchmarkRead32", func(b *testing.B) {
											parallelBenchmarkRead(b, c, 32, st.sz, pu.pu, tt)
										})
										b.Run("parallelBenchmarkRead64", func(b *testing.B) {
											parallelBenchmarkRead(b, c, 64, st.sz, pu.pu, tt)
										})
										b.Run("parallelBenchmarkRead128", func(b *testing.B) {
											parallelBenchmarkRead(b, c, 128, st.sz, pu.pu, tt)
										})
										b.Run("parallelBenchmarkRead256", func(b *testing.B) {
											parallelBenchmarkRead(b, c, 256, st.sz, pu.pu, tt)
										})
										b.Run("parallelBenchmarkRead512", func(b *testing.B) {
											parallelBenchmarkRead(b, c, 512, st.sz, pu.pu, tt)
										})
										b.Run("parallelBenchmarkRead1024", func(b *testing.B) {
											parallelBenchmarkRead(b, c, 1024, st.sz, pu.pu, tt)
										})
										b.Run("parallelBenchmarkRead2048", func(b *testing.B) {
											parallelBenchmarkRead(b, c, 2048, st.sz, pu.pu, tt)
										})
									})
								}
							})
						}
					})
				}
				c.connection.CloseConnection(ctx)
				if !c.direct {
					paip.StopServer()
				}

				StopAndRemoveContainer(ctx, c.cfg.containername)
			})
		}
	}

}

func parallelBenchmarkWrite(b *testing.B, c *testRun, blockSize int, fileSize int64, percentUnique, threads int) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type th struct {
		fn         string
		fh         int64
		bt         []byte
		offset     int64
		connection *api.SdfsConnection
	}
	inv := 100 - percentUnique
	blockSz := 1024 * blockSize
	for i := 0; i < b.N; i++ {
		var ths []*th
		for z := 0; z < threads; z++ {
			fn := string(randBytesMaskImpr(16))
			connection := benchmarkConnect(b, c)
			connection.MkNod(ctx, fn, 511, 0)
			connection.GetAttr(ctx, fn)
			fh, err := connection.Open(ctx, fn, 0)
			if err != nil {
				b.Errorf("error getting filehandle %v", err)
				return
			}
			offset := int64(0)

			bt := randBytesMaskImpr(blockSz)
			thh := &th{fn: fn, fh: fh, offset: offset, bt: bt, connection: connection}
			ths = append(ths, thh)
		}
		b.StartTimer()
		wg := &sync.WaitGroup{}
		wg.Add(threads)
		for z := 0; z < threads; z++ {
			thh := ths[z]
			go func() {

				ct := 0
				for thh.offset < fileSize {
					err := thh.connection.Write(ctx, thh.fh, thh.bt, thh.offset, int32(len(thh.bt)))
					if err != nil {
						b.Errorf("error writing data at %d  %v", thh.offset, err)
						return
					}
					thh.offset += int64(len(thh.bt))
					ct++
					if ct > inv {
						thh.bt = nil
						thh.bt = randBytesMaskImpr(blockSz)
					}
					if ct == 100 {
						ct = 0
					}
				}
				thh.connection.Release(ctx, thh.fh)
				thh.connection.CloseConnection(ctx)
				wg.Done()
			}()
		}
		wg.Wait()
		b.StopTimer()
		var sz int64
		for z := 0; z < threads; z++ {
			sz += ths[z].offset
			c.connection.Unlink(ctx, ths[z].fn)
		}
		b.SetBytes(sz)

	}

}

func parallelBenchmarkRead(b *testing.B, c *testRun, blockSize int, fileSize int64, percentUnique, threads int) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type th struct {
		fn         string
		fh         int64
		bt         []byte
		offset     int64
		connection *api.SdfsConnection
		sum        []byte
	}
	inv := 100 - percentUnique
	blockSz := 1024 * blockSize
	for i := 0; i < b.N; i++ {
		var ths []*th
		for z := 0; z < threads; z++ {
			fn := string(randBytesMaskImpr(16))
			connection := benchmarkConnect(b, c)
			connection.MkNod(ctx, fn, 511, 0)
			connection.GetAttr(ctx, fn)
			fh, err := connection.Open(ctx, fn, 0)
			if err != nil {
				b.Errorf("error getting filehandle %v", err)
				return
			}
			offset := int64(0)

			bt := randBytesMaskImpr(blockSz)
			thh := &th{fn: fn, fh: fh, offset: offset, bt: bt, connection: connection}
			ths = append(ths, thh)
		}
		wg := &sync.WaitGroup{}
		wg.Add(threads)

		for z := 0; z < threads; z++ {
			thh := ths[z]
			go func() {
				h, err := blake2b.New(32, make([]byte, 0))
				if err != nil {
					b.Errorf("error getting hash %v", err)
					return
				}
				ct := 0
				for thh.offset < fileSize {
					err := thh.connection.Write(ctx, thh.fh, thh.bt, thh.offset, int32(len(thh.bt)))
					if err != nil {
						b.Errorf("error writing data at %d  %v", thh.offset, err)
						return
					}
					h.Write(thh.bt)
					thh.offset += int64(len(thh.bt))
					ct++
					if ct > inv {
						thh.bt = nil
						thh.bt = randBytesMaskImpr(blockSz)
					}
					if ct == 100 {
						ct = 0
					}
				}
				thh.sum = h.Sum(nil)
				thh.connection.Release(ctx, thh.fh)
				wg.Done()
			}()
		}
		wg.Wait()

		var sz int64
		wg = &sync.WaitGroup{}
		wg.Add(threads)
		b.StartTimer()
		for z := 0; z < threads; z++ {
			thh := ths[z]
			go func() {
				stat, err := thh.connection.GetAttr(ctx, thh.fn)
				if err != nil {
					b.Errorf("error getting attr %v", err)
					return
				}
				fh, err := thh.connection.Open(ctx, thh.fn, 0)
				if err != nil {
					b.Errorf("error opening file %v", err)
					return
				}
				maxoffset := stat.Size
				offset := int64(0)
				bt := make([]byte, 0)
				h, err := blake2b.New(32, bt)
				if err != nil {
					b.Errorf("error getting hash %v", err)
					return
				}
				readSize := int32(blockSz)
				for offset < maxoffset {
					if int64(readSize) > int64(maxoffset-offset) {
						readSize = int32(maxoffset - offset)
					}
					bt, err = thh.connection.Read(ctx, fh, offset, int32(readSize))
					if err != nil {
						b.Errorf("error reading data %v", err)
						return
					}
					h.Write(bt)
					offset += int64(len(bt))
					bt = nil
				}
				if !reflect.DeepEqual(h.Sum(nil), thh.sum) {
					b.Errorf("read and write equal: %t\n", reflect.DeepEqual(h.Sum(nil), thh.sum))
				}
				thh.connection.Release(ctx, fh)
				thh.connection.CloseConnection(ctx)
				wg.Done()
			}()
		}
		wg.Wait()
		b.StopTimer()
		for z := 0; z < threads; z++ {
			sz += ths[z].offset
			c.connection.Unlink(ctx, ths[z].fn)
		}
		b.SetBytes(sz)
	}

}

func TestMatrix(t *testing.T) {
	tests := []string{"AZURE", "S3", "BLOCK"}
	runMatix(t, "PROXY", tests)
	runMatix(t, "PROXYDEDUPE", tests)
	runMatix(t, "DIRECTDEDUPE", tests)
}

func testNewProxyConnection(t *testing.T, c *testRun) {
	t.Logf("Creating connection for %d\n", c.volume)
	c.connection = dconnect(t, c)
	ct := 0
	for c.connection == nil {
		time.Sleep(15 * time.Second)
		c.connection = dconnect(t, c)
		ct++
		if ct > 5 {
			break
		}

	}
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
	tMb := int64(1024 * 1024 * 100)
	fMb := 1024 * 5
	fn, _ := makeLargeBlockFile(ctx, t, c, "", tMb, fMb)
	info, _ := c.connection.Stat(ctx, fn)
	t.Logf("afd %d vfd %d", info.IoMonitor.ActualBytesWritten, info.IoMonitor.VirtualBytesWritten)
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

	sz := info.Entries
	nfn := string(randBytesMaskImpr(16))
	time.Sleep(15 * time.Second)

	_, err = c.connection.Download(ctx, _nfn, nfn, 1024)
	defer os.Remove(nfn)
	assert.Nil(t, err)
	_, err = c.connection.Upload(ctx, nfn, nfn, 1024)
	assert.Nil(t, err)
	os.Remove(_nfn)
	time.Sleep(15 * time.Second)
	info, err = c.connection.DSEInfo(ctx)
	assert.Nil(t, err)
	nsz := info.Entries
	t.Logf("sz = %d nsz =%d", sz, nsz)
	assert.Less(t, sz, nsz)
	err = c.connection.DeleteFile(ctx, nfn)
	assert.Nil(t, err)
	err = c.connection.DeleteFile(ctx, _nfn)
	assert.Nil(t, err)
	time.Sleep(15 * time.Second)
	/*
		time.Sleep(15 * time.Second)
		c.connection.CleanStore(ctx, true, true)
		tm := time.Duration(60 * int(time.Second))
		time.Sleep(tm)
	*/
	info, err = c.connection.DSEInfo(ctx)
	assert.Nil(t, err)
	fnsz := info.Entries
	t.Logf("sz = %d nsz =%d, fnsz=%d", sz, nsz, fnsz)
	assert.Greater(t, sz, fnsz)
	_nfn, hs := makeFile(ctx, t, c, "", fsz)
	nfn = string(randBytesMaskImpr(16))
	time.Sleep(10 * time.Second)
	c.connection.CopyFile(ctx, _nfn, nfn, false)
	c.connection.DeleteFile(ctx, _nfn)
	time.Sleep(15 * time.Second)
	c.connection.CleanStore(ctx, true, true)
	tm := time.Duration(60 * int(time.Second))
	time.Sleep(tm)
	info, err = c.connection.DSEInfo(ctx)
	assert.Nil(t, err)
	fnsz = info.Entries
	t.Logf("sz = %d, fnsz=%d", sz, fnsz)
	nhs, err := readFile(ctx, t, c, nfn, true)
	assert.Nil(t, err)
	assert.Equal(t, hs, nhs)
	c.connection.DeleteFile(ctx, _nfn)
	/*
		time.Sleep(10 * time.Second)
		c.connection.CleanStore(ctx, true, true)
		tm = time.Duration(60 * int(time.Second))
		time.Sleep(tm)
	*/
	info, err = c.connection.DSEInfo(ctx)
	assert.Nil(t, err)
	fnsz = info.Entries
	t.Logf("sz = %d, fnsz=%d", sz, fnsz)
	_nfn, _ = makeFile(ctx, t, c, "", 1024*1024)
	nfn = string(randBytesMaskImpr(16))
	os.Remove(nfn)
	time.Sleep(10 * time.Second)
	_, err = c.connection.Download(ctx, _nfn, nfn, 1024)
	assert.Nil(t, err)
	sz = info.Entries
	for i := 0; i < 10; i++ {
		_, err = c.connection.Upload(ctx, nfn, fmt.Sprintf("file%d", i), 1024)
		if err != nil {
			t.Logf("upload error %v", err)
		}
		info, err := c.connection.Stat(ctx, fmt.Sprintf("file%d", i))
		assert.GreaterOrEqual(t, info.IoMonitor.ActualBytesWritten, int64(0))
		assert.Nil(t, err)
		time.Sleep(15 * time.Second)
	}
	time.Sleep(15 * time.Second)

	/*
		c.connection.DeleteFile(ctx, _nfn)
		time.Sleep(10 * time.Second)
		c.connection.CleanStore(ctx, true, true)
		tm = time.Duration(60 * int(time.Second))
		time.Sleep(tm)
	*/
	info, _ = c.connection.DSEInfo(ctx)
	nsz = info.Entries
	t.Logf("sz = %d, nsz=%d", sz, nsz)
	assert.Less(t, sz, nsz)
	for i := 0; i < 10; i++ {
		err = c.connection.DeleteFile(ctx, fmt.Sprintf("file%d", i))
		assert.Nil(t, err)
	}
	/*

	 */
	info, _ = c.connection.DSEInfo(ctx)
	sz = info.Entries
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
	cfg := c.cfg
	cfg.containername = fmt.Sprintf("second-%s", c.cfg.containername)
	cfg.containerPort = "6442"
	cfg.hostPort = fmt.Sprintf("2%s", c.cfg.hostPort)
	durl := fmt.Sprintf("sdfs://localhost:%s", cfg.hostPort)
	tr := &testRun{url: durl, name: fmt.Sprintf("second-%s", c.name), cfg: cfg, cloudVol: true, clientsidededupe: c.clientsidededupe}
	fe := paip.ForwardEntry{
		Address:       tr.url,
		Dedupe:        false,
		DedupeThreads: 1,
		DedupeBuffer:  4,
	}
	portR := &paip.PortRedirectors{}
	maddress := [2]*testRun{c, tr}

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
	dh, err := readFile(ctx, t, tr, fn, false)
	assert.Nil(t, err)
	assert.Equal(t, dh, sh)
	assert.Equal(t, fi.Mode, nfi.Mode)
	err = c.connection.DeleteFile(ctx, fn)
	assert.Nil(t, err)
	time.Sleep(15 * time.Second)
	c.connection.CleanStore(ctx, true, true)
	time.Sleep(65 * time.Second)
	dh, err = readFile(ctx, t, tr, fn, false)
	assert.Nil(t, err)
	assert.Equal(t, dh, sh)
	fn, sh = makeGenericFile(ctx, t, c.connection, "", 1024)
	_, err = c.connection.Stat(ctx, fn)
	assert.Nil(t, err)
	_, err = tr.connection.GetCloudFile(ctx, fn, fmt.Sprintf("nf%s", fn), true, true)
	assert.Nil(t, err)
	time.Sleep(35 * time.Second)
	dh, err = readFile(ctx, t, tr, fn, false)
	assert.Nil(t, err)
	assert.Equal(t, dh, sh)
	assert.Equal(t, fi.Mode, nfi.Mode)
	err = c.connection.DeleteFile(ctx, fn)
	assert.Nil(t, err)
	time.Sleep(15 * time.Second)
	c.connection.CleanStore(ctx, true, true)
	time.Sleep(65 * time.Second)
	dh, err = readFile(ctx, t, tr, fn, false)
	assert.Nil(t, err)
	assert.Equal(t, dh, sh)
	StopAndRemoveContainer(ctx, tr.cfg.containername)
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var port = 2
	var testRuns []*testRun
	for i := 2; i < 4; i++ {
		cfg := &containerConfig{containername: fmt.Sprintf("block-644%d", port), hostPort: fmt.Sprintf("644%d", port)}
		tst, err := CreateBlockSetup(ctx, cfg)
		assert.Nil(t, err)
		port++
		testRuns = append(testRuns, tst)
	}
	startProxyVolume(testRuns)
	var volumeIds []int64
	for _, c := range testRuns {
		volumeIds = append(volumeIds, c.volume)
	}

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
	paip.StopServer()
	for _, c := range testRuns {
		StopAndRemoveContainer(ctx, c.cfg.containername)
	}
}

func TestReloadProxyVolume(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var testRuns []*testRun
	for i := 2; i < 5; i++ {
		cfg := &containerConfig{containername: fmt.Sprintf("block-644%d", i), hostPort: fmt.Sprintf("644%d", i)}
		tst, err := CreateBlockSetup(ctx, cfg)
		assert.Nil(t, err)
		testRuns = append(testRuns, tst)
	}
	startProxyVolume(testRuns[1:])
	var volumeIds []int64
	for _, c := range testRuns[1:] {
		t.Logf("tr serial = %d", c.volume)
		volumeIds = append(volumeIds, c.volume)
	}
	connection := connect(t, false, -1)
	vis, err := connection.GetProxyVolumes(ctx)
	assert.Nil(t, err)
	var vids []int64
	for _, vi := range vis.VolumeInfoResponse {
		vids = append(vids, vi.SerialNumber)
		t.Logf("serial = %d", vi.SerialNumber)
	}
	assert.ElementsMatch(t, vids, volumeIds)
	//Get the volume id for the first volume
	cc, err := api.NewConnection(testRuns[0].url, false, true, -1, 0, 0)
	retrys := 0
	for err != nil {
		log.Printf("retries = %d", retrys)
		time.Sleep(20 * time.Second)
		cc, err = api.NewConnection(testRuns[0].url, false, true, -1, 0, 0)
		if retrys > 10 {
			fmt.Printf("SDFS Server connection timed out %s\n", testRuns[0].url)
			os.Exit(-1)
		} else {
			retrys++
		}
	}
	if err != nil {
		fmt.Printf("Unable to create connection %v", err)
	}
	log.Printf("connected to volume = %d for %s", cc.Volumeid, testRuns[0].cfg.containername)
	testRuns[0].volume = cc.Volumeid
	//Test Add a Volume
	portR := &paip.PortRedirectors{}
	for i := 2; i < 5; i++ {
		fe := paip.ForwardEntry{Address: fmt.Sprintf("sdfs://localhost:644%d", i)}
		portR.ForwardEntrys = append(portR.ForwardEntrys, fe)
	}
	b, err := json.Marshal(*portR)
	assert.Nil(t, err)
	err = ioutil.WriteFile("testpf.json", b, 0644)
	assert.Nil(t, err)
	_, err = connection.ReloadProxyConfig(ctx)
	assert.Nil(t, err)
	vis, err = connection.GetProxyVolumes(ctx)
	assert.Nil(t, err)
	vids = make([]int64, 0)
	volumeIds = make([]int64, 0)

	for _, c := range testRuns {
		volumeIds = append(volumeIds, c.volume)
		t.Logf("tr serial = %d", c.volume)
	}
	for _, vi := range vis.VolumeInfoResponse {
		vids = append(vids, vi.SerialNumber)
		t.Logf("serial = %d", vi.SerialNumber)
	}
	assert.ElementsMatch(t, vids, volumeIds)
	//Test Remove a volume
	portR = &paip.PortRedirectors{}
	for i := 2; i < 4; i++ {
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
	assert.Nil(t, err)
	vids = make([]int64, 0)
	volumeIds = make([]int64, 0)

	for _, c := range testRuns[:len(testRuns)-1] {
		volumeIds = append(volumeIds, c.volume)
		t.Logf("tr serial = %d", c.volume)
	}
	for _, vi := range vis.VolumeInfoResponse {
		vids = append(vids, vi.SerialNumber)
		t.Logf("serial = %d", vi.SerialNumber)
	}
	assert.ElementsMatch(t, vids, volumeIds)
	//Shut everything down
	paip.StopServer()
	for _, c := range testRuns {
		StopAndRemoveContainer(ctx, c.cfg.containername)
	}

}

func startProxyVolume(tr []*testRun) {
	tls = true
	api.DisableTrust = true
	paip.ServerCACert = "out/signer_key.crt"
	paip.ServerCert = "out/tls_key.crt"
	paip.ServerKey = "out/tls_key.key"
	paip.ServerTls = true

	cmp := make(map[int64]*grpc.ClientConn)
	dd := make(map[int64]paip.ForwardEntry)
	portR := &paip.PortRedirectors{}
	for _, m := range tr {
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
		log.Printf("connected to volume = %d for %s", connection.Volumeid, m.cfg.containername)
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

}

func TestMain(m *testing.M) {
	go func() {
		log.Println(http.ListenAndServe(":8081", nil))
	}()
	rand.Seed(time.Now().UTC().UnixNano())
	code := m.Run()
	fmt.Printf("Testing Return code is %d\n", code)
	os.Exit(code)
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
	if volumeid != -1 {
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
	}
	return connection
}

func benchmarkConnect(b *testing.B, c *testRun) *api.SdfsConnection {

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
	b.Logf("Connecting to %s dedupe is %v", address, c.clientsidededupe)

	connection, err := api.NewConnection(address, c.clientsidededupe, true, vid, 40000, 60)
	if err != nil {
		b.Errorf("Unable to connect to %s error: %v\n", address, err)
		return nil
	}
	b.Logf("Connection state %s", connection.Clnt.GetState())
	if vid != -1 {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		volinfo, err := connection.GetVolumeInfo(ctx)
		if err != nil {

			b.Errorf("Unable to get volume info for %s error: %v\n", address, err)
			return nil
		}
		if volinfo.SerialNumber != c.volume {
			b.Errorf("Volume serial numbers don't match expected %d got %d\n", c.volume, volinfo.SerialNumber)
			return nil
		}
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
	if vid != -1 {
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
	}
	return connection
}
