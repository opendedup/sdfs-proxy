package test

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"os/exec"
	"reflect"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"

	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
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

type ContainerConfig struct {
	cpu                                                   int64
	memory                                                int64
	encrypt                                               bool
	mountstorage                                          bool
	id, imagename, containername, hostPort, containerPort string
	inputEnv, cmd                                         []string
	copyFile                                              bool
	attachProfiler                                        bool
	disableMinio                                          bool
}

type TestRun struct {
	Name             string
	Volume           int64
	Clientsidededupe bool
	Url              string
	Connection       *api.SdfsConnection
	CloudVol         bool
	S3               bool
	Fe               *paip.ForwardEntry
	Direct           bool
	Cfg              *ContainerConfig
}

//var maddress []*TestRun

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
		var c *TestRun
		var err error
		t.Run(fmt.Sprintf("%s/%s", testType, name), func(t *testing.T) {
			switch n := name; n {
			case "AZURE":
				cfg := &ContainerConfig{containername: "azure-6442", hostPort: "6442", encrypt: true}
				//c.Cfg = cfg
				c, err = CreateAzureSetup(ctx, cfg)
				c.Name = n
				c.CloudVol = true
				assert.Nil(t, err)
			case "BLOCK":
				cfg := &ContainerConfig{containername: "block-6442", hostPort: "6442"}
				//c.Cfg = cfg
				c, err = CreateBlockSetup(ctx, cfg)
				c.Name = n
				c.CloudVol = false
				assert.Nil(t, err)
			case "S3":
				cfg := &ContainerConfig{containername: "s3-6442", hostPort: "6442"}
				//c.Cfg = cfg
				c, err = CreateS3Setup(ctx, cfg)
				if err != nil {
					t.Logf("Error %v", err)
				}
				c.Name = n
				c.CloudVol = true
				c.S3 = true
				assert.Nil(t, err)
			}
			switch z := testType; z {
			case "PROXYDEDUPE":
				c.Clientsidededupe = true
			case "DIRECTDEDUPE":
				c.Clientsidededupe = true
				c.Direct = true
			case "PROXY":
				c.Clientsidededupe = false
				c.Direct = false
				c.Volume = -1
			case "NATIVE":
				c.Clientsidededupe = false
				c.Direct = true
			}
			trs := []*TestRun{c}
			if !c.Direct {
				StartProxyVolume(trs)
			}
			testNewProxyConnection(t, c)
			t.Run("testConnection", func(t *testing.T) {
				assert.NotNil(t, c.Connection)
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
			t.Run("testCleanStoreEvent", func(t *testing.T) {
				testCleanStoreEvent(t, c)
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
			t.Run("testCrashRestart", func(t *testing.T) {
				testCrashRestart(t, c)
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
			t.Run("testCompression", func(t *testing.T) {
				testCompression(t, c)
			})
			t.Run("testReplicateFile", func(t *testing.T) {
				testReplicateFile(t, c)
			})
			t.Run("testReplicateFileLarge", func(t *testing.T) {
				testReplicateFileLarge(t, c)
			})
			t.Run("testReplicatePause", func(t *testing.T) {
				testReplicatePause(t, c)
			})
			t.Run("testReplicateCanceled", func(t *testing.T) {
				testReplicateCanceled(t, c)
			})
			t.Run("testReplicateCanceledErrors", func(t *testing.T) {
				testReplicateCanceledErrors(t, c)
			})

			t.Run("testReplicateSyncFile", func(t *testing.T) {
				testReplicateSyncFile(t, c)
			})
			t.Run("testReplicateSyncRestart", func(t *testing.T) {
				testReplicateSyncRestart(t, c)
			})
			t.Run("testReplicateCrashRestart", func(t *testing.T) {
				testReplicateCrashRestart(t, c)
			})
			t.Run("testReplicateCrashSourceRestart", func(t *testing.T) {
				testReplicateCrashSourceRestart(t, c)
			})
			t.Run("testReplicatePauseCancelDelete", func(t *testing.T) {
				testReplicatePauseCancelDelete(t, c)
			})
			t.Run("testReplicateFileOffset", func(t *testing.T) {
				testReplicateFileOffset(t, c)
			})
			t.Run("testReplicateFileAfterBadURL", func(t *testing.T) {
				testReplicateFileAfterBadURL(t, c)
			})

			t.Run("testReplicatePauseCancelCrashRestart", func(t *testing.T) {
				testReplicatePauseCancelCrashRestart(t, c)
			})
			t.Run("testReplicateSyncAddRemoveOnce", func(t *testing.T) {
				testReplicateSyncAddRemove(t, c)
			})
			t.Run("testReplicateSyncAddRemoveAdd", func(t *testing.T) {
				testReplicateSyncAddRemoveAdd(t, c)
			})
			t.Run("testReplicateSyncAddRemoveNewAdd", func(t *testing.T) {
				testReplicateSyncAddRemoveNewAdd(t, c)
			})
			t.Run("testReplicateSyncSrcRestart", func(t *testing.T) {
				testReplicateSyncSrcRestart(t, c)
			})
			t.Run("testReplicateSyncUpload", func(t *testing.T) {
				testReplicateSyncUpload(t, c)
			})
			t.Run("testReplicateSyncCmdAddRemove", func(t *testing.T) {
				testReplicateSyncCmdAddRemove(t, c)
			})
<<<<<<< HEAD
			t.Run("testDiskFull", func(t *testing.T) {
				testDiskFull(t, c)
			})
=======
>>>>>>> 8fa81aa124fa6120af5bf41ca81e7f7320781b2c
			if c.CloudVol {
				t.Run("testSetRWSpeed", func(t *testing.T) {
					testSetRWSpeed(t, c)
				})
				t.Run("testCache", func(t *testing.T) {
					testCache(t, c)
				})
				t.Run("testCloudSync", func(t *testing.T) {
					testCloudSync(t, c)
				})
				t.Run("testReconcileCloudMetadata", func(t *testing.T) {
					testReconcileCloudMetadata(t, c)
				})
				t.Run("testCloudAutoDownload", func(t *testing.T) {
					testCloudAutoDownload(t, c)
				})
				t.Run("testCloudRecover", func(t *testing.T) {
					testCloudRecover(t, c)
				})
			}
			c.Connection.CloseConnection(ctx)
			if !c.Direct {
				paip.StopServer()
			}

			StopAndRemoveContainer(ctx, c.Cfg.containername)
			if name == "S3" {
				StopAndRemoveContainer(ctx, "minio")
			}
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

	tests := []string{"AZURE", "S3", "BLOCK", "EB"}
	testTypes := []string{"PROXY", "PROXYDEDUPE", "DIRECTDEDUPE", "NATIVE"}
	uTest := []ut{{name: "0PercentUnique", pu: 0}, {name: "50PercentUnique", pu: 50}, {name: "100PercentUnique", pu: 100}}
	sTest := []st{{name: "1GB", sz: int64(1) * gb}, {name: "4GB", sz: int64(4) * gb}, {name: "10GB", sz: int64(10) * gb},
		{name: "100GB", sz: int64(100) * gb}, {name: "900GB", sz: int64(900) * gb}}
	tTest := []int{1, 2, 5, 10, 20}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, testType := range testTypes {
		for _, name := range tests {
			var c *TestRun
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
					cfg := &ContainerConfig{containername: "azure-6442", hostPort: "6442", mountstorage: true}
					//c.Cfg = cfg
					c, err = CreateAzureSetup(ctx, cfg)
					if err != nil {
						b.Errorf("error creating container %v", err)
					}
					c.Name = n
					c.CloudVol = true
				case "BLOCK":
					cfg := &ContainerConfig{containername: "block-6442", hostPort: "6442", mountstorage: true}
					//c.Cfg = cfg
					c, err = CreateBlockSetup(ctx, cfg)
					if err != nil {
						b.Errorf("error creating container %v", err)
					}
					c.Name = n
					c.CloudVol = false
				case "EB":
					cfg := &ContainerConfig{attachProfiler: true, containername: "eblock-6442", hostPort: "6442", mountstorage: true, encrypt: true}
					//c.Cfg = cfg
					c, err = CreateBlockSetup(ctx, cfg)
					if err != nil {
						b.Errorf("error creating container %v", err)
					}
					c.Name = n
					c.CloudVol = false
				case "S3":
					cfg := &ContainerConfig{containername: "s3-6442", hostPort: "6442", mountstorage: true}
					//c.Cfg = cfg
					c, err = CreateS3Setup(ctx, cfg)
					if err != nil {
						b.Errorf("error creating container %v", err)
					}
					c.S3 = true
					c.Name = n
					c.CloudVol = true
				}
				switch z := testType; z {
				case "PROXYDEDUPE":
					c.Clientsidededupe = true
				case "DIRECTDEDUPE":
					c.Clientsidededupe = true
					c.Direct = true
				case "PROXY":
					c.Clientsidededupe = false
					c.Direct = false
					c.Volume = -1
				case "NATIVE":
					c.Clientsidededupe = false
					c.Direct = true
				}
				trs := []*TestRun{c}
				if !c.Direct {
					StartProxyVolume(trs)
				}
				c.Connection = BConnect(b, c)
				cc := 0
				for c.Connection == nil {
					time.Sleep(15 * time.Second)
					c.Connection = BConnect(b, c)
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
									b.Run(fmt.Sprintf("%d-WriteThreads", tt), func(b *testing.B) {
										//parallel write
										b.Run("parallelBenchmarkUpload32", func(b *testing.B) {
											parallelBenchmarkUpload(b, c, 32, st.sz, pu.pu, tt)
										})
										b.Run("parallelBenchmarkUpload64", func(b *testing.B) {
											parallelBenchmarkUpload(b, c, 64, st.sz, pu.pu, tt)
										})
										b.Run("parallelBenchmarkUpload128", func(b *testing.B) {
											parallelBenchmarkUpload(b, c, 128, st.sz, pu.pu, tt)
										})
										b.Run("parallelBenchmarkUpload256", func(b *testing.B) {
											parallelBenchmarkUpload(b, c, 256, st.sz, pu.pu, tt)
										})
										b.Run("parallelBenchmarkUpload512", func(b *testing.B) {
											parallelBenchmarkUpload(b, c, 512, st.sz, pu.pu, tt)
										})
										b.Run("parallelBenchmarkUpload1024", func(b *testing.B) {
											parallelBenchmarkUpload(b, c, 1024, st.sz, pu.pu, tt)
										})
										b.Run("parallelBenchmarkUpload2048", func(b *testing.B) {
											parallelBenchmarkUpload(b, c, 2048, st.sz, pu.pu, tt)
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
				c.Connection.CloseConnection(ctx)
				if !c.Direct {
					paip.StopServer()
				}

				StopAndRemoveContainer(ctx, c.Cfg.containername)
				if name == "S3" {
					StopAndRemoveContainer(ctx, "minio")
				}
			})
		}
	}

}

func parallelBenchmarkUpload(b *testing.B, c *TestRun, blockSize int, fileSize int64, percentUnique, threads int) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type th struct {
		fn         string
		bt         []byte
		offset     int64
		connection *api.SdfsConnection
		f          *os.File
	}
	inv := 100 - percentUnique
	blockSz := 1024 * 2048
	cr, err := DockerExec(ctx, c.Cfg.containername, strings.Split("du -sh /opt/sdfs/volumes/.pool0/chunkstore/chunks", " "))
	if err != nil {
		b.Errorf("error while execing du %d  %v", cr.ExitCode, err)
	}
	log.Infof("storage size = %s\n", strings.Split(cr.outBuffer.String(), "\t")[0])
	for i := 0; i < b.N; i++ {
		var ths []*th

		for z := 0; z < threads; z++ {
			fn := fmt.Sprintf("%s/%d", "/opt/sdfs/tst", z)
			f, err := os.OpenFile(fn, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				b.Errorf("error while creating file %s  %v", fn, err)
			}
			defer f.Close()
			bt := randBytesMaskImpr(blockSz)
			connection := BConnect(b, c)
			thh := &th{fn: fn, offset: int64(0), bt: bt, connection: connection, f: f}
			ths = append(ths, thh)
			if err != nil {
				b.Errorf("error while creating hash file %s  %v", fn, err)
			}
			ct := 0
			for thh.offset < fileSize {
				_, err := thh.f.Write(thh.bt)
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
		}
		b.StartTimer()
		wg := &sync.WaitGroup{}
		wg.Add(threads)
		for z := 0; z < threads; z++ {
			thh := ths[z]
			go func() {

				thh.connection.Upload(ctx, thh.fn, thh.fn, blockSize)
				thh.connection.CloseConnection(ctx)
				wg.Done()
			}()
		}
		wg.Wait()
		b.StopTimer()
		var sz int64
		var asz int64
		for z := 0; z < threads; z++ {
			sz += ths[z].offset
			stat, _ := c.Connection.Stat(ctx, ths[z].fn)
			asz += stat.IoMonitor.ActualBytesWritten
			c.Connection.Unlink(ctx, ths[z].fn)
			os.Remove(ths[z].fn)
		}
		b.SetBytes(sz)
		cr, err = DockerExec(ctx, c.Cfg.containername, strings.Split("du -sh /opt/sdfs/volumes/.pool0/chunkstore/chunks", " "))
		if err != nil {
			b.Errorf("error while execing du %d  %v", cr.ExitCode, err)
		}
		log.Infof("actual bytes written= %d storage size = %s\n", asz, strings.Split(cr.outBuffer.String(), "\t")[0])
	}

}

/*
func parallelBenchmarkWrite(b *testing.B, c *TestRun, blockSize int, fileSize int64, percentUnique, threads int) {
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
			connection := BConnect(b, c)
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
			c.Connection.Unlink(ctx, ths[z].fn)
		}
		b.SetBytes(sz)

	}

}
*/
func parallelBenchmarkRead(b *testing.B, c *TestRun, blockSize int, fileSize int64, percentUnique, threads int) {
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
			connection := BConnect(b, c)
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
			c.Connection.Unlink(ctx, ths[z].fn)
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

func testNewProxyConnection(t *testing.T, c *TestRun) {
	t.Logf("Creating connection for %d\n", c.Volume)
	c.Connection = Dconnect(t, c)
	ct := 0
	for c.Connection == nil {
		time.Sleep(15 * time.Second)
		c.Connection = Dconnect(t, c)
		ct++
		if ct > 5 {
			break
		}

	}
	assert.NotNil(t, c.Connection)

}

func testCompression(t *testing.T, c *TestRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	uf := "resources/testdata.tar"
	fi, err := os.Stat(uf)
	assert.Nil(t, err)
	fn := string(randBytesMaskImpr(16))
	_, err = c.Connection.Upload(ctx, uf, fn, 1024)
	assert.Nil(t, err)
	info, _ := c.Connection.Stat(ctx, fn)
	t.Logf("afd %d vfd %d fsz %d", info.IoMonitor.ActualBytesWritten, info.IoMonitor.VirtualBytesWritten, fi.Size())
	assert.Less(t, info.IoMonitor.ActualBytesWritten, fi.Size())
	err = c.Connection.DeleteFile(ctx, fn)
	assert.Nil(t, err)
}

func testChow(t *testing.T, c *TestRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fn, _ := makeFile(ctx, t, c, "", 128)
	err := c.Connection.Chown(ctx, fn, int32(100), int32(100))
	assert.Nil(t, err)
	stat, err := c.Connection.GetAttr(ctx, fn)
	assert.Nil(t, err)
	assert.Equal(t, stat.Gid, int32(100))
	assert.Equal(t, stat.Uid, int32(100))
	deleteFile(t, c, fn)
}

func testMkNod(t *testing.T, c *TestRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fn, _ := makeFile(ctx, t, c, "", 500*1024*1024)
	exists, err := c.Connection.FileExists(ctx, fn)
	assert.Nil(t, err)
	assert.True(t, exists)
	deleteFile(t, c, fn)
}

func testCrashRead(t *testing.T, c *TestRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fn, _ := makeFile(ctx, t, c, "", 128)
	exists, err := c.Connection.FileExists(ctx, fn)
	assert.Nil(t, err)
	assert.True(t, exists)
	deleteFile(t, c, fn)
}

func testCrashRead(t *testing.T, c *TestRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fn, _ := makeFile(ctx, t, c, "", 128)
	exists, err := c.Connection.FileExists(ctx, fn)
	assert.Nil(t, err)
	assert.True(t, exists)
	deleteFile(t, c, fn)
}

func testMkDir(t *testing.T, c *TestRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := c.Connection.MkDir(ctx, "testdir", 511)
	assert.Nil(t, err)
	stat, err := c.Connection.GetAttr(ctx, "testdir")
	assert.Nil(t, err)
	if runtime.GOOS != "windows" {
		assert.Equal(t, stat.Mode, int32(16895))
	}
	err = c.Connection.RmDir(ctx, "testdir")
	assert.Nil(t, err)
	_, err = c.Connection.GetAttr(ctx, "testdir")
	assert.NotNil(t, err)
}

func testMkDirAll(t *testing.T, c *TestRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := c.Connection.MkDirAll(ctx, "testdir/t")
	assert.Nil(t, err)
	stat, err := c.Connection.GetAttr(ctx, "testdir/t")
	assert.Nil(t, err)
	if runtime.GOOS != "windows" {
		assert.Equal(t, stat.Mode, int32(16832))
	}
	err = c.Connection.RmDir(ctx, "testdir/t")
	assert.Nil(t, err)
	_, err = c.Connection.GetAttr(ctx, "testdir/t")
	assert.NotNil(t, err)
	err = c.Connection.RmDir(ctx, "testdir")
	assert.Nil(t, err)
	_, err = c.Connection.GetAttr(ctx, "testdir")
	assert.NotNil(t, err)
}

func testListDir(t *testing.T, c *TestRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dn := string(randBytesMaskImpr(16))
	err := c.Connection.MkDir(ctx, dn, 511)
	assert.Nil(t, err)
	var files []string
	for i := 0; i < 10; i++ {
		fn, _ := makeFile(ctx, t, c, dn, 1024)
		files = append(files, fn)
	}
	_, list, err := c.Connection.ListDir(ctx, dn, "", false, 20)
	assert.Nil(t, err)
	var afiles []string
	for _, l := range list {
		afiles = append(afiles, l.FilePath)
		c.Connection.DeleteFile(ctx, l.FilePath)
	}
	if runtime.GOOS != "windows" {
		assert.ElementsMatch(t, files, afiles)
	} else {
		assert.Equal(t, len(files), len(afiles))
	}
	err = c.Connection.RmDir(ctx, dn)
	assert.Nil(t, err)
	_, err = c.Connection.GetAttr(ctx, dn)
	assert.NotNil(t, err)
}

func testReplicateFile(t *testing.T, c *TestRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := &ContainerConfig{containername: "block-6443", hostPort: "6443", mountstorage: false}
	//c.Cfg = cfg
	_c, err := CreateBlockSetup(ctx, cfg)
	_c.Direct = true
	_c.Clientsidededupe = false
	assert.Nil(t, err)
	testNewProxyConnection(t, _c)
	assert.NotNil(t, _c.Connection)
	defer StopAndRemoveContainer(ctx, _c.Cfg.containername)
	fn, hs := makeFile(ctx, t, c, "", 80*1024*1024)
	address := fmt.Sprintf("sdfs://%s:6442", c.Cfg.containername)
	_, err = _c.Connection.ReplicateRemoteFile(ctx, fn, fn, address, c.Volume, false, 0, 0, 0, false, true)
	assert.Nil(t, err)
	nhs, _ := readFile(ctx, t, _c, fn, false)
	assert.Equal(t, nhs, hs)
	_, err = _c.Connection.ReplicateRemoteFile(ctx, fn, fn+"-1", address, c.Volume, false, 0, 0, 0, false, true)
	assert.Nil(t, err)
	nhs, _ = readFile(ctx, t, _c, fn+"-1", false)
	assert.Equal(t, nhs, hs)
	_, stats, _ := _c.Connection.ListDir(ctx, fn+"-1", "", false, 1)
	assert.Equal(t, stats[0].IoMonitor.ActualBytesWritten, int64(0))
	_c.Connection.DeleteFile(ctx, fn)
	_c.Connection.DeleteFile(ctx, fn+"-1")
	c.Connection.DeleteFile(ctx, fn)
}

func testReplicateFileOffset(t *testing.T, c *TestRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := &ContainerConfig{containername: "block-6443", hostPort: "6443", mountstorage: false}
	//c.Cfg = cfg
	_c, err := CreateBlockSetup(ctx, cfg)
	_c.Direct = true
	_c.Clientsidededupe = false
	offset := int64(16 * 1024 * 1024)
	fsz := int64(1024 * 1024 * 1024)
	assert.Nil(t, err)
	testNewProxyConnection(t, _c)
	assert.NotNil(t, _c.Connection)
	defer StopAndRemoveContainer(ctx, _c.Cfg.containername)
	//Test Upload after offset
	fn, _ := makeFile(ctx, t, c, "", offset)
	fn, ehs := makeFileOffset(ctx, t, c, fn, fsz, offset)
	rfn, rhs := makeFile(ctx, t, _c, "", offset)
	address := fmt.Sprintf("sdfs://%s:6442", c.Cfg.containername)
	_, err = _c.Connection.ReplicateRemoteFile(ctx, fn, rfn, address, c.Volume, false, offset, fsz, offset, true, true)
	assert.Nil(t, err)
	nhs, _ := readFileOffset(ctx, t, _c, rfn, 0, offset, false)
	assert.Equal(t, nhs, rhs)
	nhs, _ = readFileOffset(ctx, t, _c, rfn, offset, fsz, true)
	assert.Equal(t, nhs, ehs)
	_c.Connection.DeleteFile(ctx, rfn)
	c.Connection.DeleteFile(ctx, fn)
	//Test Replicate before offset
	fn, hs := makeFile(ctx, t, c, "", offset)
	rfn, _ = makeFile(ctx, t, _c, "", offset)
	evt, err := _c.Connection.ReplicateRemoteFile(ctx, fn, rfn, address, c.Volume, false, 0, offset, 0, true, true)
	assert.Nil(t, err)
	log.Infof("evt %v", evt)
	nhs, err = readFileOffset(ctx, t, _c, rfn, 0, offset, false)
	assert.Nil(t, err)
	assert.Equal(t, nhs, hs)
	fi, _ := _c.Connection.Stat(ctx, rfn)
	assert.Equal(t, fi.Size, offset)
	_c.Connection.DeleteFile(ctx, rfn)
	c.Connection.DeleteFile(ctx, fn)

}

func testReplicateFileAfterBadURL(t *testing.T, c *TestRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := &ContainerConfig{containername: "block-6443", hostPort: "6443", mountstorage: false}
	//c.Cfg = cfg
	_c, err := CreateBlockSetup(ctx, cfg)
	_c.Direct = true
	_c.Clientsidededupe = false
	assert.Nil(t, err)
	testNewProxyConnection(t, _c)
	assert.NotNil(t, _c.Connection)
	defer StopAndRemoveContainer(ctx, _c.Cfg.containername)
	fn, hs := makeFile(ctx, t, c, "", 80*1024*1024)
	address := "sdfs://badurl:6442"
	evt, err := _c.Connection.ReplicateRemoteFile(ctx, fn, fn, address, c.Volume, false, 0, 0, 0, false, false)
	assert.Nil(t, err)
	time.Sleep(30 * time.Second)
	evt, _ = _c.Connection.GetEvent(ctx, evt.Uuid)
	assert.Equal(t, evt.EndTime, int64(-1))
	address = fmt.Sprintf("sdfs://%s:6442", c.Cfg.containername)
	_, err = _c.Connection.ReplicateRemoteFile(ctx, fn, fn, address, c.Volume, false, 0, 0, 0, false, true)
	assert.NotNil(t, err)
	time.Sleep(30 * time.Second)
	err = _c.Connection.CancelReplication(ctx, evt.Uuid)
	assert.Nil(t, err)
	time.Sleep(30 * time.Second)
	_, err = _c.Connection.ReplicateRemoteFile(ctx, fn, fn, address, c.Volume, false, 0, 0, 0, false, true)
	assert.Nil(t, err)
	nhs, _ := readFile(ctx, t, _c, fn, false)
	assert.Equal(t, nhs, hs)
	_c.Connection.DeleteFile(ctx, fn)
	c.Connection.DeleteFile(ctx, fn)
}

func testReplicateFileLarge(t *testing.T, c *TestRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := &ContainerConfig{containername: "block-6443", hostPort: "6443", mountstorage: false}
	//c.Cfg = cfg
	_c, err := CreateBlockSetup(ctx, cfg)
	_c.Direct = true
	_c.Clientsidededupe = false
	assert.Nil(t, err)
	testNewProxyConnection(t, _c)
	assert.NotNil(t, _c.Connection)
	defer StopAndRemoveContainer(ctx, _c.Cfg.containername)
	for i := 1; i < 11; i++ {
		t.Logf("Test %d Started", i)
		fn, hs := makeFile(ctx, t, c, "", 10*1000*1024*1024)
		address := fmt.Sprintf("sdfs://%s:6442", c.Cfg.containername)
		_, err = _c.Connection.ReplicateRemoteFile(ctx, fn, fn, address, c.Volume, false, 0, 0, 0, false, true)
		assert.Nil(t, err)
		nhs, _ := readFile(ctx, t, _c, fn, false)
		assert.Equal(t, nhs, hs)
		_c.Connection.DeleteFile(ctx, fn)
		c.Connection.DeleteFile(ctx, fn)
		t.Logf("Test %d Done", i)
	}
}

func testReplicatePauseCancelDelete(t *testing.T, c *TestRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := &ContainerConfig{containername: "block-6443", hostPort: "6443", mountstorage: false}
	//c.Cfg = cfg
	_c, err := CreateBlockSetup(ctx, cfg)
	_c.Direct = true
	_c.Clientsidededupe = false
	assert.Nil(t, err)
	testNewProxyConnection(t, _c)
	assert.NotNil(t, _c.Connection)
	defer StopAndRemoveContainer(ctx, _c.Cfg.containername)
	fn, _ := makeFile(ctx, t, c, "", 3*1024*1024*1024)
	address := fmt.Sprintf("sdfs://%s:6442", c.Cfg.containername)
	eid, err := _c.Connection.ReplicateRemoteFile(ctx, fn, fn, address, c.Volume, false, 0, 0, 0, false, false)
	assert.Nil(t, err)
	_c.Connection.PauseReplication(ctx, eid.Uuid, true)
	evt, _ := _c.Connection.GetEvent(ctx, eid.Uuid)
	assert.Equal(t, evt.Attributes["paused"], "true")
	info, err := _c.Connection.DSEInfo(ctx)
	assert.Nil(t, err)
	sz := info.CurrentSize
	time.Sleep(5 * time.Second)
	info, err = _c.Connection.DSEInfo(ctx)
	assert.Nil(t, err)
	nsz := info.CurrentSize
	assert.Equal(t, sz, nsz)
	_c.Connection.PauseReplication(ctx, eid.Uuid, false)
	err = _c.Connection.CancelReplication(ctx, eid.Uuid)
	assert.Nil(t, err)
	time.Sleep(30 * time.Second)
	_, fi, err := _c.Connection.ListDir(ctx, fn, "", false, 1)
	assert.NotNil(t, err)
	t.Logf("File info %v", fi)
	_c.Connection.DeleteFile(ctx, fn)
	c.Connection.DeleteFile(ctx, fn)
}

func testReplicatePause(t *testing.T, c *TestRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := &ContainerConfig{containername: "block-6443", hostPort: "6443", mountstorage: false}
	//c.Cfg = cfg
	_c, err := CreateBlockSetup(ctx, cfg)
	_c.Direct = true
	_c.Clientsidededupe = false
	assert.Nil(t, err)
	testNewProxyConnection(t, _c)
	assert.NotNil(t, _c.Connection)
	defer StopAndRemoveContainer(ctx, _c.Cfg.containername)
	fn, hs := makeFile(ctx, t, c, "", 1024*1024*1024)
	address := fmt.Sprintf("sdfs://%s:6442", c.Cfg.containername)
	eid, err := _c.Connection.ReplicateRemoteFile(ctx, fn, fn, address, c.Volume, false, 0, 0, 0, false, false)
	assert.Nil(t, err)
	_c.Connection.PauseReplication(ctx, eid.Uuid, true)
	evt, _ := _c.Connection.GetEvent(ctx, eid.Uuid)
	assert.Equal(t, evt.Attributes["paused"], "true")
	info, err := _c.Connection.DSEInfo(ctx)
	assert.Nil(t, err)
	sz := info.CurrentSize
	time.Sleep(5 * time.Second)
	info, err = _c.Connection.DSEInfo(ctx)
	assert.Nil(t, err)
	nsz := info.CurrentSize
	assert.Equal(t, sz, nsz)
	t.Logf("Pause Size is %d", nsz)
	_c.Connection.PauseReplication(ctx, eid.Uuid, false)
	evt, _ = _c.Connection.GetEvent(ctx, eid.Uuid)
	assert.Equal(t, evt.Attributes["paused"], "false")
	_c.Connection.WaitForEvent(ctx, evt.Uuid)
	nhs, _ := readFile(ctx, t, _c, fn, true)
	assert.Equal(t, nhs, hs)
	_c.Connection.DeleteFile(ctx, fn)
	c.Connection.DeleteFile(ctx, fn)
	//Pause after 5 seconds
	fn, hs = makeFile(ctx, t, c, "", 1024*1024*1024)
	eid, err = _c.Connection.ReplicateRemoteFile(ctx, fn, fn, address, c.Volume, false, 0, 0, 0, false, false)
	assert.Nil(t, err)
	_c.Connection.PauseReplication(ctx, eid.Uuid, true)
	time.Sleep(5 * time.Second)
	evt, _ = _c.Connection.GetEvent(ctx, eid.Uuid)
	assert.Equal(t, evt.Attributes["paused"], "true")
	info, err = _c.Connection.DSEInfo(ctx)
	assert.Nil(t, err)
	sz = info.CurrentSize
	time.Sleep(5 * time.Second)
	info, err = _c.Connection.DSEInfo(ctx)
	assert.Nil(t, err)
	nsz = info.CurrentSize
	assert.Equal(t, sz, nsz)
	t.Logf("Pause Size is %d", nsz)
	_c.Connection.PauseReplication(ctx, eid.Uuid, false)
	evt, _ = _c.Connection.GetEvent(ctx, eid.Uuid)
	assert.Equal(t, evt.Attributes["paused"], "false")
	_c.Connection.WaitForEvent(ctx, evt.Uuid)
	info, err = _c.Connection.DSEInfo(ctx)
	assert.Nil(t, err)
	nsz = info.CurrentSize
	assert.Greater(t, nsz, sz)
	nhs, _ = readFile(ctx, t, _c, fn, true)
	assert.Equal(t, nhs, hs)
	_c.Connection.DeleteFile(ctx, fn)
	c.Connection.DeleteFile(ctx, fn)
}

func testReplicateCanceledErrors(t *testing.T, c *TestRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := &ContainerConfig{containername: "block-6443", hostPort: "6443", mountstorage: false}
	//c.Cfg = cfg
	_c, err := CreateBlockSetup(ctx, cfg)
	_c.Direct = true
	_c.Clientsidededupe = false
	assert.Nil(t, err)
	testNewProxyConnection(t, _c)
	assert.NotNil(t, _c.Connection)
	defer StopAndRemoveContainer(ctx, _c.Cfg.containername)
	fn, _ := makeFile(ctx, t, c, "", 1000*1024*1024)
	address := fmt.Sprintf("sdfs://%s:6442", c.Cfg.containername)
	eid, err := _c.Connection.ReplicateRemoteFile(ctx, fn, fn, address, c.Volume, false, 0, 0, 0, false, false)
	assert.Nil(t, err)
	_c.Connection.CancelReplication(ctx, eid.Uuid)
	err = _c.Connection.CancelReplication(ctx, eid.Uuid)
	assert.NotNil(t, err)
	t.Logf("Error %v", err)
	_c.Connection.DeleteFile(ctx, fn)
	eid, err = _c.Connection.ReplicateRemoteFile(ctx, fn, fn, address, c.Volume, false, 0, 0, 0, false, true)
	assert.Nil(t, err)
	err = _c.Connection.CancelReplication(ctx, eid.Uuid)
	assert.NotNil(t, err)
	t.Logf("Error %v", err)
	_c.Connection.DeleteFile(ctx, fn)
	c.Connection.DeleteFile(ctx, fn)
}

func testReplicateCanceled(t *testing.T, c *TestRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := &ContainerConfig{containername: "block-6443", hostPort: "6443", mountstorage: false}
	//c.Cfg = cfg
	_c, err := CreateBlockSetup(ctx, cfg)
	_c.Direct = true
	_c.Clientsidededupe = false
	assert.Nil(t, err)
	testNewProxyConnection(t, _c)
	assert.NotNil(t, _c.Connection)
	defer StopAndRemoveContainer(ctx, _c.Cfg.containername)
	fn, _ := makeFile(ctx, t, c, "", 1000*1024*1024)
	address := fmt.Sprintf("sdfs://%s:6442", c.Cfg.containername)
	eid, err := _c.Connection.ReplicateRemoteFile(ctx, fn, fn, address, c.Volume, false, 0, 0, 0, false, false)
	assert.Nil(t, err)
	_c.Connection.CancelReplication(ctx, eid.Uuid)
	err = _c.Connection.CancelReplication(ctx, eid.Uuid)
	assert.NotNil(t, err)
	t.Logf("Error %v", err)
	time.Sleep(15 * time.Second)
	evt, _ := _c.Connection.GetEvent(ctx, eid.Uuid)
	assert.Greater(t, evt.EndTime, int64(0))
	_, _, err = _c.Connection.ListDir(ctx, fn, "", false, 1)
	assert.NotNil(t, err)
	_c.Connection.DeleteFile(ctx, fn)
	//After 6 seconds
	eid, err = _c.Connection.ReplicateRemoteFile(ctx, fn, fn, address, c.Volume, false, 0, 0, 0, false, false)
	assert.Nil(t, err)
	time.Sleep(6 * time.Second)
	_c.Connection.CancelReplication(ctx, eid.Uuid)
	time.Sleep(15 * time.Second)
	evt, _ = _c.Connection.GetEvent(ctx, eid.Uuid)
	assert.Greater(t, evt.EndTime, int64(0))
	_, _, err = _c.Connection.ListDir(ctx, fn, "", false, 1)
	assert.NotNil(t, err)
	//Clean store and make sure the canceled files aren't there anymore
	time.Sleep(65 * time.Second)
	_c.Connection.CleanStore(ctx, true, true)
	time.Sleep(65 * time.Second)
	info, err := _c.Connection.DSEInfo(ctx)
	assert.Nil(t, err)
	nsz := info.CurrentSize
	assert.Greater(t, int64(16*1024), nsz)
	_c.Connection.DeleteFile(ctx, fn)
	c.Connection.DeleteFile(ctx, fn)
}

func testReplicateCrashRestart(t *testing.T, c *TestRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := &ContainerConfig{containername: "block-6443", hostPort: "6443", mountstorage: false}
	//c.Cfg = cfg
	_c, err := CreateBlockSetup(ctx, cfg)
	_c.Direct = true
	_c.Clientsidededupe = false
	assert.Nil(t, err)
	testNewProxyConnection(t, _c)
	assert.NotNil(t, _c.Connection)
	defer StopAndRemoveContainer(ctx, _c.Cfg.containername)
	fn, fh := makeFile(ctx, t, c, "", 10*1024*1024*1024)
	nhs, err := readFile(ctx, t, c, fn, false)
	assert.Nil(t, err)
	assert.Equal(t, nhs, fh)
	address := fmt.Sprintf("sdfs://%s:6442", c.Cfg.containername)
	_, err = _c.Connection.ReplicateRemoteFile(ctx, fn, fn, address, c.Volume, false, 0, 0, 0, false, false)
	assert.Nil(t, err)
	StopContainer(ctx, cfg.containername)
	StartContainer(ctx, cfg)
	time.Sleep(5 * time.Minute)
	nhs, err = readFile(ctx, t, _c, fn, false)
	assert.Nil(t, err)
	assert.Equal(t, nhs, fh)
	_c.Connection.DeleteFile(ctx, fn)
	c.Connection.DeleteFile(ctx, fn)
}

func testCrashRestart(t *testing.T, c *TestRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fn, fh := makeFile(ctx, t, c, "", 1*1024*1024*1024) //1 GB File
	nhs, err := readFile(ctx, t, c, fn, false)
	assert.Nil(t, err)
	assert.Equal(t, nhs, fh)
	dse, _ := c.Connection.DSEInfo(ctx)
	StopContainer(ctx, c.Cfg.containername)
	StartContainer(ctx, c.Cfg)
	time.Sleep(60 * time.Second)
	nhs, _ = readFile(ctx, t, c, fn, false)
	ndse, err := c.Connection.DSEInfo(ctx)
	assert.Equal(t, dse.Entries, ndse.Entries)
	assert.Nil(t, err)
	assert.Equal(t, nhs, fh)
	c.Connection.DeleteFile(ctx, fn)
}

func testReplicatePauseCancelCrashRestart(t *testing.T, c *TestRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := &ContainerConfig{containername: "block-6443", hostPort: "6443", mountstorage: false}
	//c.Cfg = cfg
	_c, err := CreateBlockSetup(ctx, cfg)
	_c.Direct = true
	_c.Clientsidededupe = false
	assert.Nil(t, err)
	testNewProxyConnection(t, _c)
	assert.NotNil(t, _c.Connection)
	defer StopAndRemoveContainer(ctx, _c.Cfg.containername)
	fn, fh := makeFile(ctx, t, c, "", 10*1024*1024*1024) //10 GB File
	nhs, err := readFile(ctx, t, c, fn, false)
	assert.Nil(t, err)
	assert.Equal(t, nhs, fh)
	address := fmt.Sprintf("sdfs://%s:6442", c.Cfg.containername)
	eid, _ := _c.Connection.ReplicateRemoteFile(ctx, fn, fn, address, c.Volume, false, 0, 0, 0, false, false)
	time.Sleep(15 * time.Second)
	_c.Connection.PauseReplication(ctx, eid.Uuid, true)
	evt, _ := _c.Connection.GetEvent(ctx, eid.Uuid)
	assert.Equal(t, evt.Attributes["paused"], "true")
	_c.Connection.PauseReplication(ctx, eid.Uuid, false)
	time.Sleep(15 * time.Second)
	_c.Connection.CancelReplication(ctx, eid.Uuid)
	time.Sleep(65 * time.Second)
	_c.Connection.CleanStore(ctx, true, true)
	time.Sleep(65 * time.Second)
	eid, err = _c.Connection.ReplicateRemoteFile(ctx, fn, fn, address, c.Volume, false, 0, 0, 0, false, false)
	assert.Nil(t, err)
	time.Sleep(15 * time.Second)
	eid, _ = _c.Connection.GetEvent(ctx, eid.Uuid)
	t.Logf("event %v", eid)
	StopContainer(ctx, cfg.containername)
	StopContainer(ctx, c.Cfg.containername)
	StartContainer(ctx, c.Cfg)
	StartContainer(ctx, cfg)
	time.Sleep(8 * time.Minute)
	nhs, err = readFile(ctx, t, _c, fn, false)
	assert.Nil(t, err)
	assert.Equal(t, nhs, fh)
	_c.Connection.DeleteFile(ctx, fn)
	c.Connection.DeleteFile(ctx, fn)
}

func testReplicateCrashSourceRestart(t *testing.T, c *TestRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := &ContainerConfig{containername: "block-6443", hostPort: "6443", mountstorage: false}
	//c.Cfg = cfg
	_c, err := CreateBlockSetup(ctx, cfg)
	_c.Direct = true
	_c.Clientsidededupe = false
	assert.Nil(t, err)
	testNewProxyConnection(t, _c)
	assert.NotNil(t, _c.Connection)
	defer StopAndRemoveContainer(ctx, _c.Cfg.containername)
	fn, fh := makeFile(ctx, t, c, "", 10*1024*1024*1024)
	nhs, err := readFile(ctx, t, c, fn, false)
	assert.Nil(t, err)
	assert.Equal(t, nhs, fh)
	address := fmt.Sprintf("sdfs://%s:6442", c.Cfg.containername)
	eid, err := _c.Connection.ReplicateRemoteFile(ctx, fn, fn, address, c.Volume, false, 0, 0, 0, false, false)
	assert.Nil(t, err)
	StopContainer(ctx, c.Cfg.containername)
	StartContainer(ctx, c.Cfg)
	time.Sleep(6 * time.Minute)
	for i := 1; i < 5; i++ {
		evt, err := _c.Connection.GetEvent(ctx, eid.Uuid)
		assert.Nil(t, err)
		if err != nil {
			break
		}
		if evt.EndTime > 0 {
			break
		}
		time.Sleep(15 * time.Second)
	}
	nhs, err = readFile(ctx, t, _c, fn, false)
	assert.Nil(t, err)
	assert.Equal(t, nhs, fh)
	_c.Connection.DeleteFile(ctx, fn)
	c.Connection.DeleteFile(ctx, fn)
}

func testDiskFull(t *testing.T, c *TestRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := &ContainerConfig{containername: "block-6443", hostPort: "6443", mountstorage: false}
	//c.Cfg = cfg
	_c, err := CreateSmallBlockSetup(ctx, cfg)
	_c.Direct = true
	_c.Clientsidededupe = false
	assert.Nil(t, err)
	testNewProxyConnection(t, _c)
	assert.NotNil(t, _c.Connection)
	defer StopAndRemoveContainer(ctx, _c.Cfg.containername)
	makeFile(ctx, t, _c, "", 200*1024*1024*1024)
}

func testReplicateSyncFile(t *testing.T, c *TestRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := &ContainerConfig{containername: "block-6443", hostPort: "6443", mountstorage: false}
	//c.Cfg = cfg
	_c, err := CreateBlockSetup(ctx, cfg)
	_c.Direct = true
	_c.Clientsidededupe = false
	assert.Nil(t, err)
	testNewProxyConnection(t, _c)
	assert.NotNil(t, _c.Connection)
	defer StopAndRemoveContainer(ctx, _c.Cfg.containername)
	fn, hs := makeFile(ctx, t, c, "", 500*1024*1024)
	address := fmt.Sprintf("sdfs://%s:6442", c.Cfg.containername)
	err = _c.Connection.AddReplicationSrc(ctx, address, c.Volume, false)
	assert.Nil(t, err)
	time.Sleep(15 * time.Second)
	nhs, _ := readFile(ctx, t, _c, fn, false)
	assert.Equal(t, nhs, hs)
	c.Connection.DeleteFile(ctx, fn)

	time.Sleep(15 * time.Second)
	_, _, err = _c.Connection.ListDir(ctx, fn, "", false, 1)
	assert.NotNil(t, err)
	fn, hs = makeFile(ctx, t, c, "", 500*1024*1024)
	time.Sleep(15 * time.Second)
	nhs, _ = readFile(ctx, t, _c, fn, false)
	assert.Equal(t, nhs, hs)
	c.Connection.DeleteFile(ctx, fn)
	time.Sleep(15 * time.Second)
	_, _, err = _c.Connection.ListDir(ctx, fn, "", false, 1)
	assert.NotNil(t, err)

}

func testReplicateSyncUpload(t *testing.T, c *TestRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := &ContainerConfig{containername: "block-6443", hostPort: "6443", mountstorage: false}
	//c.Cfg = cfg
	_c, err := CreateBlockSetup(ctx, cfg)
	_c.Direct = true
	_c.Clientsidededupe = false
	assert.Nil(t, err)
	testNewProxyConnection(t, _c)
	assert.NotNil(t, _c.Connection)
	defer StopAndRemoveContainer(ctx, _c.Cfg.containername)
	//Create local 2GB File
	blockSz := 32 * 1024
	fileSize := int64(2 * 1024 * 1024 * 1024)
	var offset int64
	fn := fmt.Sprintf("%s/%s", "/opt/sdfs/tst", string(randBytesMaskImpr(16)))
	f, err := os.OpenFile(fn, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Errorf("error while creating file %s  %v", fn, err)
	}
	defer f.Close()
	bt := randBytesMaskImpr(blockSz)

	if err != nil {
		t.Errorf("error while creating hash file %s  %v", fn, err)
	}
	for offset < fileSize {
		_, err := f.WriteString(string(bt))
		if err != nil {
			t.Errorf("error writing data at %d  %v", offset, err)
			return

		}
		offset += int64(len(bt))
		bt = randBytesMaskImpr(blockSz)

	}

	defer os.Remove(fn)
	assert.Nil(t, err)

	assert.Nil(t, err)
	address := fmt.Sprintf("sdfs://%s:6442", c.Cfg.containername)
	err = _c.Connection.AddReplicationSrc(ctx, address, c.Volume, false)
	assert.Nil(t, err)
	time.Sleep(15 * time.Second)
	_, err = c.Connection.Upload(ctx, fn, fn, 1024)
	assert.Nil(t, err)
	hs, _ := readFile(ctx, t, c, fn, false)
	time.Sleep(32 * time.Second)
	nhs, _ := readFile(ctx, t, _c, fn, false)
	assert.Equal(t, hs, nhs)
	_c.Connection.Download(ctx, fn, fn+".n", 32*1024)
	c.Connection.DeleteFile(ctx, fn)
	time.Sleep(15 * time.Second)
	_, _, err = _c.Connection.ListDir(ctx, fn, "", false, 1)
	assert.NotNil(t, err)
}

func testReplicateSyncAddRemove(t *testing.T, c *TestRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := &ContainerConfig{containername: "block-6443", hostPort: "6443", mountstorage: false}
	//c.Cfg = cfg
	_c, err := CreateBlockSetup(ctx, cfg)
	_c.Direct = true
	_c.Clientsidededupe = false
	assert.Nil(t, err)
	testNewProxyConnection(t, _c)
	assert.NotNil(t, _c.Connection)
	defer StopAndRemoveContainer(ctx, _c.Cfg.containername)
	fn, hs := makeFile(ctx, t, c, "", 500*1024*1024)
	address := fmt.Sprintf("sdfs://%s:6442", c.Cfg.containername)
	err = _c.Connection.AddReplicationSrc(ctx, address, c.Volume, false)
	assert.Nil(t, err)
	time.Sleep(15 * time.Second)
	nhs, _ := readFile(ctx, t, _c, fn, false)
	assert.Equal(t, nhs, hs)
	c.Connection.DeleteFile(ctx, fn)
	time.Sleep(15 * time.Second)
	_, _, err = _c.Connection.ListDir(ctx, fn, "", false, 1)
	assert.NotNil(t, err)
	fn, _ = makeFile(ctx, t, c, "", 5*1024*1024*1024)
	time.Sleep(10 * time.Second)
	err = _c.Connection.RemoveReplicationSrc(ctx, address, c.Volume)
	assert.Nil(t, err)
	_, _, err = _c.Connection.ListDir(ctx, fn, "", false, 1)
	assert.NotNil(t, err)
}

func testReplicateSyncAddRemoveAdd(t *testing.T, c *TestRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := &ContainerConfig{containername: "block-6443", hostPort: "6443", mountstorage: false}
	//c.Cfg = cfg
	_c, err := CreateBlockSetup(ctx, cfg)
	_c.Direct = true
	_c.Clientsidededupe = false
	assert.Nil(t, err)
	testNewProxyConnection(t, _c)
	assert.NotNil(t, _c.Connection)
	defer StopAndRemoveContainer(ctx, _c.Cfg.containername)
	fn, hs := makeFile(ctx, t, c, "", 500*1024*1024)
	address := fmt.Sprintf("sdfs://%s:6442", c.Cfg.containername)
	err = _c.Connection.AddReplicationSrc(ctx, address, c.Volume, false)
	assert.Nil(t, err)
	time.Sleep(15 * time.Second)
	nhs, _ := readFile(ctx, t, _c, fn, false)
	assert.Equal(t, nhs, hs)
	c.Connection.DeleteFile(ctx, fn)
	time.Sleep(15 * time.Second)
	_, _, err = _c.Connection.ListDir(ctx, fn, "", false, 1)
	assert.NotNil(t, err)
	fn, hs = makeFile(ctx, t, c, "", 5*1024*1024*1024)
	time.Sleep(10 * time.Second)
	err = _c.Connection.RemoveReplicationSrc(ctx, address, c.Volume)
	assert.Nil(t, err)
	time.Sleep(30 * time.Second)
	err = _c.Connection.AddReplicationSrc(ctx, address, c.Volume, false)
	assert.Nil(t, err)
	time.Sleep(60 * time.Second)
	nhs, _ = readFile(ctx, t, _c, fn, false)
	assert.Equal(t, nhs, hs)
	fn, hs = makeFile(ctx, t, c, "", 500*1024*1024)
	time.Sleep(30 * time.Second)
	nhs, _ = readFile(ctx, t, _c, fn, false)
	assert.Equal(t, nhs, hs)
}

func testReplicateSyncCmdAddRemove(t *testing.T, c *TestRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := &ContainerConfig{containername: "block-6443", hostPort: "6443", mountstorage: false}
	//c.Cfg = cfg
	address := fmt.Sprintf("sdfs://%s:6442", c.Cfg.containername)
	_c, err := CreateReplBlockSetup(ctx, cfg, address, c.Volume)
	_c.Direct = true
	_c.Clientsidededupe = false
	assert.Nil(t, err)
	testNewProxyConnection(t, _c)
	assert.NotNil(t, _c.Connection)
	defer StopAndRemoveContainer(ctx, _c.Cfg.containername)
	fn, hs := makeFile(ctx, t, c, "", 500*1024*1024)
	nhs, _ := readFile(ctx, t, c, fn, false)
	assert.Equal(t, nhs, hs)
	assert.Nil(t, err)
	time.Sleep(15 * time.Second)
	nhs, _ = readFile(ctx, t, _c, fn, false)
	assert.Equal(t, nhs, hs)
	nhs, _ = readFile(ctx, t, c, fn, false)
	assert.Equal(t, nhs, hs)
	c.Connection.DeleteFile(ctx, fn)
	time.Sleep(15 * time.Second)
<<<<<<< HEAD
	fn, _ = makeFile(ctx, t, c, "", 5*1024*1024*1024)
	time.Sleep(20 * time.Second)
	c.Connection.DeleteFile(ctx, fn)
	time.Sleep(15 * time.Second)
	_, _, err = _c.Connection.ListDir(ctx, fn, "", false, 1)
	assert.NotNil(t, err)
	fn, _ = makeFile(ctx, t, c, "", 5*1024*1024*1024)
	time.Sleep(20 * time.Second)
	err = _c.Connection.RemoveReplicationSrc(ctx, address, c.Volume)
	assert.Nil(t, err)
	_, _, err = _c.Connection.ListDir(ctx, fn, "", false, 1)
	assert.NotNil(t, err)
=======
	_, _, err = _c.Connection.ListDir(ctx, fn, "", false, 1)

	assert.NotNil(t, err)
	//fn, _ = makeFile(ctx, t, c, "", 5*1024*1024*1024)
	//time.Sleep(10 * time.Second)
	//err = _c.Connection.RemoveReplicationSrc(ctx, address, c.Volume)
	//assert.Nil(t, err)
	//_, _, err = _c.Connection.ListDir(ctx, fn, "", false, 1)
	//assert.NotNil(t, err)
>>>>>>> 8fa81aa124fa6120af5bf41ca81e7f7320781b2c
}

func testReplicateSyncAddRemoveNewAdd(t *testing.T, c *TestRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := &ContainerConfig{containername: "block-6443", hostPort: "6443", mountstorage: false}
	//c.Cfg = cfg
	_c, err := CreateBlockSetup(ctx, cfg)
	_c.Direct = true
	_c.Clientsidededupe = false
	assert.Nil(t, err)
	testNewProxyConnection(t, _c)
	assert.NotNil(t, _c.Connection)
	defer StopAndRemoveContainer(ctx, _c.Cfg.containername)
	fn, hs := makeFile(ctx, t, c, "", 500*1024*1024)
	address := fmt.Sprintf("sdfs://%s:6442", c.Cfg.containername)
	err = _c.Connection.AddReplicationSrc(ctx, address, c.Volume, false)
	assert.Nil(t, err)
	time.Sleep(15 * time.Second)
	nhs, _ := readFile(ctx, t, _c, fn, false)
	assert.Equal(t, nhs, hs)
	c.Connection.DeleteFile(ctx, fn)
	time.Sleep(15 * time.Second)
	_, _, err = _c.Connection.ListDir(ctx, fn, "", false, 1)
	assert.NotNil(t, err)
	fn, hs = makeFile(ctx, t, c, "", 5*1024*1024*1024)
	time.Sleep(10 * time.Second)
	err = _c.Connection.RemoveReplicationSrc(ctx, address, c.Volume)
	assert.Nil(t, err)
	time.Sleep(30 * time.Second)
	StopAndRemoveContainer(ctx, _c.Cfg.containername)
	time.Sleep(60 * time.Second)
	_c, err = CreateBlockSetup(ctx, cfg)
	_c.Direct = true
	_c.Clientsidededupe = false
	assert.Nil(t, err)
	testNewProxyConnection(t, _c)

	err = _c.Connection.AddReplicationSrc(ctx, address, c.Volume, false)

	assert.Nil(t, err)
	time.Sleep(60 * time.Second)
	nhs, _ = readFile(ctx, t, _c, fn, false)
	assert.Equal(t, nhs, hs)
	fn, hs = makeFile(ctx, t, c, "", 500*1024*1024)
	time.Sleep(30 * time.Second)
	nhs, _ = readFile(ctx, t, _c, fn, false)
	assert.Equal(t, nhs, hs)
	evts, _ := _c.Connection.ListEvents(ctx)
	t.Logf("%v", evts)
}

func testReplicateSyncRestart(t *testing.T, c *TestRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := &ContainerConfig{containername: "block-6443", hostPort: "6443", mountstorage: false}
	//c.Cfg = cfg
	_c, err := CreateBlockSetup(ctx, cfg)
	_c.Direct = true
	_c.Clientsidededupe = false
	assert.Nil(t, err)
	testNewProxyConnection(t, _c)
	assert.NotNil(t, _c.Connection)
	defer StopAndRemoveContainer(ctx, _c.Cfg.containername)
	type fileinfo struct {
		fn string
		fh []byte
	}
	fn, hs := makeFile(ctx, t, c, "", 80*1024*1024)
	address := fmt.Sprintf("sdfs://%s:6442", c.Cfg.containername)
	err = _c.Connection.AddReplicationSrc(ctx, address, c.Volume, false)
	assert.Nil(t, err)
	assert.Nil(t, err)
	time.Sleep(15 * time.Second)
	nhs, _ := readFile(ctx, t, _c, fn, false)
	assert.Equal(t, nhs, hs)
	c.Connection.DeleteFile(ctx, fn)
	time.Sleep(15 * time.Second)
	_, _, err = _c.Connection.ListDir(ctx, fn, "", false, 1)
	assert.NotNil(t, err)
	fn, hs = makeFile(ctx, t, c, "", 80*1024*1024)
	time.Sleep(15 * time.Second)
	nhs, _ = readFile(ctx, t, _c, fn, false)
	assert.Equal(t, nhs, hs)
	c.Connection.DeleteFile(ctx, fn)
	time.Sleep(15 * time.Second)
	_, _, err = _c.Connection.ListDir(ctx, fn, "", false, 1)
	assert.NotNil(t, err)
	time.Sleep(62 * time.Second)
	nfn, nhs := makeFile(ctx, t, c, "", 1024*1024*1024)
	StopContainer(ctx, cfg.containername)
	time.Sleep(15 * time.Second)
	var files []fileinfo
	for i := 0; i < 8; i++ {
		fn, fh := makeFile(ctx, t, c, "", 1024*1024*80)
		files = append(files, fileinfo{fn: fn, fh: fh})
	}
	StartContainer(ctx, cfg)
	time.Sleep(2 * time.Minute)
	knhs, _ := readFile(ctx, t, _c, nfn, false)
	assert.Equal(t, nhs, knhs)
	for _, fi := range files {
		nhs, _ := readFile(ctx, t, _c, fi.fn, false)
		assert.Equal(t, nhs, fi.fh)
	}
}

func testReplicateSyncSrcRestart(t *testing.T, c *TestRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := &ContainerConfig{containername: "block-6443", hostPort: "6443", mountstorage: false}
	//c.Cfg = cfg
	_c, err := CreateBlockSetup(ctx, cfg)
	_c.Direct = true
	_c.Clientsidededupe = false
	assert.Nil(t, err)
	testNewProxyConnection(t, _c)
	assert.NotNil(t, _c.Connection)
	defer StopAndRemoveContainer(ctx, _c.Cfg.containername)
	type fileinfo struct {
		fn string
		fh []byte
	}
	fn, hs := makeFile(ctx, t, c, "", 80*1024*1024)
	address := fmt.Sprintf("sdfs://%s:6442", c.Cfg.containername)
	err = _c.Connection.AddReplicationSrc(ctx, address, c.Volume, false)
	assert.Nil(t, err)
	assert.Nil(t, err)
	time.Sleep(15 * time.Second)
	nhs, _ := readFile(ctx, t, _c, fn, false)
	assert.Equal(t, nhs, hs)
	c.Connection.DeleteFile(ctx, fn)
	time.Sleep(15 * time.Second)
	_, _, err = _c.Connection.ListDir(ctx, fn, "", false, 1)
	assert.NotNil(t, err)
	StopContainer(ctx, c.Cfg.containername)
	time.Sleep(30 * time.Second)

	StartContainer(ctx, c.Cfg)
	time.Sleep(120 * time.Second)
	var files []fileinfo
	for i := 0; i < 12; i++ {
		fn, fh := makeFile(ctx, t, c, "", 1024*1024*80)
		files = append(files, fileinfo{fn: fn, fh: fh})
	}
	time.Sleep(120 * time.Second)
	for _, fi := range files {
		nhs, _ := readFile(ctx, t, _c, fi.fn, false)
		assert.Equal(t, nhs, fi.fh)
	}
}

func testCleanStore(t *testing.T, c *TestRun) {
	cleanStore(t, c)
}

func testStatFS(t *testing.T, c *TestRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := c.Connection.StatFS(ctx)
	assert.Nil(t, err)
}

func testReconcileCloudMetadata(t *testing.T, c *TestRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var keys []string
	fn, _ := makeFile(ctx, t, c, "", 20480)
	if c.S3 {
		endpoint := "localhost:9000"
		accessKeyID := "MINIO"
		secretAccessKey := "MINIO1234"
		useSSL := false
		minioClient, err := minio.New(endpoint, &minio.Options{
			Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
			Secure: useSSL,
		})
		assert.Nil(t, err)
		objectCh := minioClient.ListObjects(ctx, "pool0", minio.ListObjectsOptions{
			Prefix:    "files",
			Recursive: true,
		})
		for object := range objectCh {
			if object.Err != nil {
				t.Errorf(" error while listing objects %v", object.Err)
				assert.Nil(t, object.Err)
			}
			keys = append(keys, object.Key)
			_ = minioClient.RemoveObject(context.Background(), "pool0", object.Key, minio.RemoveObjectOptions{
				GovernanceBypass: true,
			})
		}
		objectCh = minioClient.ListObjects(ctx, "pool0", minio.ListObjectsOptions{
			Prefix:    "ddb",
			Recursive: true,
		})
		for object := range objectCh {
			if object.Err != nil {
				t.Errorf(" error while listing objects %v", object.Err)
				assert.Nil(t, object.Err)
			}
			keys = append(keys, object.Key)
			_ = minioClient.RemoveObject(context.Background(), "pool0", object.Key, minio.RemoveObjectOptions{
				GovernanceBypass: true,
			})
		}
		c.Connection.ReconcileCloudMetadata(ctx, true)
		for _, key := range keys {
			objInfo, err := minioClient.StatObject(context.Background(), "pool0", key, minio.StatObjectOptions{})
			if err != nil {
				t.Errorf(" error while getting object %k", err)
				assert.Nil(t, err)
			}
			assert.NotNil(t, objInfo)
		}
	}
	err := c.Connection.DeleteFile(ctx, fn)
	assert.Nil(t, err)

}

func testRename(t *testing.T, c *TestRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fn, _ := makeFile(ctx, t, c, "", 1024)
	nfn := string(randBytesMaskImpr(16))

	err := c.Connection.Rename(ctx, fn, nfn)
	assert.Nil(t, err)
	_, err = c.Connection.Stat(ctx, fn)
	assert.NotNil(t, err)
	_, err = c.Connection.Stat(ctx, nfn)
	assert.Nil(t, err)
	err = c.Connection.DeleteFile(ctx, nfn)
	assert.Nil(t, err)
}

func testCopyFile(t *testing.T, c *TestRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fn, hash := makeFile(ctx, t, c, "", 1024)
	nfn := string(randBytesMaskImpr(16))
	_, err := c.Connection.CopyFile(ctx, fn, nfn, false)
	assert.Nil(t, err)
	nhash, err := readFile(ctx, t, c, nfn, false)
	assert.Nil(t, err)
	if err != nil {
		assert.Equal(t, hash, nhash)
		err = c.Connection.DeleteFile(ctx, nfn)
		assert.Nil(t, err)
		err = c.Connection.DeleteFile(ctx, fn)
		assert.Nil(t, err)
	}
}

func testEvents(t *testing.T, c *TestRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fn, hash := makeFile(ctx, t, c, "", 1024)
	nfn := string(randBytesMaskImpr(16))
	evt, err := c.Connection.CopyFile(ctx, fn, nfn, true)
	assert.Nil(t, err)
	assert.NotNil(t, evt)
	if evt != nil {
		_, err = c.Connection.WaitForEvent(ctx, evt.Uuid)
		assert.Nil(t, err)
	}
	nhash, err := readFile(ctx, t, c, nfn, false)
	assert.Nil(t, err)
	if err != nil {
		assert.Equal(t, hash, nhash)
		err = c.Connection.DeleteFile(ctx, nfn)
		assert.Nil(t, err)
		err = c.Connection.DeleteFile(ctx, fn)
		assert.Nil(t, err)
		_, err = c.Connection.GetEvent(ctx, evt.Uuid)
		assert.Nil(t, err)
		_, err = c.Connection.ListEvents(ctx)
		assert.Nil(t, err)
	}
}

func testXAttrs(t *testing.T, c *TestRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fn, _ := makeFile(ctx, t, c, "", 1024)
	_, err := c.Connection.GetXAttrSize(ctx, fn, "key")
	assert.NotNil(t, err)
	err = c.Connection.SetXAttr(ctx, "key", "value", fn)
	assert.Nil(t, err)
	val, err := c.Connection.GetXAttr(ctx, "key", fn)
	assert.Equal(t, val, "value")
	assert.Nil(t, err)
	_, err = c.Connection.GetXAttrSize(ctx, fn, "key")
	assert.Nil(t, err)
	err = c.Connection.RemoveXAttr(ctx, "key", fn)
	assert.Nil(t, err)
	_, err = c.Connection.GetXAttrSize(ctx, fn, "key")
	assert.NotNil(t, err)
	fa := []*spb.FileAttributes{{Key: "key1", Value: "value1"}, {Key: "key2", Value: "value2"}}
	err = c.Connection.SetUserMetaData(ctx, fn, fa)
	assert.Nil(t, err)
	_, fal, err := c.Connection.ListDir(ctx, fn, "", false, int32(1000))
	assert.Nil(t, err)
	for _, attrs := range fal {
		if attrs.FileAttributes[0].Key == "key1" {
			assert.Equal(t, attrs.FileAttributes[0].Value, "value1")
		} else {
			assert.Equal(t, attrs.FileAttributes[0].Key, "key2")
			assert.Equal(t, attrs.FileAttributes[0].Value, "value2")
		}
	}
	err = c.Connection.DeleteFile(ctx, fn)
	assert.Nil(t, err)
}

func testSetUtime(t *testing.T, c *TestRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fn, _ := makeFile(ctx, t, c, "", 1024)
	err := c.Connection.Utime(ctx, fn, int64(0), int64(0))
	assert.Nil(t, err)
	stat, err := c.Connection.GetAttr(ctx, fn)
	assert.Nil(t, err)
	assert.Equal(t, stat.Atime, int64(0))
	assert.Equal(t, stat.Mtim, int64(0))
	err = c.Connection.DeleteFile(ctx, fn)
	assert.Nil(t, err)
}

func testTuncate(t *testing.T, c *TestRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fn, _ := makeFile(ctx, t, c, "", 1024*1024*10)
	err := c.Connection.Truncate(ctx, fn, int64(0))
	assert.Nil(t, err)
	stat, err := c.Connection.GetAttr(ctx, fn)
	assert.Nil(t, err)
	assert.Equal(t, stat.Size, int64(0))
	err = c.Connection.DeleteFile(ctx, fn)
	assert.Nil(t, err)
}

func testWriteLargeBlock(t *testing.T, c *TestRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tMb := int64(1024 * 1024 * 100)
	fMb := 1024 * 5
	fn, _ := makeLargeBlockFile(ctx, t, c, "", tMb, fMb)
	info, _ := c.Connection.Stat(ctx, fn)
	t.Logf("afd %d vfd %d", info.IoMonitor.ActualBytesWritten, info.IoMonitor.VirtualBytesWritten)
	err := c.Connection.DeleteFile(ctx, fn)
	assert.Nil(t, err)
}

func testSymLink(t *testing.T, c *TestRun) {
	if runtime.GOOS != "windows" {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		fn, _ := makeFile(ctx, t, c, "", 1024)
		sfn := string(randBytesMaskImpr(16))
		_, fls, err := c.Connection.ListDir(ctx, "/", "", false, int32(100))
		assert.Nil(t, err)
		err = c.Connection.SymLink(ctx, fn, sfn)
		assert.Nil(t, err)
		_sfn, err := c.Connection.ReadLink(ctx, sfn)
		assert.Nil(t, err)
		assert.Equal(t, fn, _sfn)
		_, err = c.Connection.GetAttr(ctx, sfn)
		assert.Nil(t, err)
		_, nfls, err := c.Connection.ListDir(ctx, "/", "", false, int32(100))
		assert.Equal(t, len(fls), len(nfls)-1)
		assert.Nil(t, err)
		err = c.Connection.Unlink(ctx, sfn)
		assert.Nil(t, err)
		_, err = c.Connection.GetAttr(ctx, sfn)
		assert.NotNil(t, err)
		err = c.Connection.DeleteFile(ctx, fn)
		assert.Nil(t, err)
	}
}

func testSync(t *testing.T, c *TestRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fn, _ := makeFile(ctx, t, c, "", 1024)
	fh, err := c.Connection.Open(ctx, fn, int32(-1))
	assert.Nil(t, err)
	b := randBytesMaskImpr(16)
	err = c.Connection.Write(ctx, fh, b, 0, int32(len(b)))
	assert.Nil(t, err)
	err = c.Connection.Flush(ctx, fn, fh)
	assert.Nil(t, err)
	err = c.Connection.Release(ctx, fh)
	assert.Nil(t, err)
	err = c.Connection.DeleteFile(ctx, fn)
	assert.Nil(t, err)
}

func testMaxAge(t *testing.T, c *TestRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	info, err := c.Connection.DSEInfo(ctx)
	assert.Nil(t, err)
	t.Logf("max age : %d", info.MaxAge)
	err = c.Connection.SetMaxAge(ctx, 1000)
	assert.Nil(t, err)
	info, err = c.Connection.DSEInfo(ctx)
	assert.Nil(t, err)
	assert.Equal(t, int64(1000), info.MaxAge)
	t.Logf("new max age : %d", info.MaxAge)
	fsz := int64(1024 * 1024)
	_nfn, _ := makeFile(ctx, t, c, "", fsz)
	time.Sleep(15 * time.Second)
	info, err = c.Connection.DSEInfo(ctx)
	assert.Nil(t, err)

	sz := info.Entries
	nfn := string(randBytesMaskImpr(16))
	time.Sleep(15 * time.Second)

	_, err = c.Connection.Download(ctx, _nfn, nfn, 1024)
	defer os.Remove(nfn)
	assert.Nil(t, err)
	_, err = c.Connection.Upload(ctx, nfn, nfn, 1024)
	assert.Nil(t, err)
	os.Remove(_nfn)
	time.Sleep(15 * time.Second)
	info, err = c.Connection.DSEInfo(ctx)
	assert.Nil(t, err)
	nsz := info.Entries
	t.Logf("sz = %d nsz =%d", sz, nsz)
	assert.Less(t, sz, nsz)
	err = c.Connection.DeleteFile(ctx, nfn)
	assert.Nil(t, err)
	err = c.Connection.DeleteFile(ctx, _nfn)
	assert.Nil(t, err)
	time.Sleep(15 * time.Second)
	/*
		time.Sleep(15 * time.Second)
		c.Connection.CleanStore(ctx, true, true)
		tm := time.Duration(60 * int(time.Second))
		time.Sleep(tm)
	*/
	info, err = c.Connection.DSEInfo(ctx)
	assert.Nil(t, err)
	fnsz := info.Entries
	t.Logf("sz = %d nsz =%d, fnsz=%d", sz, nsz, fnsz)
	assert.Greater(t, sz, fnsz)
	_nfn, hs := makeFile(ctx, t, c, "", fsz)
	nfn = string(randBytesMaskImpr(16))
	time.Sleep(10 * time.Second)
	c.Connection.CopyFile(ctx, _nfn, nfn, false)
	c.Connection.DeleteFile(ctx, _nfn)
	time.Sleep(15 * time.Second)
	c.Connection.CleanStore(ctx, true, true)
	tm := time.Duration(60 * int(time.Second))
	time.Sleep(tm)
	info, err = c.Connection.DSEInfo(ctx)
	assert.Nil(t, err)
	fnsz = info.Entries
	t.Logf("sz = %d, fnsz=%d", sz, fnsz)
	nhs, err := readFile(ctx, t, c, nfn, true)
	assert.Nil(t, err)
	assert.Equal(t, hs, nhs)
	c.Connection.DeleteFile(ctx, _nfn)
	/*
		time.Sleep(10 * time.Second)
		c.Connection.CleanStore(ctx, true, true)
		tm = time.Duration(60 * int(time.Second))
		time.Sleep(tm)
	*/
	info, err = c.Connection.DSEInfo(ctx)
	assert.Nil(t, err)
	fnsz = info.Entries
	t.Logf("sz = %d, fnsz=%d", sz, fnsz)
	_nfn, _ = makeFile(ctx, t, c, "", 1024*1024)
	nfn = string(randBytesMaskImpr(16))
	os.Remove(nfn)
	time.Sleep(10 * time.Second)
	_, err = c.Connection.Download(ctx, _nfn, nfn, 1024)
	assert.Nil(t, err)
	sz = info.Entries
	for i := 0; i < 10; i++ {
		_, err = c.Connection.Upload(ctx, nfn, fmt.Sprintf("file%d", i), 1024)
		if err != nil {
			t.Logf("upload error %v", err)
		}
		info, err := c.Connection.Stat(ctx, fmt.Sprintf("file%d", i))
		assert.GreaterOrEqual(t, info.IoMonitor.ActualBytesWritten, int64(0))
		assert.Nil(t, err)
		time.Sleep(15 * time.Second)
	}
	time.Sleep(15 * time.Second)

	/*
		c.Connection.DeleteFile(ctx, _nfn)
		time.Sleep(10 * time.Second)
		c.Connection.CleanStore(ctx, true, true)
		tm = time.Duration(60 * int(time.Second))
		time.Sleep(tm)
	*/
	info, _ = c.Connection.DSEInfo(ctx)
	nsz = info.Entries
	t.Logf("sz = %d, nsz=%d", sz, nsz)
	assert.Less(t, sz, nsz)
	for i := 0; i < 10; i++ {
		err = c.Connection.DeleteFile(ctx, fmt.Sprintf("file%d", i))
		assert.Nil(t, err)
	}
	/*

	 */
	info, _ = c.Connection.DSEInfo(ctx)
	sz = info.Entries
	t.Logf("sz = %d, nsz=%d", sz, nsz)
	assert.Less(t, sz, nsz)
	os.Remove(nfn)
	err = c.Connection.SetMaxAge(ctx, -1)
	assert.Nil(t, err)
}

func testCopyExtent(t *testing.T, c *TestRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fn, _ := makeFile(ctx, t, c, "", 1024)
	sfn, _ := makeFile(ctx, t, c, "", 1024)
	fh, err := c.Connection.Open(ctx, fn, int32(-1))
	assert.Nil(t, err)
	sfh, err := c.Connection.Open(ctx, sfn, int32(-1))
	assert.Nil(t, err)
	b := randBytesMaskImpr(16)
	err = c.Connection.Write(ctx, fh, b, 0, int32(len(b)))
	assert.Nil(t, err)
	err = c.Connection.Flush(ctx, fn, fh)
	assert.Nil(t, err)
	err = c.Connection.Release(ctx, fh)
	assert.Nil(t, err)
	fh, err = c.Connection.Open(ctx, fn, int32(-1))
	assert.Nil(t, err)
	_, err = c.Connection.CopyExtent(ctx, fn, sfn, 0, 0, int64(len(b)))
	assert.Nil(t, err)
	err = c.Connection.Flush(ctx, sfn, sfh)
	assert.Nil(t, err)
	err = c.Connection.Release(ctx, sfh)
	assert.Nil(t, err)
	sfh, err = c.Connection.Open(ctx, sfn, int32(-1))
	assert.Nil(t, err)
	nb, err := c.Connection.Read(ctx, sfh, 0, int32(len(b)))
	assert.Nil(t, err)
	assert.Equal(t, nb, b)
	err = c.Connection.Release(ctx, fh)
	assert.Nil(t, err)
	err = c.Connection.Release(ctx, sfh)
	assert.Nil(t, err)
	err = c.Connection.DeleteFile(ctx, fn)
	assert.Nil(t, err)
	err = c.Connection.DeleteFile(ctx, sfn)
	assert.Nil(t, err)
}

func testInfo(t *testing.T, c *TestRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := c.Connection.GetVolumeInfo(ctx)
	assert.Nil(t, err)
	_, err = c.Connection.DSEInfo(ctx)
	assert.Nil(t, err)
	_, err = c.Connection.SystemInfo(ctx)
	assert.Nil(t, err)
}

func testGCSchedule(t *testing.T, c *TestRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gc, err := c.Connection.GetGCSchedule(ctx)
	assert.Nil(t, err)
	t.Logf("GC Sched = %s", gc)
}

func testCache(t *testing.T, c *TestRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err := c.Connection.SetCacheSize(ctx, int64(1)*tb, true)
	assert.NotNil(t, err)
	dse, err := c.Connection.DSEInfo(ctx)
	assert.Nil(t, err)
	assert.Equal(t, int64(10)*gb, dse.MaxCacheSize)
	_, err = c.Connection.SetCacheSize(ctx, int64(5)*gb, true)
	assert.Nil(t, err)
	dse, err = c.Connection.DSEInfo(ctx)
	assert.Nil(t, err)
	assert.Equal(t, int64(5)*gb, dse.MaxCacheSize)
	_, err = c.Connection.SetCacheSize(ctx, int64(10)*gb, true)
	assert.Nil(t, err)
	dse, err = c.Connection.DSEInfo(ctx)
	assert.Nil(t, err)
	assert.Equal(t, int64(10)*gb, dse.MaxCacheSize)
}

func testSetRWSpeed(t *testing.T, c *TestRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := c.Connection.SetReadSpeed(ctx, int32(1000))
	assert.Nil(t, err)
	err = c.Connection.SetWriteSpeed(ctx, int32(2000))
	assert.Nil(t, err)
	dse, err := c.Connection.DSEInfo(ctx)
	assert.Nil(t, err)
	assert.Equal(t, int32(1000), dse.ReadSpeed)
	assert.Equal(t, int32(2000), dse.WriteSpeed)
}

func testSetVolumeSize(t *testing.T, c *TestRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := c.Connection.SetVolumeCapacity(ctx, int64(100)*tb)
	assert.Nil(t, err)
	vol, err := c.Connection.GetVolumeInfo(ctx)
	assert.Nil(t, err)
	assert.Equal(t, int64(100)*tb, vol.Capactity)
}

func cleanStore(t *testing.T, c *TestRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var files []string
	time.Sleep(65 * time.Second)
	c.Connection.CleanStore(ctx, true, true)
	time.Sleep(65 * time.Second)
	for i := 0; i < 12; i++ {
		fn, _ := makeFile(ctx, t, c, "", 1024*1024*10)
		files = append(files, fn)
	}
	_nfn, nh := makeFile(ctx, t, c, "", 1024*1024*1024*10)
	time.Sleep(60 * time.Second)
	info, err := c.Connection.DSEInfo(ctx)
	assert.Nil(t, err)
	sz := info.CurrentSize
	for _, l := range files {
		c.Connection.DeleteFile(ctx, l)
	}
	time.Sleep(65 * time.Second)
	c.Connection.CleanStore(ctx, true, true)
	time.Sleep(65 * time.Second)
	info, err = c.Connection.DSEInfo(ctx)
	assert.Nil(t, err)
	nsz := info.CurrentSize
	assert.Greater(t, sz, nsz)
	nhn, err := readFile(ctx, t, c, _nfn, true)
	assert.Nil(t, err)
	assert.Equal(t, nh, nhn)
	t.Logf("orig = %d new = %d", sz, nsz)
	time.Sleep(65 * time.Second)
	c.Connection.CleanStore(ctx, true, true)
	time.Sleep(65 * time.Second)
}

func testCleanStoreEvent(t *testing.T, c *TestRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fn, fh := makeFile(ctx, t, c, "", 1024*1024*1024*1)
	nfn := string(randBytesMaskImpr(16))
	_, err := c.Connection.Download(ctx, fn, nfn, 1024)
	defer os.Remove(nfn)
	assert.Nil(t, err)
	_, err = c.Connection.Upload(ctx, nfn, nfn, 1024)
	assert.Nil(t, err)
	time.Sleep(60 * time.Second)
	c.Connection.DeleteFile(ctx, fn)
	time.Sleep(65 * time.Second)
	c.Connection.CleanStore(ctx, true, true)
	nhn, err := readFile(ctx, t, c, nfn, true)
	c.Connection.DeleteFile(ctx, nfn)
	assert.Nil(t, err)
	assert.Equal(t, fh, nhn)
	time.Sleep(65 * time.Second)
	c.Connection.CleanStore(ctx, true, true)
	time.Sleep(65 * time.Second)
	info, err := c.Connection.DSEInfo(ctx)
	assert.Nil(t, err)
	elen := int64(1024 * 1024 * 10)
	assert.LessOrEqual(t, info.CompressedSize, elen)
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

func testShutdown(t *testing.T, c *TestRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := c.Connection.DSEInfo(ctx)
	assert.Nil(t, err)
	err = c.Connection.ShutdownVolume(ctx)
	assert.Nil(t, err)
	time.Sleep(20 * time.Second)
	//because the volume is not actually shutdown during debug
	_, err = c.Connection.DSEInfo(ctx)
	assert.Nil(t, err)

}

func testUpload(t *testing.T, c *TestRun) {
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
	wr, err := c.Connection.Upload(ctx, fn, fn, 1024)
	assert.Nil(t, err)
	assert.Equal(t, int64(len(data)), wr)
	nhs, err := readFile(ctx, t, c, fn, false)
	assert.Nil(t, err)
	assert.Equal(t, bs, nhs)
	nfn := string(randBytesMaskImpr(16))
	rr, err := c.Connection.Download(ctx, fn, nfn, 1024)
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
	c.Connection.DeleteFile(ctx, fn)
}

func testCloudAutoDownload(t *testing.T, c *TestRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fsz := int64(1024 * 1024 * 10)
	fn, hs := makeFile(ctx, t, c, "", fsz)
	fi, err := c.Connection.Stat(ctx, fn)
	assert.Nil(t, err)
	fl := fi.Size
	cmd := "chattr -i /opt/sdfs/volumes/.pool0/files/" + fn
	cr, err := DockerExec(ctx, c.Cfg.containername, strings.Split(cmd, " "))
	//t.Logf("chattr cmd output %s", cr.outBuffer.String())
	//t.Logf("chattr err cmd output %s", cr.errBuffer.String())
	assert.Nil(t, err)
	assert.Equal(t, 0, cr.ExitCode)
	cmd = "rm /opt/sdfs/volumes/.pool0/files/" + fn
	cr, err = DockerExec(ctx, c.Cfg.containername, strings.Split(cmd, " "))
	//t.Logf("rm cmd output %s", cr.outBuffer.String())
	//t.Logf("rm err cmd output %s", cr.errBuffer.String())
	assert.Nil(t, err)
	assert.Equal(t, 0, cr.ExitCode)
	_, fls, err := c.Connection.ListDir(ctx, fn, "", false, int32(10))
	assert.Nil(t, err)
	assert.Equal(t, fl, fls[0].Size)
	chs, err := readFile(ctx, t, c, fn, true)
	assert.Nil(t, err)
	assert.Equal(t, chs, hs)
	fn, hs = makeFile(ctx, t, c, "", fsz)
	fi, err = c.Connection.Stat(ctx, fn)
	assert.Nil(t, err)
	ddbpth := fmt.Sprintf("%s/%s/", fi.MapGuid[:2], fi.MapGuid)
	cmd = "rm -rf /opt/sdfs/volumes/.pool0/ddb/" + ddbpth
	cr, err = DockerExec(ctx, c.Cfg.containername, strings.Split(cmd, " "))
	//t.Logf("rm cmd output %s", cr.outBuffer.String())
	//t.Logf("rm err cmd output %s", cr.errBuffer.String())
	assert.Nil(t, err)
	assert.Equal(t, 0, cr.ExitCode)
	cmd = "ls -lah /opt/sdfs/volumes/.pool0/ddb/" + ddbpth
	cr, err = DockerExec(ctx, c.Cfg.containername, strings.Split(cmd, " "))
	//t.Logf("rm cmd output %s", cr.outBuffer.String())
	//t.Logf("rm err cmd output %s", cr.errBuffer.String())
	assert.Nil(t, err)
	assert.Equal(t, 2, cr.ExitCode)
	chs, err = readFile(ctx, t, c, fn, true)
	assert.Nil(t, err)
	assert.Equal(t, chs, hs)
	fn, hs = makeFile(ctx, t, c, "", fsz)
	fi, err = c.Connection.Stat(ctx, fn)
	assert.Nil(t, err)
	ddbpth = fmt.Sprintf("%s/%s/", fi.MapGuid[:2], fi.MapGuid)
	cmd = "rm -rf /opt/sdfs/volumes/.pool0/ddb/" + ddbpth
	cr, err = DockerExec(ctx, c.Cfg.containername, strings.Split(cmd, " "))
	assert.Nil(t, err)
	assert.Equal(t, 0, cr.ExitCode)
	cmd = "ls -lah /opt/sdfs/volumes/.pool0/ddb/" + ddbpth
	cr, err = DockerExec(ctx, c.Cfg.containername, strings.Split(cmd, " "))
	assert.Nil(t, err)
	assert.Equal(t, 2, cr.ExitCode)
	cmd = "chattr -i /opt/sdfs/volumes/.pool0/files/" + fn
	cr, err = DockerExec(ctx, c.Cfg.containername, strings.Split(cmd, " "))
	assert.Nil(t, err)
	assert.Equal(t, 0, cr.ExitCode)
	cmd = "rm /opt/sdfs/volumes/.pool0/files/" + fn
	cr, err = DockerExec(ctx, c.Cfg.containername, strings.Split(cmd, " "))
	assert.Nil(t, err)
	assert.Equal(t, 0, cr.ExitCode)
	chs, err = readFile(ctx, t, c, fn, true)
	assert.Nil(t, err)
	assert.Equal(t, chs, hs)

}

func testCloudRecover(t *testing.T, c *TestRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type fhes struct {
		fn string
		fh []byte
	}
	_, err := c.Connection.SetCacheSize(ctx, int64(1)*gb, true)
	assert.Nil(t, err)
	dse, err := c.Connection.DSEInfo(ctx)
	assert.Nil(t, err)
	assert.Equal(t, int64(1)*gb, dse.MaxCacheSize)
	var fls []fhes
	fsz := int64(1024 * 1024 * 1024 * 3)
	for i := 0; i < 2; i++ {
		fn, hs := makeFile(ctx, t, c, "", fsz)
		fls = append(fls, fhes{fn: fn, fh: hs})
	}
	cmd := "rm -rf /opt/sdfs/volumes/.pool0/"
	cr, err := DockerExec(ctx, c.Cfg.containername, strings.Split(cmd, " "))
	if err != nil {
		log.Warnf("rm failed sterr = %s\n", cr.errBuffer.String())
		log.Warnf("rm failed stout = %s\n", cr.outBuffer.String())

	}
	assert.Nil(t, err)
	assert.Equal(t, 0, cr.ExitCode)
	uf := "resources/docker_run_reload.sh"
	df := "/usr/share/sdfs/docker_run.sh"
	err = copyToContainer(ctx, c.Cfg.containername, uf, df)
	assert.Nil(t, err)
	cmd = "chmod 777 /usr/share/sdfs/docker_run.sh"
	cr, err = DockerExec(ctx, c.Cfg.containername, strings.Split(cmd, " "))
	assert.Nil(t, err)
	err = RestartContainer(ctx, c.Cfg)
	assert.Nil(t, err)
	time.Sleep(240 * time.Second)
	for _, fs := range fls {
		dh, err := readFile(ctx, t, c, fs.fn, true)
		assert.Nil(t, err)
		assert.Equal(t, dh, fs.fh)
	}
	_, err = c.Connection.SetCacheSize(ctx, int64(10)*gb, true)
	assert.Nil(t, err)
	dse, err = c.Connection.DSEInfo(ctx)
	assert.Nil(t, err)
	assert.Equal(t, int64(10)*gb, dse.MaxCacheSize)
}

func testCloudSync(t *testing.T, c *TestRun) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := c.Cfg
	cfg.containername = fmt.Sprintf("second-%s", c.Cfg.containername)
	cfg.containerPort = "6442"
	cfg.disableMinio = true
	cfg.hostPort = fmt.Sprintf("2%s", c.Cfg.hostPort)
	var tr *TestRun
	var err error
	if c.S3 {
		tr, err = CreateS3Setup(ctx, cfg)
		if err != nil {
			t.Errorf("error creating container %v", err)
		}
		tr.S3 = true
		tr.Name = "second S3"
		tr.CloudVol = true
	} else {
		tr, err = CreateAzureSetup(ctx, cfg)
		if err != nil {
			t.Errorf("error creating container %v", err)
		}
		tr.S3 = true
		tr.Name = "second Azure"
		tr.CloudVol = true
	}
	tr.Clientsidededupe = c.Clientsidededupe
	fe := paip.ForwardEntry{
		Address:       tr.Url,
		Dedupe:        false,
		DedupeThreads: 1,
		DedupeBuffer:  4,
	}
	tr.Fe = &fe
	portR := &paip.PortRedirectors{}
	maddress := []*TestRun{c, tr}

	for _, lc := range maddress {
		portR.ForwardEntrys = append(portR.ForwardEntrys, *lc.Fe)
	}
	connection, err := api.NewConnection(tr.Url, false, true, -1, 0, 0)
	retrys := 0
	for err != nil {
		t.Logf("retries = %d\n", retrys)
		time.Sleep(20 * time.Second)
		connection, err = api.NewConnection(tr.Url, false, true, -1, 0, 0)
		if retrys > 10 {
			t.Errorf("SDFS Server connection timed out %s\n", tr.Url)
			os.Exit(-1)
		} else {
			retrys++
		}
	}
	assert.Nil(t, err)

	tr.Volume = connection.Volumeid
	t.Logf("connected to volume = %d\n", connection.Volumeid)
	b, err := json.Marshal(*portR)
	assert.Nil(t, err)
	err = ioutil.WriteFile("testpf.json", b, 0644)
	assert.Nil(t, err)

	_, err = c.Connection.ReloadProxyConfig(ctx)
	assert.Nil(t, err)
	connection = Connect(t, false, connection.Volumeid)
	assert.NotNil(t, connection)
	tr.Connection = connection
	defer connection.CloseConnection(ctx)
	vis, err := connection.GetProxyVolumes(ctx)
	if err != nil {
		t.Logf("error %v", err)
	}
	assert.Nil(t, err)
	assert.Equal(t, len(portR.ForwardEntrys), len(vis.VolumeInfoResponse))
	tr.Connection = Connect(t, tr.Clientsidededupe, tr.Volume)
	assert.NotNil(t, tr.Connection)

	info, err := c.Connection.GetVolumeInfo(ctx)
	assert.Nil(t, err)
	time.Sleep(35 * time.Second)
	cinfo, err := tr.Connection.GetConnectedVolumes(ctx)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(cinfo))
	fn, sh := makeGenericFile(ctx, t, c.Connection, "", 1024)
	fi, err := c.Connection.Stat(ctx, fn)
	assert.Nil(t, err)
	_, err = tr.Connection.SyncFromCloudVolume(ctx, info.SerialNumber, true, true)
	assert.Nil(t, err)
	nfi, err := tr.Connection.Stat(ctx, fn)
	assert.Nil(t, err)
	dh, err := readFile(ctx, t, tr, fn, false)
	assert.Nil(t, err)
	assert.Equal(t, dh, sh)
	assert.Equal(t, fi.Mode, nfi.Mode)
	err = c.Connection.DeleteFile(ctx, fn)
	assert.Nil(t, err)
	time.Sleep(15 * time.Second)
	c.Connection.CleanStore(ctx, true, true)
	time.Sleep(65 * time.Second)
	dh, err = readFile(ctx, t, tr, fn, false)
	assert.Nil(t, err)
	assert.Equal(t, dh, sh)
	fn, sh = makeGenericFile(ctx, t, c.Connection, "", 1024)
	_, err = c.Connection.Stat(ctx, fn)
	assert.Nil(t, err)
	_, err = tr.Connection.GetCloudFile(ctx, fn, fmt.Sprintf("nf%s", fn), true, true)
	assert.Nil(t, err)
	time.Sleep(35 * time.Second)
	dh, err = readFile(ctx, t, tr, fn, false)
	assert.Nil(t, err)
	assert.Equal(t, dh, sh)
	assert.Equal(t, fi.Mode, nfi.Mode)
	err = c.Connection.DeleteFile(ctx, fn)
	assert.Nil(t, err)
	time.Sleep(15 * time.Second)
	c.Connection.CleanStore(ctx, true, true)
	time.Sleep(65 * time.Second)
	dh, err = readFile(ctx, t, tr, fn, false)
	assert.Nil(t, err)
	assert.Equal(t, dh, sh)
	StopAndRemoveContainer(ctx, tr.Cfg.containername)
	portR = &paip.PortRedirectors{}
	maddress = []*TestRun{c}

	for _, lc := range maddress {
		portR.ForwardEntrys = append(portR.ForwardEntrys, *lc.Fe)
	}
	err = ioutil.WriteFile("testpf.json", b, 0644)
	assert.Nil(t, err)
	_, err = c.Connection.ReloadProxyConfig(ctx)
	assert.Nil(t, err)
	vis, err = c.Connection.GetProxyVolumes(ctx)
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
	var TestRuns []*TestRun
	for i := 2; i < 4; i++ {
		cfg := &ContainerConfig{containername: fmt.Sprintf("block-644%d", port), hostPort: fmt.Sprintf("644%d", port)}
		tst, err := CreateBlockSetup(ctx, cfg)
		assert.Nil(t, err)
		port++
		TestRuns = append(TestRuns, tst)
	}
	StartProxyVolume(TestRuns)
	var volumeIds []int64
	for _, c := range TestRuns {
		volumeIds = append(volumeIds, c.Volume)
	}

	connection := Connect(t, false, -1)
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
	for _, c := range TestRuns {
		StopAndRemoveContainer(ctx, c.Cfg.containername)
	}
}

func TestReloadProxyVolume(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var TestRuns []*TestRun
	for i := 2; i < 5; i++ {
		cfg := &ContainerConfig{containername: fmt.Sprintf("block-644%d", i), hostPort: fmt.Sprintf("644%d", i)}
		tst, err := CreateBlockSetup(ctx, cfg)
		assert.Nil(t, err)
		TestRuns = append(TestRuns, tst)
	}
	StartProxyVolume(TestRuns[1:])
	var volumeIds []int64
	for _, c := range TestRuns[1:] {
		t.Logf("tr serial = %d", c.Volume)
		volumeIds = append(volumeIds, c.Volume)
	}
	connection := Connect(t, false, -1)
	vis, err := connection.GetProxyVolumes(ctx)
	assert.Nil(t, err)
	var vids []int64
	for _, vi := range vis.VolumeInfoResponse {
		vids = append(vids, vi.SerialNumber)
		t.Logf("serial = %d", vi.SerialNumber)
	}
	assert.ElementsMatch(t, vids, volumeIds)
	//Get the volume id for the first volume
	cc, err := api.NewConnection(TestRuns[0].Url, false, true, -1, 0, 0)
	retrys := 0
	for err != nil {
		log.Printf("retries = %d", retrys)
		time.Sleep(20 * time.Second)
		cc, err = api.NewConnection(TestRuns[0].Url, false, true, -1, 0, 0)
		if retrys > 10 {
			fmt.Printf("SDFS Server connection timed out %s\n", TestRuns[0].Url)
			os.Exit(-1)
		} else {
			retrys++
		}
	}
	assert.Nil(t, err)
	if err != nil {
		fmt.Printf("Unable to create connection %v", err)
	}
	log.Printf("connected to volume = %d for %s", cc.Volumeid, TestRuns[0].Cfg.containername)
	TestRuns[0].Volume = cc.Volumeid
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

	for _, c := range TestRuns {
		volumeIds = append(volumeIds, c.Volume)
		t.Logf("tr serial = %d", c.Volume)
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

	for _, c := range TestRuns[:len(TestRuns)-1] {
		volumeIds = append(volumeIds, c.Volume)
		t.Logf("tr serial = %d", c.Volume)
	}
	for _, vi := range vis.VolumeInfoResponse {
		vids = append(vids, vi.SerialNumber)
		t.Logf("serial = %d", vi.SerialNumber)
	}
	assert.ElementsMatch(t, vids, volumeIds)
	//Shut everything down
	paip.StopServer()
	for _, c := range TestRuns {
		StopAndRemoveContainer(ctx, c.Cfg.containername)
	}

}

func StartProxyVolume(tr []*TestRun) {
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
		fe := paip.ForwardEntry{Address: m.Url}
		portR.ForwardEntrys = append(portR.ForwardEntrys, fe)
		connection, err := api.NewConnection(m.Url, false, true, -1, 0, 0)
		retrys := 0
		for err != nil {
			log.Debugf("retries = %d", retrys)
			time.Sleep(20 * time.Second)
			connection, err = api.NewConnection(m.Url, false, true, -1, 0, 0)
			if retrys > 10 {
				fmt.Printf("SDFS Server connection timed out %s\n", m.Url)
				os.Exit(-1)
			} else {
				retrys++
			}
		}
		if err != nil {
			fmt.Printf("Unable to create connection %v", err)
		}
		log.Debugf("connected to volume = %d for %s", connection.Volumeid, m.Cfg.containername)
		m.Volume = connection.Volumeid
		cmp[connection.Volumeid] = connection.Clnt
		fe = paip.ForwardEntry{
			Address:       m.Url,
			Dedupe:        false,
			DedupeThreads: 1,
			DedupeBuffer:  4,
		}
		dd[connection.Volumeid] = fe
		m.Fe = &fe
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

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")
var memprofile = flag.String("memprofile", "", "write memory profile to `file`")

func TestMain(m *testing.M) {
	go func() {
		log.Println(http.ListenAndServe(":8081", nil))
	}()
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	// ... rest of the program ...

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		runtime.GC()    // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}
	rand.Seed(time.Now().UTC().UnixNano())
	code := m.Run()
	fmt.Printf("Testing Return code is %d\n", code)
	os.Exit(code)
}

func Connect(t *testing.T, dedupe bool, volumeid int64) *api.SdfsConnection {

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
	retrys := 0
	for err != nil {
		t.Logf("retries = %d\n", retrys)
		time.Sleep(20 * time.Second)
		connection, err = api.NewConnection(address, dedupe, true, volumeid, 40000, 60)
		if retrys > 10 {
			t.Errorf("SDFS Server connection timed out %s\n", address)
			os.Exit(-1)
		} else {
			retrys++
		}
	}
	assert.Nil(t, err)
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

func BConnect(b *testing.B, c *TestRun) *api.SdfsConnection {
	//api.DisableTrust = true
	api.Debug = false
	api.UserName = "admin"
	api.Password = "admin"
	api.Mtls = false
	var address = c.Url
	vid := int64(-1)
	if !c.Direct {
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
		vid = c.Volume
	}
	log.Debugf("Connecting to %s dedupe is %v", address, c.Clientsidededupe)

	connection, err := api.NewConnection(address, c.Clientsidededupe, true, vid, 40000, 60)
	retrys := 0
	for err != nil {
		log.Debugf("retries = %d\n", retrys)
		time.Sleep(20 * time.Second)
		connection, err = api.NewConnection(address, c.Clientsidededupe, true, vid, 40000, 60)
		if retrys > 10 {
			log.Warnf("SDFS Server connection timed out %s\n", address)
		} else {
			retrys++
		}
	}

	log.Debugf("Connection state %s", connection.Clnt.GetState())
	if vid != -1 {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		volinfo, err := connection.GetVolumeInfo(ctx)
		if err != nil {

			b.Errorf("Unable to get volume info for %s error: %v\n", address, err)
			return nil
		}
		if volinfo.SerialNumber != c.Volume {
			b.Errorf("Volume serial numbers don't match expected %d got %d\n", c.Volume, volinfo.SerialNumber)
			return nil
		}
	}
	return connection
}

func Dconnect(t *testing.T, c *TestRun) *api.SdfsConnection {

	//api.DisableTrust = true
	api.Debug = false
	api.UserName = "admin"
	api.Password = "admin"
	api.Mtls = false
	var address = c.Url
	vid := int64(-1)
	if !c.Direct {
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
		vid = c.Volume
	}
	t.Logf("Connecting to %s dedupe is %v", address, c.Clientsidededupe)

	connection, err := api.NewConnection(address, c.Clientsidededupe, true, vid, 40000, 60)
	retrys := 0
	for err != nil {
		log.Warnf("retries = %d\n", retrys)
		time.Sleep(20 * time.Second)
		connection, err = api.NewConnection(address, c.Clientsidededupe, true, vid, 40000, 60)
		if retrys > 10 {
			log.Warnf("SDFS Server connection timed out %s\n", address)
		} else {
			retrys++
		}
	}
	assert.Nil(t, err)
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
		assert.Equal(t, volinfo.SerialNumber, c.Volume)
		if volinfo.SerialNumber != c.Volume {
			t.Errorf("Volume serial numbers don't match expected %d got %d\n", c.Volume, volinfo.SerialNumber)
			return nil
		}
	}
	return connection
}
