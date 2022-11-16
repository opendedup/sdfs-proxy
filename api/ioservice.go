package api

import (
	"context"
	"fmt"
	"io"
	"os"
	"runtime"

	log "github.com/sirupsen/logrus"

	"github.com/opendedup/sdfs-client-go/dedupe"
	spb "github.com/opendedup/sdfs-client-go/sdfs"
	pool "github.com/processout/grpc-go-pool"
	"google.golang.org/grpc"
)

type FileIOProxy struct {
	spb.UnimplementedFileIOServiceServer
	fc            map[int64]spb.FileIOServiceClient
	dfc           int64
	proxy         bool
	dedupe        map[int64]*dedupe.DedupeEngine
	dedupeEnabled map[int64]ForwardEntry
}

func (s *FileIOProxy) GetXAttrSize(ctx context.Context, req *spb.GetXAttrSizeRequest) (*spb.GetXAttrSizeResponse, error) {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID

	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dfc
	}
	if val, ok := s.fc[volid]; ok {
		return val.GetXAttrSize(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}
}

func (s *FileIOProxy) Fsync(ctx context.Context, req *spb.FsyncRequest) (*spb.FsyncResponse, error) {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID

	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dfc
	}
	if s.dedupeEnabled[volid].Dedupe {
		s.dedupe[volid].SyncFile(req.Path, volid)
	}
	if val, ok := s.fc[volid]; ok {
		return val.Fsync(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}
}

func (s *FileIOProxy) SetXAttr(ctx context.Context, req *spb.SetXAttrRequest) (*spb.SetXAttrResponse, error) {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID

	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dfc
	}
	if val, ok := s.fc[volid]; ok {
		return val.SetXAttr(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}

func (s *FileIOProxy) RemoveXAttr(ctx context.Context, req *spb.RemoveXAttrRequest) (*spb.RemoveXAttrResponse, error) {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID

	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dfc
	}
	if val, ok := s.fc[volid]; ok {
		return val.RemoveXAttr(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}
}

func (s *FileIOProxy) GetXAttr(ctx context.Context, req *spb.GetXAttrRequest) (*spb.GetXAttrResponse, error) {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID

	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dfc
	}
	if val, ok := s.fc[volid]; ok {
		return val.GetXAttr(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}

func (s *FileIOProxy) Utime(ctx context.Context, req *spb.UtimeRequest) (*spb.UtimeResponse, error) {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID

	if volid == 0 || volid == -1 {
		volid = s.dfc
	}
	if val, ok := s.fc[volid]; ok {
		return val.Utime(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}
}

func (s *FileIOProxy) Truncate(ctx context.Context, req *spb.TruncateRequest) (*spb.TruncateResponse, error) {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID
	if volid == 0 || volid == -1 {
		volid = s.dfc
	}
	if val, ok := s.fc[volid]; ok {
		if dval, ok := s.dedupe[volid]; ok {
			dval.SyncFile(req.Path, req.PvolumeID)
		}

		return val.Truncate(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}
func (s *FileIOProxy) SymLink(ctx context.Context, req *spb.SymLinkRequest) (*spb.SymLinkResponse, error) {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID

	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dfc
	}
	if val, ok := s.fc[volid]; ok {
		return val.SymLink(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}
func (s *FileIOProxy) GetAttr(ctx context.Context, req *spb.StatRequest) (*spb.StatResponse, error) {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID

	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dfc
	}
	if s.dedupeEnabled[volid].Dedupe {
		s.dedupe[volid].SyncFile(req.Path, volid)
	}
	if val, ok := s.fc[volid]; ok {
		return val.GetAttr(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}

func (s *FileIOProxy) ReadLink(ctx context.Context, req *spb.LinkRequest) (*spb.LinkResponse, error) {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID

	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dfc
	}
	if val, ok := s.fc[volid]; ok {
		return val.ReadLink(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}
}

func (s *FileIOProxy) Flush(ctx context.Context, req *spb.FlushRequest) (*spb.FlushResponse, error) {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID

	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dfc
	}
	if s.dedupeEnabled[volid].Dedupe {
		err := s.dedupe[volid].Sync(req.Fd, volid)
		if err != nil {
			log.Printf("unable to Sync : %v\n", err)
			return nil, err
		}
	}
	if val, ok := s.fc[volid]; ok {
		return val.Flush(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}
func (s *FileIOProxy) Chown(ctx context.Context, req *spb.ChownRequest) (*spb.ChownResponse, error) {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID

	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dfc
	}
	if val, ok := s.fc[volid]; ok {
		return val.Chown(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}

func (s *FileIOProxy) Chmod(ctx context.Context, req *spb.ChmodRequest) (*spb.ChmodResponse, error) {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID

	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dfc
	}
	if val, ok := s.fc[volid]; ok {
		return val.Chmod(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}

func (s *FileIOProxy) MkDir(ctx context.Context, req *spb.MkDirRequest) (*spb.MkDirResponse, error) {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID

	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dfc
	}
	if val, ok := s.fc[volid]; ok {
		return val.MkDir(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}

func (s *FileIOProxy) RmDir(ctx context.Context, req *spb.RmDirRequest) (*spb.RmDirResponse, error) {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID

	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dfc
	}
	if val, ok := s.fc[volid]; ok {
		return val.RmDir(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}
func (s *FileIOProxy) Unlink(ctx context.Context, req *spb.UnlinkRequest) (*spb.UnlinkResponse, error) {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID

	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dfc
	}
	if val, ok := s.fc[volid]; ok {
		return val.Unlink(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}

func (s *FileIOProxy) Write(ctx context.Context, req *spb.DataWriteRequest) (*spb.DataWriteResponse, error) {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID

	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dfc
	}

	if val, ok := s.fc[volid]; ok {
		if dval, ok := s.dedupe[volid]; ok {
			if req.Compressed {
				out, err := dedupe.DecompressData(req.Data, req.Len)
				if err != nil {
					log.Error(err)
					return nil, err
				}
				err = dval.Write(req.FileHandle, req.Start, out, req.Len, req.PvolumeID)
				if err != nil {
					log.Errorf("error writing %v", err)
					return nil, err
				} else {
					return &spb.DataWriteResponse{}, nil
				}
			} else {
				err := dval.Write(req.FileHandle, req.Start, req.Data, req.Len, req.PvolumeID)
				if err != nil {
					log.Errorf("error writing %v", err)
					return nil, err
				} else {
					return &spb.DataWriteResponse{}, nil
				}
			}
		}
		if len(req.Data) > 10 && !req.Compressed && s.dedupeEnabled[volid].CompressData {
			buf, err := dedupe.CompressData(req.Data)
			if err != nil {
				log.Error(err)
				return nil, err
			}
			req.Data = buf
			req.Compressed = true
		}
		return val.Write(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}
}

func (s *FileIOProxy) Read(ctx context.Context, req *spb.DataReadRequest) (*spb.DataReadResponse, error) {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID
	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dfc
	}
	if val, ok := s.fc[volid]; ok {
		if dval, ok := s.dedupe[volid]; ok {
			dval.Sync(req.FileHandle, req.PvolumeID)
		}
		return val.Read(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}
}

func (s *FileIOProxy) Release(ctx context.Context, req *spb.FileCloseRequest) (*spb.FileCloseResponse, error) {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID

	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dfc
	}
	if val, ok := s.fc[volid]; ok {
		if dval, ok := s.dedupe[volid]; ok {
			err := dval.Close(req.FileHandle, req.PvolumeID)
			if err != nil {
				log.Printf("unable to close : %v\n", err)
				return nil, err
			}
		}
		return val.Release(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}
func (s *FileIOProxy) Mknod(ctx context.Context, req *spb.MkNodRequest) (*spb.MkNodResponse, error) {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID

	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dfc
	}
	if val, ok := s.fc[volid]; ok {
		mknodr, err := val.Mknod(ctx, req)
		if err != nil {
			log.Errorf("unable to mkdnod %d, %v", volid, err)
		}
		return mknodr, err
	} else {
		log.Errorf("unable to find volume %d", volid)
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}
func (s *FileIOProxy) Open(ctx context.Context, req *spb.FileOpenRequest) (*spb.FileOpenResponse, error) {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID

	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dfc
	}
	if val, ok := s.fc[volid]; ok {
		rsp, err := val.Open(ctx, req)
		if err != nil {
			return rsp, err
		} else if rsp.ErrorCode > 0 {
			return rsp, err
		}
		if dval, ok := s.dedupe[volid]; ok {
			err = dval.Open(req.Path, rsp.FileHandle, req.PvolumeID)
			if err != nil {
				log.Errorf("unable to open %v", err)
				return nil, err
			}
		}
		return rsp, nil
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}

func (s *FileIOProxy) GetaAllFileInfo(req *spb.FileInfoRequest, stream spb.FileIOService_GetaAllFileInfoServer) error {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID

	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dfc
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if val, ok := s.fc[volid]; ok {
		fi, err := val.GetaAllFileInfo(ctx, req)
		if err != nil {
			return err
		}
		for {
			fl, err := fi.Recv()
			log.Infof("Listing %v", fl)
			if err == io.EOF {
				// we've reached the end of the stream
				break
			}
			if err != nil {
				return err
			}
			if err := stream.Send(fl); err != nil {
				return err
			}
		}
	} else {
		return fmt.Errorf("unable to find volume %d", volid)
	}
	return nil
}

func (s *FileIOProxy) GetFileInfo(ctx context.Context, req *spb.FileInfoRequest) (*spb.FileMessageResponse, error) {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID

	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dfc
	}
	if val, ok := s.fc[volid]; ok {
		if dval, ok := s.dedupe[volid]; ok {
			err := dval.SyncFile(req.FileName, req.PvolumeID)
			if err != nil {
				log.Printf("unable to syncfile : %v\n", err)
				return nil, err
			}
		}
		return val.GetFileInfo(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}
func (s *FileIOProxy) CreateCopy(ctx context.Context, req *spb.FileSnapshotRequest) (*spb.FileSnapshotResponse, error) {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID

	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dfc
	}
	if val, ok := s.fc[volid]; ok {
		if dval, ok := s.dedupe[volid]; ok {
			dval.SyncFile(req.Src, req.PvolumeID)
			dval.SyncFile(req.Dest, req.PvolumeID)
		}
		return val.CreateCopy(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}
func (s *FileIOProxy) FileExists(ctx context.Context, req *spb.FileExistsRequest) (*spb.FileExistsResponse, error) {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID

	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dfc
	}
	if val, ok := s.fc[volid]; ok {
		return val.FileExists(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}
func (s *FileIOProxy) MkDirAll(ctx context.Context, req *spb.MkDirRequest) (*spb.MkDirResponse, error) {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID

	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dfc
	}
	if val, ok := s.fc[volid]; ok {
		return val.MkDirAll(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}
func (s *FileIOProxy) Stat(ctx context.Context, req *spb.FileInfoRequest) (*spb.FileMessageResponse, error) {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID

	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dfc
	}
	if val, ok := s.fc[volid]; ok {
		if dval, ok := s.dedupe[volid]; ok {
			dval.SyncFile(req.FileName, volid)
		}
		return val.Stat(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}
func (s *FileIOProxy) Rename(ctx context.Context, req *spb.FileRenameRequest) (*spb.FileRenameResponse, error) {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID

	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dfc
	}
	if val, ok := s.fc[volid]; ok {
		if dval, ok := s.dedupe[volid]; ok {
			dval.CloseFile(req.Src, req.PvolumeID)
			dval.CloseFile(req.Dest, req.PvolumeID)
		}
		return val.Rename(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}
func (s *FileIOProxy) CopyExtent(ctx context.Context, req *spb.CopyExtentRequest) (*spb.CopyExtentResponse, error) {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID

	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dfc
	}
	if val, ok := s.fc[volid]; ok {
		if dval, ok := s.dedupe[volid]; ok {
			dval.SyncFile(req.SrcFile, req.PvolumeID)
			dval.SyncFile(req.DstFile, req.PvolumeID)
		}
		return val.CopyExtent(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}
func (s *FileIOProxy) SetUserMetaData(ctx context.Context, req *spb.SetUserMetaDataRequest) (*spb.SetUserMetaDataResponse, error) {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID

	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dfc
	}
	if val, ok := s.fc[volid]; ok {
		return val.SetUserMetaData(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}
func (s *FileIOProxy) GetCloudFile(ctx context.Context, req *spb.GetCloudFileRequest) (*spb.GetCloudFileResponse, error) {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID

	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dfc
	}
	if val, ok := s.fc[volid]; ok {
		return val.GetCloudFile(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}
func (s *FileIOProxy) GetCloudMetaFile(ctx context.Context, req *spb.GetCloudFileRequest) (*spb.GetCloudFileResponse, error) {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID

	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dfc
	}
	if val, ok := s.fc[volid]; ok {
		return val.GetCloudFile(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}
func (s *FileIOProxy) StatFS(ctx context.Context, req *spb.StatFSRequest) (*spb.StatFSResponse, error) {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID

	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dfc
	}
	if val, ok := s.fc[volid]; ok {
		return val.StatFS(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}

func (s *FileIOProxy) ReloadVolumeMap(clnts map[int64]*grpc.ClientConn, pclnts map[int64]*pool.Pool, dedupeEnabled map[int64]ForwardEntry, debug bool) error {
	log.Debug("in")
	defer log.Debug("out")
	fcm := make(map[int64]spb.FileIOServiceClient)
	dd := make(map[int64]*dedupe.DedupeEngine)
	var defaultVolume int64
	for indx, clnt := range clnts {
		vc := spb.NewFileIOServiceClient(clnt)
		fcm[indx] = vc
		if dedupeEnabled[indx].Dedupe {
			ctx, cancel := context.WithCancel(context.Background())
			p := pclnts[indx]
			defer cancel()
			de, err := dedupe.NewDedupeEngine(ctx, p, 4, 8, debug, dedupeEnabled[indx].CompressData, indx, dedupeEnabled[indx].CacheSize, dedupeEnabled[indx].CacheAge)
			if err != nil {
				log.Printf("error initializing dedupe connection: %v\n", err)
				return err
			}
			dd[indx] = de

		}
		defaultVolume = indx
	}
	s.dedupeEnabled = dedupeEnabled
	s.dfc = defaultVolume
	s.dedupe = dd
	s.fc = fcm
	return nil
}

func (s *FileIOProxy) SetRetrievalTier(ctx context.Context, req *spb.SetRetrievalTierRequest) (*spb.SetRetrievalTierResponse, error) {
	log.Debug("in")
	defer log.Debug("out")

	volid := req.PvolumeID

	if s.proxy || volid == 0 || volid == -1 {

		volid = s.dfc

	}

	if val, ok := s.fc[volid]; ok {

		return val.SetRetrievalTier(ctx, req)

	} else {

		return nil, fmt.Errorf("unable to find volume %d", volid)

	}

}

func (s *FileIOProxy) GetRetrievalTier(ctx context.Context, req *spb.GetRetrievalTierRequest) (*spb.GetRetrievalTierResponse, error) {
	log.Debug("in")
	defer log.Debug("out")

	volid := req.PvolumeID

	if s.proxy || volid == 0 || volid == -1 {

		volid = s.dfc

	}

	if val, ok := s.fc[volid]; ok {

		return val.GetRetrievalTier(ctx, req)

	} else {

		return nil, fmt.Errorf("unable to find volume %d", volid)

	}

}

func NewFileIOProxy(clnts map[int64]*grpc.ClientConn, pclnts map[int64]*pool.Pool, dedupeEnabled map[int64]ForwardEntry, proxy, debug bool) (*FileIOProxy, error) {
	fcm := make(map[int64]spb.FileIOServiceClient)
	dd := make(map[int64]*dedupe.DedupeEngine)
	if debug {
		log.SetLevel(log.DebugLevel)
	}
	if runtime.GOOS == "windows" {
		lpth := "c:/temp/sdfs/"
		f, err := os.OpenFile(fmt.Sprintf("%s/%s", lpth, "sdfs-proxy.log"), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			fmt.Println("Failed to create logfile" + fmt.Sprintf("%s/%s", lpth, "sdfs-proxy.log"))
			panic(err)
		}
		log.SetOutput(f)
	} else {
		log.SetOutput(os.Stdout)
	}
	log.SetReportCaller(true)
	var defaultVolume int64
	for indx, clnt := range clnts {
		vc := spb.NewFileIOServiceClient(clnt)
		fcm[indx] = vc
		if dedupeEnabled[indx].Dedupe {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			de, err := dedupe.NewDedupeEngine(ctx, pclnts[indx], dedupeEnabled[indx].DedupeBuffer, dedupeEnabled[indx].DedupeThreads, debug, dedupeEnabled[indx].CompressData, indx, dedupeEnabled[indx].CacheSize, dedupeEnabled[indx].CacheAge)
			if err != nil {
				log.Errorf("error initializing dedupe connection: %v\n", err)
				return nil, err
			}
			dd[indx] = de
		}
		defaultVolume = indx
	}
	log.Debugf("Default Volume %d", defaultVolume)
	sc := &FileIOProxy{fc: fcm, dedupeEnabled: dedupeEnabled, dedupe: dd, dfc: defaultVolume, proxy: proxy}
	return sc, nil

}
