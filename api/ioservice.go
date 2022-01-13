package api

import (
	"context"
	"fmt"
	"sync"

	log "github.com/sirupsen/logrus"

	"github.com/opendedup/sdfs-client-go/dedupe"
	spb "github.com/opendedup/sdfs-client-go/sdfs"
	"google.golang.org/grpc"
)

type FileIOProxy struct {
	spb.UnimplementedFileIOServiceServer
	fc            map[int64]spb.FileIOServiceClient
	dfc           int64
	proxy         bool
	dedupe        map[int64]*dedupe.DedupeEngine
	dedupeEnabled map[int64]ForwardEntry
	configLock    sync.RWMutex
}

func (s *FileIOProxy) GetXAttrSize(ctx context.Context, req *spb.GetXAttrSizeRequest) (*spb.GetXAttrSizeResponse, error) {
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
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
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dfc
	}
	if s.dedupeEnabled[volid].Dedupe {
		s.dedupe[volid].SyncFile(req.Path)
	}
	if val, ok := s.fc[volid]; ok {
		return val.Fsync(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}
}

func (s *FileIOProxy) SetXAttr(ctx context.Context, req *spb.SetXAttrRequest) (*spb.SetXAttrResponse, error) {
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
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
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
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
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
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
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dfc
	}
	if val, ok := s.fc[volid]; ok {
		return val.Utime(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}
}

func (s *FileIOProxy) Truncate(ctx context.Context, req *spb.TruncateRequest) (*spb.TruncateResponse, error) {
	volid := req.PvolumeID
	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dfc
	}
	if val, ok := s.fc[volid]; ok {
		if dval, ok := s.dedupe[volid]; ok {
			dval.SyncFile(req.Path)
		}

		return val.Truncate(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}
func (s *FileIOProxy) SymLink(ctx context.Context, req *spb.SymLinkRequest) (*spb.SymLinkResponse, error) {
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
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
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dfc
	}
	if s.dedupeEnabled[volid].Dedupe {
		s.dedupe[volid].SyncFile(req.Path)
	}
	if val, ok := s.fc[volid]; ok {
		return val.GetAttr(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}

func (s *FileIOProxy) ReadLink(ctx context.Context, req *spb.LinkRequest) (*spb.LinkResponse, error) {
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
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
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dfc
	}
	if s.dedupeEnabled[volid].Dedupe {
		s.dedupe[volid].Sync(req.Fd)
	}
	if val, ok := s.fc[volid]; ok {
		return val.Flush(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}
func (s *FileIOProxy) Chown(ctx context.Context, req *spb.ChownRequest) (*spb.ChownResponse, error) {
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dfc
	}
	if val, ok := s.fc[volid]; ok {
		return val.Chown(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}
func (s *FileIOProxy) MkDir(ctx context.Context, req *spb.MkDirRequest) (*spb.MkDirResponse, error) {
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
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
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
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
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
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
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dfc
	}
	if val, ok := s.fc[volid]; ok {
		if dval, ok := s.dedupe[volid]; ok {
			err := dval.Write(req.FileHandle, req.Start, req.Data, req.Len)
			if err != nil {
				log.Errorf("error writing %v", err)
				return nil, err
			} else {
				return &spb.DataWriteResponse{}, nil
			}
		}
		return val.Write(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}
}
func (s *FileIOProxy) Read(ctx context.Context, req *spb.DataReadRequest) (*spb.DataReadResponse, error) {
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dfc
	}
	if val, ok := s.fc[volid]; ok {
		if dval, ok := s.dedupe[volid]; ok {
			dval.Sync(req.FileHandle)
		}
		return val.Read(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}
func (s *FileIOProxy) Release(ctx context.Context, req *spb.FileCloseRequest) (*spb.FileCloseResponse, error) {
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dfc
	}
	if val, ok := s.fc[volid]; ok {
		if dval, ok := s.dedupe[volid]; ok {
			dval.Close(req.FileHandle)
		}
		return val.Release(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}
func (s *FileIOProxy) Mknod(ctx context.Context, req *spb.MkNodRequest) (*spb.MkNodResponse, error) {
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
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
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
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
			err = dval.Open(req.Path, rsp.FileHandle)
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
func (s *FileIOProxy) GetFileInfo(ctx context.Context, req *spb.FileInfoRequest) (*spb.FileMessageResponse, error) {
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dfc
	}
	if val, ok := s.fc[volid]; ok {
		if dval, ok := s.dedupe[volid]; ok {
			dval.SyncFile(req.FileName)
		}
		return val.GetFileInfo(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}
func (s *FileIOProxy) CreateCopy(ctx context.Context, req *spb.FileSnapshotRequest) (*spb.FileSnapshotResponse, error) {
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dfc
	}
	if val, ok := s.fc[volid]; ok {
		if dval, ok := s.dedupe[volid]; ok {
			dval.SyncFile(req.Src)
			dval.SyncFile(req.Dest)
		}
		return val.CreateCopy(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}
func (s *FileIOProxy) FileExists(ctx context.Context, req *spb.FileExistsRequest) (*spb.FileExistsResponse, error) {
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
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
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
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
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dfc
	}
	if val, ok := s.fc[volid]; ok {
		if dval, ok := s.dedupe[volid]; ok {
			dval.SyncFile(req.FileName)
		}
		return val.Stat(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}
func (s *FileIOProxy) Rename(ctx context.Context, req *spb.FileRenameRequest) (*spb.FileRenameResponse, error) {
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dfc
	}
	if val, ok := s.fc[volid]; ok {
		if dval, ok := s.dedupe[volid]; ok {
			dval.CloseFile(req.Src)
			dval.CloseFile(req.Dest)
		}
		return val.Rename(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}
func (s *FileIOProxy) CopyExtent(ctx context.Context, req *spb.CopyExtentRequest) (*spb.CopyExtentResponse, error) {
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dfc
	}
	if val, ok := s.fc[volid]; ok {
		if dval, ok := s.dedupe[volid]; ok {
			dval.SyncFile(req.SrcFile)
			dval.SyncFile(req.DstFile)
		}
		return val.CopyExtent(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}
func (s *FileIOProxy) SetUserMetaData(ctx context.Context, req *spb.SetUserMetaDataRequest) (*spb.SetUserMetaDataResponse, error) {
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
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
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
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
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
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
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dfc
	}
	if val, ok := s.fc[volid]; ok {
		return val.StatFS(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}

func (s *FileIOProxy) ReloadVolumeMap(clnts map[int64]*grpc.ClientConn, dedupeEnabled map[int64]ForwardEntry, debug bool) error {
	s.configLock.Lock()
	defer s.configLock.Unlock()
	fcm := make(map[int64]spb.FileIOServiceClient)
	dd := make(map[int64]*dedupe.DedupeEngine)
	var defaultVolume int64
	for indx, clnt := range clnts {
		vc := spb.NewFileIOServiceClient(clnt)
		fcm[indx] = vc
		if dedupeEnabled[indx].Dedupe {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			de, err := dedupe.NewDedupeEngine(ctx, clnt, 4, 8, debug, indx)
			if err != nil {
				log.Printf("error initializing dedupe connection: %v\n", err)
				return err
			}
			dd[indx] = de

		}
		defaultVolume = indx
	}
	s.dfc = defaultVolume
	s.dedupe = dd
	s.fc = fcm
	return nil
}

func (s *FileIOProxy) SetRetrievalTier(ctx context.Context, req *spb.SetRetrievalTierRequest) (*spb.SetRetrievalTierResponse, error) {

	volid := req.PvolumeID

	s.configLock.RLock()

	defer s.configLock.RUnlock()

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

	volid := req.PvolumeID

	s.configLock.RLock()

	defer s.configLock.RUnlock()

	if s.proxy || volid == 0 || volid == -1 {

		volid = s.dfc

	}

	if val, ok := s.fc[volid]; ok {

		return val.GetRetrievalTier(ctx, req)

	} else {

		return nil, fmt.Errorf("unable to find volume %d", volid)

	}

}

func NewFileIOProxy(clnts map[int64]*grpc.ClientConn, dedupeEnabled map[int64]ForwardEntry, proxy, debug bool) (*FileIOProxy, error) {
	fcm := make(map[int64]spb.FileIOServiceClient)
	dd := make(map[int64]*dedupe.DedupeEngine)
	var defaultVolume int64
	for indx, clnt := range clnts {
		vc := spb.NewFileIOServiceClient(clnt)
		fcm[indx] = vc
		if dedupeEnabled[indx].Dedupe {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			de, err := dedupe.NewDedupeEngine(ctx, clnt, dedupeEnabled[indx].DedupeBuffer, dedupeEnabled[indx].DedupeThreads, debug, indx)
			if err != nil {
				log.Errorf("error initializing dedupe connection: %v\n", err)
				return nil, err
			}
			dd[indx] = de
		}
		defaultVolume = indx
	}
	log.Errorf("Default Volume %d", defaultVolume)
	sc := &FileIOProxy{fc: fcm, dedupeEnabled: dedupeEnabled, dedupe: dd, dfc: defaultVolume, proxy: proxy}
	return sc, nil

}
