package api

import (
	"context"
	"fmt"
	"sync"

	spb "github.com/opendedup/sdfs-client-go/sdfs"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type StorageServiceProxy struct {
	spb.UnimplementedStorageServiceServer
	dd         map[int64]spb.StorageServiceClient
	dss        int64
	proxy      bool
	configLock sync.RWMutex
}

func (s *StorageServiceProxy) HashingInfo(ctx context.Context, req *spb.HashingInfoRequest) (*spb.HashingInfoResponse, error) {
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dss
	}
	if val, ok := s.dd[volid]; ok {
		return val.HashingInfo(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}
}

func (s *StorageServiceProxy) CheckHashes(ctx context.Context, req *spb.CheckHashesRequest) (*spb.CheckHashesResponse, error) {
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
	if s.proxy || volid == 0 || volid == -1 {
		log.Debugf("CheckHashes using default volume %d", volid)
		volid = s.dss
	}
	if val, ok := s.dd[volid]; ok {
		log.Debugf("CheckHashes using volume %d %d", volid, req.PvolumeID)
		return val.CheckHashes(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}

func (s *StorageServiceProxy) WriteChunks(ctx context.Context, req *spb.WriteChunksRequest) (*spb.WriteChunksResponse, error) {
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
	if s.proxy || volid == 0 || volid == -1 {
		log.Debugf("WriteChunks using default volume %d", volid)
		volid = s.dss
	}
	if val, ok := s.dd[volid]; ok {
		log.Debugf("WriteChunks using default volume %d %d", volid, req.PvolumeID)
		return val.WriteChunks(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}

func (s *StorageServiceProxy) ReadChunks(ctx context.Context, req *spb.ReadChunksRequest) (*spb.ReadChunksResponse, error) {
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
	if s.proxy || volid == 0 || volid == -1 {
		log.Debugf("ReadChunks using default volume %d", volid)
		volid = s.dss
	}
	if val, ok := s.dd[volid]; ok {
		return val.ReadChunks(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}
}

func (s *StorageServiceProxy) WriteSparseDataChunk(ctx context.Context, req *spb.SparseDedupeChunkWriteRequest) (*spb.SparseDedupeChunkWriteResponse, error) {
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
	if s.proxy || volid == 0 || volid == -1 {
		log.Debugf("WriteSparseDataChunk using default volume %d", volid)
		volid = s.dss
	}
	if val, ok := s.dd[volid]; ok {
		log.Debugf("WriteSparseDataChunk using volume %d %d", volid, req.PvolumeID)
		return val.WriteSparseDataChunk(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}
}

func (s *StorageServiceProxy) ReadSparseDataChunk(ctx context.Context, req *spb.SparseDedupeChunkReadRequest) (*spb.SparseDedupeChunkReadResponse, error) {
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
	if s.proxy || volid == 0 || volid == -1 {
		log.Debugf("ReadSparseDataChunk using default volume %d", volid)
		volid = s.dss
	}
	if val, ok := s.dd[volid]; ok {
		return val.ReadSparseDataChunk(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}
}

func (s *StorageServiceProxy) ReloadVolumeMap(clnts map[int64]*grpc.ClientConn, debug bool) error {
	s.configLock.Lock()
	defer s.configLock.Unlock()
	vcm := make(map[int64]spb.StorageServiceClient)
	var defaultVolume int64
	for indx, clnt := range clnts {
		evt := spb.NewStorageServiceClient(clnt)
		vcm[indx] = evt
		defaultVolume = indx
	}
	s.dd = vcm
	s.dss = defaultVolume
	return nil
}

func (s *StorageServiceProxy) GetMetaDataDedupeFile(req *spb.MetaDataDedupeFileRequest, stream spb.StorageService_GetMetaDataDedupeFileServer) error {
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dss
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if val, ok := s.dd[volid]; ok {
		fi, err := val.GetMetaDataDedupeFile(ctx, req)
		if err != nil {
			return err
		}
		for {
			fl, err := fi.Recv()
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
}

func (s *StorageServiceProxy) GetSparseDedupeFile(req *spb.SparseDedupeFileRequest, stream spb.StorageService_GetSparseDedupeFileServer) error {
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dss
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if val, ok := s.dd[volid]; ok {
		fi, err := val.GetSparseDedupeFile(ctx, req)
		if err != nil {
			return err
		}
		for {
			fl, err := fi.Recv()
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
}

func NewStorageService(clnts map[int64]*grpc.ClientConn, proxy bool) *StorageServiceProxy {
	vcm := make(map[int64]spb.StorageServiceClient)
	var defaultVolume int64
	for indx, clnt := range clnts {
		evt := spb.NewStorageServiceClient(clnt)
		vcm[indx] = evt
		defaultVolume = indx
	}
	sc := &StorageServiceProxy{dd: vcm, dss: defaultVolume, proxy: proxy}
	return sc

}
