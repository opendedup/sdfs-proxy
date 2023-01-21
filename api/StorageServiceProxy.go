package api

import (
	"context"
	"fmt"
	"io"
	"os"

	spb "github.com/opendedup/sdfs-client-go/sdfs"
	pool "github.com/processout/grpc-go-pool"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type StorageServiceProxy struct {
	spb.UnimplementedStorageServiceServer
	dd     map[int64]spb.StorageServiceClient
	pclnts map[int64]*pool.Pool
	dss    int64
	proxy  bool
}

func (s *StorageServiceProxy) HashingInfo(ctx context.Context, req *spb.HashingInfoRequest) (*spb.HashingInfoResponse, error) {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID
	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dss
	}
	if val, ok := s.dd[volid]; ok {
		return val.HashingInfo(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}
}

func (s *StorageServiceProxy) ReplicateRemoteFile(ctx context.Context, req *spb.FileReplicationRequest) (*spb.FileReplicationResponse, error) {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID
	if s.proxy || volid == 0 || volid == -1 {
		log.Debugf("Replicating using default volume %d", volid)
		volid = s.dss
	}
	if val, ok := s.dd[volid]; ok {
		log.Debugf("Replicating using volume %d %d", volid, req.PvolumeID)
		return val.ReplicateRemoteFile(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}
}

func (s *StorageServiceProxy) WriteChunksStream(stream spb.StorageService_WriteChunksStreamServer) error {
	var cstream spb.StorageService_WriteChunksStreamClient
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			resp, err := cstream.CloseAndRecv()

			stream.SendAndClose(resp)
			return err
		}
		if err != nil {
			return err
		}
		volid := req.PvolumeID
		if s.proxy || volid == 0 || volid == -1 {
			log.Debugf("Replicating using default volume %d", volid)
			volid = s.dss
		}
		if cstream == nil {
			if val, ok := s.pclnts[volid]; ok {
				log.Debugf("Replicating using volume %d %d", volid, req.PvolumeID)
				client, err := val.Get(ctx)
				if err != nil {
					return err
				}
				defer client.Close()
				sserv := spb.NewStorageServiceClient(client)
				cstream, err = sserv.WriteChunksStream(ctx)
				if err != nil {
					return err
				}
			} else {
				return fmt.Errorf("unable to find volume %d", volid)
			}
		}
		err = cstream.Send(req)
		if err != nil {
			return err
		}
	}

}

func (s *StorageServiceProxy) RestoreArchives(ctx context.Context, req *spb.RestoreArchivesRequest) (*spb.RestoreArchivesResponse, error) {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID
	if s.proxy || volid == 0 || volid == -1 {
		log.Debugf("Retoring using default volume %d", volid)
		volid = s.dss
	}
	if val, ok := s.dd[volid]; ok {
		log.Debugf("Retoring using volume %d %d", volid, req.PvolumeID)
		return val.RestoreArchives(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}
}

func (s *StorageServiceProxy) CancelReplication(ctx context.Context, req *spb.CancelReplicationRequest) (*spb.CancelReplicationResponse, error) {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID
	if s.proxy || volid == 0 || volid == -1 {
		log.Debugf("Cancel Replication using default volume %d", volid)
		volid = s.dss
	}
	if val, ok := s.dd[volid]; ok {
		log.Debugf("Cancel Replication using volume %d %d", volid, req.PvolumeID)
		return val.CancelReplication(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}
}

func (s *StorageServiceProxy) PauseReplication(ctx context.Context, req *spb.PauseReplicationRequest) (*spb.PauseReplicationResponse, error) {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID
	if s.proxy || volid == 0 || volid == -1 {
		log.Debugf("Pause Replication using default volume %d", volid)
		volid = s.dss
	}
	if val, ok := s.dd[volid]; ok {
		log.Debugf("Pause Replication using volume %d %d", volid, req.PvolumeID)
		return val.PauseReplication(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}
}

func (s *StorageServiceProxy) CheckHashes(ctx context.Context, req *spb.CheckHashesRequest) (*spb.CheckHashesResponse, error) {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID
	if s.proxy || volid == 0 || volid == -1 {
		log.Debugf("CheckHashes using default volume %d", volid)
		volid = s.dss
	}
	if val, ok := s.pclnts[volid]; ok {
		log.Debugf("CheckHashes using volume %d %d", volid, req.PvolumeID)
		client, err := val.Get(ctx)
		if err != nil {
			return nil, err
		}
		defer client.Close()
		sserv := spb.NewStorageServiceClient(client)
		return sserv.CheckHashes(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}

func (s *StorageServiceProxy) WriteChunks(ctx context.Context, req *spb.WriteChunksRequest) (*spb.WriteChunksResponse, error) {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID
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

func (s *StorageServiceProxy) SubscribeToVolume(req *spb.VolumeEventListenRequest, stream spb.StorageService_SubscribeToVolumeServer) error {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID
	if s.proxy || volid == 0 || volid == -1 {
		log.Debugf("SubscribeToVolume using default volume %d", volid)
		volid = s.dss
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if val, ok := s.dd[volid]; ok {
		fi, err := val.SubscribeToVolume(ctx, req)
		if err != nil {
			return err
		}
		for {
			fl, err := fi.Recv()
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

func (s *StorageServiceProxy) GetChunks(req *spb.GetChunksRequest, stream spb.StorageService_GetChunksServer) error {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID
	if s.proxy || volid == 0 || volid == -1 {
		log.Debugf("GetChunks using default volume %d", volid)
		volid = s.dss
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if val, ok := s.dd[volid]; ok {
		fi, err := val.GetChunks(ctx, req)
		if err != nil {
			return err
		}
		for {
			fl, err := fi.Recv()
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

func (s *StorageServiceProxy) ListReplLogs(req *spb.VolumeEventListenRequest, stream spb.StorageService_ListReplLogsServer) error {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID

	if s.proxy || volid == 0 || volid == -1 {
		log.Debugf("ListReplLogs using default volume %d", volid)
		volid = s.dss
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if val, ok := s.dd[volid]; ok {
		fi, err := val.ListReplLogs(ctx, req)
		if err != nil {
			return err
		}
		for {
			fl, err := fi.Recv()
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

func (s *StorageServiceProxy) AddReplicaSource(ctx context.Context, req *spb.AddReplicaSourceRequest) (*spb.AddReplicaSourceResponse, error) {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID

	if s.proxy || volid == 0 || volid == -1 {
		log.Debugf("AddReplicaSource using default volume %d", volid)
		volid = s.dss
	}
	if val, ok := s.dd[volid]; ok {
		log.Debugf("AddReplicaSource using volume %d %d", volid, req.PvolumeID)
		return val.AddReplicaSource(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}
}

func (s *StorageServiceProxy) RemoveReplicaSource(ctx context.Context, req *spb.RemoveReplicaSourceRequest) (*spb.RemoveReplicaSourceResponse, error) {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID

	if s.proxy || volid == 0 || volid == -1 {
		log.Debugf("RemoveReplicaSource using default volume %d", volid)
		volid = s.dss
	}
	if val, ok := s.dd[volid]; ok {
		log.Debugf("RemoveReplicaSource using volume %d %d", volid, req.PvolumeID)
		return val.RemoveReplicaSource(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}
}

func (s *StorageServiceProxy) WriteSparseDataChunk(ctx context.Context, req *spb.SparseDedupeChunkWriteRequest) (*spb.SparseDedupeChunkWriteResponse, error) {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID

	if s.proxy || volid == 0 || volid == -1 {
		log.Debugf("WriteSparseDataChunk using default volume %d", volid)
		volid = s.dss
	}
	if val, ok := s.pclnts[volid]; ok {
		log.Debugf("WriteSparseDataChunk using volume %d %d", volid, req.PvolumeID)
		client, err := val.Get(ctx)
		if err != nil {
			return nil, err
		}
		defer client.Close()
		sserv := spb.NewStorageServiceClient(client)
		return sserv.WriteSparseDataChunk(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}
}

func (s *StorageServiceProxy) ReadSparseDataChunk(ctx context.Context, req *spb.SparseDedupeChunkReadRequest) (*spb.SparseDedupeChunkReadResponse, error) {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID

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
	log.Debug("in")
	defer log.Debug("out")
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

func (s *StorageServiceProxy) GetMetaDataDedupeFile(ctx context.Context, req *spb.MetaDataDedupeFileRequest) (*spb.MetaDataDedupeFileResponse, error) {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID

	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dss
	}
	if val, ok := s.dd[volid]; ok {
		fi, err := val.GetMetaDataDedupeFile(ctx, req)
		if err != nil {
			return nil, err
		}
		return fi, nil
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}
}

func (s *StorageServiceProxy) GetSparseDedupeFile(req *spb.SparseDedupeFileRequest, stream spb.StorageService_GetSparseDedupeFileServer) error {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID

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

func NewStorageService(clnts map[int64]*grpc.ClientConn, pclnts map[int64]*pool.Pool, proxy, debug bool) *StorageServiceProxy {
	if debug {
		log.SetLevel(log.DebugLevel)
	}
	log.SetReportCaller(true)
	log.SetOutput(os.Stdout)
	vcm := make(map[int64]spb.StorageServiceClient)
	var defaultVolume int64
	for indx, clnt := range clnts {
		evt := spb.NewStorageServiceClient(clnt)
		vcm[indx] = evt
		defaultVolume = indx
	}
	sc := &StorageServiceProxy{dd: vcm, dss: defaultVolume, proxy: proxy, pclnts: pclnts}
	return sc

}
