package api

import (
	"context"
	"fmt"
	"os"
	"sync"

	spb "github.com/opendedup/sdfs-client-go/sdfs"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type SDFSEventProxy struct {
	spb.UnimplementedSDFSEventServiceServer
	evt        map[int64]spb.SDFSEventServiceClient
	devt       int64
	proxy      bool
	configLock sync.RWMutex
}

func (s *SDFSEventProxy) GetEvent(ctx context.Context, req *spb.SDFSEventRequest) (*spb.SDFSEventResponse, error) {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
	if s.proxy || volid == 0 || volid == -1 {
		volid = s.devt
	}
	if val, ok := s.evt[volid]; ok {
		return val.GetEvent(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}
}

func (s *SDFSEventProxy) GetEvents(ctx context.Context, req *spb.SDFSEventsRequest) (*spb.SDFSEventsResponse, error) {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
	if s.proxy || volid == 0 || volid == -1 {
		volid = s.devt
	}
	if val, ok := s.evt[volid]; ok {
		return val.GetEvents(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}
}

func (s *SDFSEventProxy) ListEvents(ctx context.Context, req *spb.SDFSEventListRequest) (*spb.SDFSEventListResponse, error) {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
	if s.proxy || volid == 0 || volid == -1 {
		volid = s.devt
	}
	if val, ok := s.evt[volid]; ok {
		return val.ListEvents(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}

func (s *SDFSEventProxy) SubscribeEvent(req *spb.SDFSEventRequest, stream spb.SDFSEventService_SubscribeEventServer) error {
	log.Debug("in")
	defer log.Debug("out")
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
	if s.proxy || volid == 0 || volid == -1 {
		volid = s.devt
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if val, ok := s.evt[volid]; ok {
		fi, err := val.SubscribeEvent(ctx, req)
		if err != nil {
			return err
		}
		for {
			event, err := fi.Recv()
			if err != nil {
				return err

			}
			if err := stream.Send(event); err != nil {
				return err
			}
		}
	} else {
		return fmt.Errorf("unable to find volume %d", volid)
	}
}

func (s *SDFSEventProxy) ReloadVolumeMap(clnts map[int64]*grpc.ClientConn, debug bool) error {
	log.Debug("in")
	defer log.Debug("out")
	s.configLock.Lock()
	defer s.configLock.Unlock()
	vcm := make(map[int64]spb.SDFSEventServiceClient)
	var defaultVolume int64
	for indx, clnt := range clnts {
		evt := spb.NewSDFSEventServiceClient(clnt)
		vcm[indx] = evt
		defaultVolume = indx
	}
	s.evt = vcm
	s.devt = defaultVolume
	return nil
}

func NewEventProxy(clnts map[int64]*grpc.ClientConn, proxy, debug bool) *SDFSEventProxy {
	if debug {
		log.SetLevel(log.DebugLevel)
	}
	log.SetOutput(os.Stdout)
	log.SetReportCaller(true)
	vcm := make(map[int64]spb.SDFSEventServiceClient)
	var defaultVolume int64
	for indx, clnt := range clnts {
		evt := spb.NewSDFSEventServiceClient(clnt)
		vcm[indx] = evt
		defaultVolume = indx
	}
	sc := &SDFSEventProxy{evt: vcm, devt: defaultVolume, proxy: proxy}
	return sc

}
