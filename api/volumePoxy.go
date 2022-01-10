package api

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/dgrijalva/jwt-go"
	spb "github.com/opendedup/sdfs-client-go/sdfs"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type customClaims struct {
	Username string `json:"username"`
	jwt.StandardClaims
}

type VolumeProxy struct {
	spb.UnimplementedVolumeServiceServer
	vc         map[int64]spb.VolumeServiceClient
	dvc        int64
	Clnt       map[int64]*grpc.ClientConn
	password   string
	proxy      bool
	configLock sync.RWMutex
}

func (s *VolumeProxy) AuthenticateUser(ctx context.Context, req *spb.AuthenticationRequest) (*spb.AuthenticationResponse, error) {
	pwd := req.Password
	if pwd == s.password {
		expirationTime := time.Now().Add(5 * time.Minute)
		claims := customClaims{
			Username: req.Username,
			StandardClaims: jwt.StandardClaims{
				ExpiresAt: expirationTime.Unix(),
				Issuer:    "sdfs-proxy",
			},
		}
		token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
		signedToken, err := token.SignedString([]byte(s.password))
		if err != nil {
			return nil, err
		}
		return &spb.AuthenticationResponse{Token: signedToken}, nil
	} else {
		return &spb.AuthenticationResponse{Error: "Unable to authenticate user to proxy", ErrorCode: spb.ErrorCodes_EACCES}, nil
	}
}

func (s *VolumeProxy) SetMaxAge(ctx context.Context, req *spb.SetMaxAgeRequest) (*spb.SetMaxAgeResponse, error) {
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dvc
	}
	if val, ok := s.vc[volid]; ok {
		return val.SetMaxAge(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}
}

func (s *VolumeProxy) GetVolumeInfo(ctx context.Context, req *spb.VolumeInfoRequest) (*spb.VolumeInfoResponse, error) {
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dvc
	}
	if val, ok := s.vc[volid]; ok {
		return val.GetVolumeInfo(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}
}

func (s *VolumeProxy) ShutdownVolume(ctx context.Context, req *spb.ShutdownRequest) (*spb.ShutdownResponse, error) {
	go s.shutdown()
	return &spb.ShutdownResponse{}, nil

}

func (s *VolumeProxy) CleanStore(ctx context.Context, req *spb.CleanStoreRequest) (*spb.CleanStoreResponse, error) {
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dvc
	}
	if val, ok := s.vc[volid]; ok {
		return val.CleanStore(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}

func (s *VolumeProxy) DeleteCloudVolume(ctx context.Context, req *spb.DeleteCloudVolumeRequest) (*spb.DeleteCloudVolumeResponse, error) {
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dvc
	}
	if val, ok := s.vc[volid]; ok {
		return val.DeleteCloudVolume(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}

func (s *VolumeProxy) DSEInfo(ctx context.Context, req *spb.DSERequest) (*spb.DSEResponse, error) {
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dvc
	}
	if val, ok := s.vc[volid]; ok {
		return val.DSEInfo(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}

func (s *VolumeProxy) SystemInfo(ctx context.Context, req *spb.SystemInfoRequest) (*spb.SystemInfoResponse, error) {
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dvc
	}
	if val, ok := s.vc[volid]; ok {
		return val.SystemInfo(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}
func (s *VolumeProxy) SetVolumeCapacity(ctx context.Context, req *spb.SetVolumeCapacityRequest) (*spb.SetVolumeCapacityResponse, error) {
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dvc
	}
	if val, ok := s.vc[volid]; ok {
		return val.SetVolumeCapacity(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}
func (s *VolumeProxy) GetConnectedVolumes(ctx context.Context, req *spb.CloudVolumesRequest) (*spb.CloudVolumesResponse, error) {
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dvc
	}
	if val, ok := s.vc[volid]; ok {
		return val.GetConnectedVolumes(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}
func (s *VolumeProxy) GetGCSchedule(ctx context.Context, req *spb.GCScheduleRequest) (*spb.GCScheduleResponse, error) {
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dvc
	}
	if val, ok := s.vc[volid]; ok {
		return val.GetGCSchedule(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}
func (s *VolumeProxy) SetCacheSize(ctx context.Context, req *spb.SetCacheSizeRequest) (*spb.SetCacheSizeResponse, error) {
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dvc
	}
	if val, ok := s.vc[volid]; ok {
		return val.SetCacheSize(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}

func (s *VolumeProxy) SetReadSpeed(ctx context.Context, req *spb.SpeedRequest) (*spb.SpeedResponse, error) {
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dvc
	}
	if val, ok := s.vc[volid]; ok {
		return val.SetReadSpeed(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}
func (s *VolumeProxy) SetWriteSpeed(ctx context.Context, req *spb.SpeedRequest) (*spb.SpeedResponse, error) {
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dvc
	}
	if val, ok := s.vc[volid]; ok {
		return val.SetWriteSpeed(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}
func (s *VolumeProxy) SyncFromCloudVolume(ctx context.Context, req *spb.SyncFromVolRequest) (*spb.SyncFromVolResponse, error) {
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dvc
	}
	if val, ok := s.vc[volid]; ok {
		return val.SyncFromCloudVolume(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}
func (s *VolumeProxy) SyncCloudVolume(ctx context.Context, req *spb.SyncVolRequest) (*spb.SyncVolResponse, error) {
	volid := req.PvolumeID
	s.configLock.RLock()
	defer s.configLock.RUnlock()
	if s.proxy || volid == 0 || volid == -1 {
		volid = s.dvc
	}
	if val, ok := s.vc[volid]; ok {
		return val.SyncCloudVolume(ctx, req)
	} else {
		return nil, fmt.Errorf("unable to find volume %d", volid)
	}

}

func (s *VolumeProxy) shutdown() {
	timer := time.NewTimer(10 * time.Second)
	log.Warn("Shutting down volume")
	<-timer.C
	if !NOSHUTDOWN {
		os.Exit(0)
	}

}

func (s *VolumeProxy) ReloadVolumeMap(clnts map[int64]*grpc.ClientConn, debug bool) error {
	s.configLock.Lock()
	defer s.configLock.Unlock()
	vcm := make(map[int64]spb.VolumeServiceClient)
	var defaultVolume int64
	for indx, clnt := range clnts {
		vc := spb.NewVolumeServiceClient(clnt)
		vcm[indx] = vc
		defaultVolume = indx
	}
	s.vc = vcm
	s.dvc = defaultVolume
	return nil
}

func NewVolumeProxy(clnts map[int64]*grpc.ClientConn, password string, proxy bool) *VolumeProxy {
	vcm := make(map[int64]spb.VolumeServiceClient)
	var defaultVolume int64
	for indx, clnt := range clnts {
		vc := spb.NewVolumeServiceClient(clnt)
		vcm[indx] = vc
		defaultVolume = indx
	}
	sc := &VolumeProxy{vc: vcm, Clnt: clnts, password: password, dvc: defaultVolume, proxy: proxy}
	return sc

}
