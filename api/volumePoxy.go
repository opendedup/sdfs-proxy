package api

import (
	"context"
	"os"
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
	vc       spb.VolumeServiceClient
	Clnt     *grpc.ClientConn
	password string
	debug    bool
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
	return s.vc.SetMaxAge(ctx, req)
}

func (s *VolumeProxy) GetVolumeInfo(ctx context.Context, req *spb.VolumeInfoRequest) (*spb.VolumeInfoResponse, error) {
	return s.vc.GetVolumeInfo(ctx, req)

}

func (s *VolumeProxy) ShutdownVolume(ctx context.Context, req *spb.ShutdownRequest) (*spb.ShutdownResponse, error) {
	go s.shutdown()
	return &spb.ShutdownResponse{}, nil

}

func (s *VolumeProxy) CleanStore(ctx context.Context, req *spb.CleanStoreRequest) (*spb.CleanStoreResponse, error) {
	return s.vc.CleanStore(ctx, req)

}

func (s *VolumeProxy) DeleteCloudVolume(ctx context.Context, req *spb.DeleteCloudVolumeRequest) (*spb.DeleteCloudVolumeResponse, error) {
	return s.vc.DeleteCloudVolume(ctx, req)

}

func (s *VolumeProxy) DSEInfo(ctx context.Context, req *spb.DSERequest) (*spb.DSEResponse, error) {
	return s.vc.DSEInfo(ctx, req)

}

func (s *VolumeProxy) SystemInfo(ctx context.Context, req *spb.SystemInfoRequest) (*spb.SystemInfoResponse, error) {
	return s.vc.SystemInfo(ctx, req)

}
func (s *VolumeProxy) SetVolumeCapacity(ctx context.Context, req *spb.SetVolumeCapacityRequest) (*spb.SetVolumeCapacityResponse, error) {
	return s.vc.SetVolumeCapacity(ctx, req)

}
func (s *VolumeProxy) GetConnectedVolumes(ctx context.Context, req *spb.CloudVolumesRequest) (*spb.CloudVolumesResponse, error) {
	return s.vc.GetConnectedVolumes(ctx, req)

}
func (s *VolumeProxy) GetGCSchedule(ctx context.Context, req *spb.GCScheduleRequest) (*spb.GCScheduleResponse, error) {
	return s.vc.GetGCSchedule(ctx, req)

}
func (s *VolumeProxy) SetCacheSize(ctx context.Context, req *spb.SetCacheSizeRequest) (*spb.SetCacheSizeResponse, error) {
	return s.vc.SetCacheSize(ctx, req)

}

func (s *VolumeProxy) SetReadSpeed(ctx context.Context, req *spb.SpeedRequest) (*spb.SpeedResponse, error) {
	return s.vc.SetReadSpeed(ctx, req)

}
func (s *VolumeProxy) SetWriteSpeed(ctx context.Context, req *spb.SpeedRequest) (*spb.SpeedResponse, error) {
	return s.vc.SetWriteSpeed(ctx, req)

}
func (s *VolumeProxy) SyncFromCloudVolume(ctx context.Context, req *spb.SyncFromVolRequest) (*spb.SyncFromVolResponse, error) {
	return s.vc.SyncFromCloudVolume(ctx, req)

}
func (s *VolumeProxy) SyncCloudVolume(ctx context.Context, req *spb.SyncVolRequest) (*spb.SyncVolResponse, error) {
	return s.vc.SyncCloudVolume(ctx, req)

}

func (s *VolumeProxy) shutdown() {
	timer := time.NewTimer(10 * time.Second)
	log.Warn("Shutting down volume")
	<-timer.C
	if !s.debug {
		os.Exit(0)
	}

}

func NewVolumeProxy(clnt *grpc.ClientConn, password string, debug bool) *VolumeProxy {
	vc := spb.NewVolumeServiceClient(clnt)
	sc := &VolumeProxy{vc: vc, Clnt: clnt, password: password, debug: debug}
	return sc

}
