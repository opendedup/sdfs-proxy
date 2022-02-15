package api

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"sync"

	log "github.com/sirupsen/logrus"

	pb "github.com/opendedup/sdfs-client-go/api"
	spb "github.com/opendedup/sdfs-client-go/sdfs"
	"google.golang.org/grpc"
)

type PortRedictor struct {
	spb.UnimplementedPortRedirectorServiceServer
	vp            *VolumeProxy
	ep            *SDFSEventProxy
	iop           *FileIOProxy
	sp            *StorageServiceProxy
	config        string
	pr            PortRedirectors
	Cmp           map[int64]*grpc.ClientConn
	pcmp          []*grpc.ClientConn
	Dd            map[int64]ForwardEntry
	configLock    sync.RWMutex
	listenPort    string
	portforwarder bool
	pc            spb.PortRedirectorServiceClient
}

type ForwardEntry struct {
	Pwd           string `json:"pwd"`
	User          string `json:"user"`
	Lpwd          string `json:"local-auth"`
	Address       string `json:"address" required:"true"`
	DisableTrust  bool   `json:"trust-all"`
	Mtls          bool   `json:"mtls"`
	Mtlsca        string `json:"root-ca"`
	Mtlskey       string `json:"mtls-key"`
	Mtlscert      string `json:"mtls-cert"`
	Dedupe        bool   `json:"dedupe"`
	DedupeThreads int    `json:"dedupe-threads" default:"8"`
	DedupeBuffer  int    `json:"dedupe-buffer" default:"4"`
	CompressData  bool   `json:"compress"`
	CacheSize     int    `json:"dedupe-cache-size" default:"1000000"`
	CacheAge      int    `json:"dedupe-cache-age" default:"30"`
}

type PortRedirectors struct {
	ListenPort    string         `json:"port"`
	ForwardEntrys []ForwardEntry `json:"forwarders"`
}

func (s *PortRedictor) WriteConfig() error {
	file, _ := json.MarshalIndent(s.pr, "", " ")

	_ = ioutil.WriteFile(s.config, file, 0644)
	return nil
}

func (s *PortRedictor) ReloadConfig(ctx context.Context, req *spb.ReloadConfigRequest) (*spb.ReloadConfigResponse, error) {
	if s.portforwarder {
		return s.pc.ReloadConfig(ctx, req)
	}
	s.configLock.Lock()
	defer s.configLock.Unlock()
	err := s.localReadConfig()
	if err != nil {
		return nil, err
	}
	err = s.iop.ReloadVolumeMap(s.Cmp, s.Dd, false)
	if err != nil {
		return nil, err
	}
	err = s.vp.ReloadVolumeMap(s.Cmp, false)
	if err != nil {
		return nil, err
	}
	err = s.ep.ReloadVolumeMap(s.Cmp, false)
	if err != nil {
		return nil, err
	}
	err = s.sp.ReloadVolumeMap(s.Cmp, false)
	if err != nil {
		return nil, err
	}
	cons := make([]*grpc.ClientConn, 0)

	for _, l := range s.Cmp {
		cons = append(cons, l)
	}

	err = ReloadEncryptionClient(cons)

	if err != nil {
		return nil, err
	}

	for _, l := range s.pcmp {
		if l != nil {
			l.Close()
		}
	}
	return &spb.ReloadConfigResponse{}, nil
}

func (s *PortRedictor) localWriteConfig() error {
	jsonFile, err := os.Open(s.config)
	if err != nil {
		return err
	}
	byteValue, err := ioutil.ReadAll(jsonFile)
	if err != nil {
		return err
	}
	var fes PortRedirectors
	err = json.Unmarshal(byteValue, &fes)
	if err != nil {
		log.Printf("unable to parse %s : %v", s.config, err)
		return err
	}
	fes.ListenPort = s.listenPort
	data, err := json.MarshalIndent(fes, "", " ")
	if err != nil {
		log.Printf("unable to marshal %s : %v", s.config, err)
		return err
	}
	err = ioutil.WriteFile(s.config, data, 0644)
	if err != nil {
		log.Printf("unable to write to file %s : %v", s.config, err)
		return err
	}
	return nil
}

func (s *PortRedictor) localReadConfig() error {
	jsonFile, err := os.Open(s.config)
	if err != nil {
		return err
	}
	byteValue, err := ioutil.ReadAll(jsonFile)
	if err != nil {
		return err
	}
	var fes PortRedirectors
	err = json.Unmarshal(byteValue, &fes)
	if err != nil {
		log.Printf("unable to parse %s : %v", s.config, err)
		return err
	}
	cmp := make(map[int64]*grpc.ClientConn)
	dd := make(map[int64]ForwardEntry)
	for _, fe := range fes.ForwardEntrys {
		Connection, err := pb.NewConnection(fe.Address, fe.Dedupe, fe.CompressData, -1, fe.CacheSize, fe.CacheAge)
		if err != nil {
			log.Errorf("Skipping because unable to connect to %s: %v\n", fe.Address, err)
		} else {
			cmp[Connection.Volumeid] = Connection.Clnt
			dd[Connection.Volumeid] = fe
		}
	}
	s.pcmp = nil
	for _, l := range s.Cmp {
		s.pcmp = append(s.pcmp, l)
	}
	s.Cmp = cmp
	s.Dd = dd
	s.pr = fes
	return nil
}

func (s *PortRedictor) GetProxyVolumes(ctx context.Context, req *spb.ProxyVolumeInfoRequest) (*spb.ProxyVolumeInfoResponse, error) {
	if s.portforwarder {
		return s.pc.GetProxyVolumes(ctx, req)
	}
	var vis []*spb.VolumeInfoResponse
	for id, con := range s.vp.vc {

		vi, err := con.GetVolumeInfo(ctx, &spb.VolumeInfoRequest{})
		if err != nil {
			log.Errorf("Error connecting to volume %d error:%v", id, err)
		} else if id != vi.SerialNumber {
			log.Warnf("Returned Volume Serial Number %d does not match locally recored %d\n", vi.SerialNumber, id)
		} else {
			vis = append(vis, vi)
		}
	}
	return &spb.ProxyVolumeInfoResponse{VolumeInfoResponse: vis}, nil
}

func NewPortRedirector(config string, listenPort string, portforwarder bool, clnt *grpc.ClientConn, debug bool) *PortRedictor {
	if debug {
		log.SetLevel(log.DebugLevel)
	}
	sc := &PortRedictor{config: config, listenPort: listenPort}
	if portforwarder {
		sc.portforwarder = true
		sc.pc = spb.NewPortRedirectorServiceClient(clnt)

	} else if len(config) > 0 {
		sc.localReadConfig()
		sc.localWriteConfig()
	}
	return sc

}
