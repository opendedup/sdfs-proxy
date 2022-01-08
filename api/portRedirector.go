package api

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"sync"

	pb "github.com/opendedup/sdfs-client-go/api"
	spb "github.com/opendedup/sdfs-client-go/sdfs"
	"google.golang.org/grpc"
)

type PortRedictor struct {
	spb.UnimplementedPortRedirectorServiceServer
	vp         *VolumeProxy
	ep         *SDFSEventProxy
	iop        *FileIOProxy
	config     string
	pr         PortRedirectors
	Cmp        map[int64]*grpc.ClientConn
	Dd         map[int64]bool
	configLock sync.RWMutex
}

type ForwardEntry struct {
	Pwd          string `json:"pwd"`
	User         string `json:"user"`
	Lpwd         string `json:"local-auth"`
	Address      string `json:"address" required:"true"`
	DisableTrust bool   `json:"trust-all"`
	Mtls         bool   `json:"mtls"`
	Mtlsca       string `json:"root-ca"`
	Mtlskey      string `json:"mtls-key"`
	Mtlscert     string `json:"mtls-cert"`
	Dedupe       bool   `json:"dedupe"`
}

type PortRedirectors struct {
	ForwardEntrys []ForwardEntry `json:"forwarders"`
}

func (s *PortRedictor) WriteConfig() error {
	file, _ := json.MarshalIndent(s.pr, "", " ")

	_ = ioutil.WriteFile(s.config, file, 0644)
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
		log.Printf("unable to parse %s", s.config)
		return err
	}
	cmp := make(map[int64]*grpc.ClientConn)
	dd := make(map[int64]bool)
	for _, fe := range fes.ForwardEntrys {
		Connection, err := pb.NewConnection(fe.Address, fe.Dedupe, -1)
		if err != nil {
			log.Fatalf("Unable to connect to %s: %v\n", fe.Address, err)
		}
		cmp[Connection.Volumeid] = Connection.Clnt
		dd[Connection.Volumeid] = fe.Dedupe
	}
	s.Cmp = cmp
	s.Dd = dd
	s.pr = fes
	return nil
}

func NewPortRedirector(config string) *PortRedictor {

	sc := &PortRedictor{config: config}
	if len(config) > 0 {
		sc.localReadConfig()
	}
	return sc

}
