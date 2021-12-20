package main

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"runtime"
	"strings"

	pb "github.com/opendedup/sdfs-client-go/api"
	"github.com/opendedup/sdfs-proxy/api"
	"github.com/sevlyar/go-daemon"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

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

type PortRedirector struct {
	ForwardEntrys []ForwardEntry `json:"forwarders"`
}

func NewPortForward(filepath string, enableAuth, standalone bool, port string, debug bool, lpwd string) error {
	jsonFile, err := os.Open(filepath)
	if err != nil {
		return err
	}
	var fes PortRedirector
	byteValue, err := ioutil.ReadAll(jsonFile)
	if err != nil {
		return err
	}
	err = json.Unmarshal(byteValue, &fes)
	if err != nil {
		log.Printf("unable to parse %s", filepath)
		return err
	}
	cmp := make(map[int64]*grpc.ClientConn)
	dd := make(map[int64]bool)
	os.MkdirAll("/var/run/sdfs/", os.ModePerm)
	os.MkdirAll("/var/log/sdfs/", os.ModePerm)
	for _, fe := range fes.ForwardEntrys {
		Connection, err := pb.NewConnection(fe.Address, fe.Dedupe, -1)
		if err != nil {
			log.Fatalf("Unable to connect to %s: %v\n", fe.Address, err)
		}
		cmp[Connection.Volumeid] = Connection.Clnt
		dd[Connection.Volumeid] = fe.Dedupe
	}
	if !standalone && runtime.GOOS != "windows" {
		pidFile := "/var/run/sdfs/proxy-" + strings.ReplaceAll(port, ":", "-") + ".pid"
		logFile := "/var/log/sdfs/proxy-" + strings.ReplaceAll(port, ":", "-") + ".log"
		mcntxt := &daemon.Context{
			PidFileName: pidFile,
			PidFilePerm: 0644,
			LogFileName: logFile,
			LogFilePerm: 0640,
			WorkDir:     "/var/run/",
			Umask:       027,
		}

		d, err := mcntxt.Reborn()
		if err != nil {
			log.Errorf("Unable to run: %v \n", err)
			os.Exit(3)
		}
		if d != nil {
			return nil
		}
		defer mcntxt.Release()
		api.StartServer(cmp, port, enableAuth, dd, debug, lpwd)
	} else {
		api.StartServer(cmp, port, enableAuth, dd, debug, lpwd)
	}

	return nil

}
