package main

import (
	"os"
	"runtime"
	"strings"

	ps "github.com/mitchellh/go-ps"
	"github.com/opendedup/sdfs-proxy/api"
	"github.com/sevlyar/go-daemon"
	log "github.com/sirupsen/logrus"
)

func NewPortForward(filepath string, enableAuth, standalone bool, port string, debug bool, lpwd string, args []string, remoteTls bool) error {
	pf := api.NewPortRedirector(filepath)
	args = append(args, "-s")

	os.MkdirAll("/var/run/sdfs/", os.ModePerm)
	os.MkdirAll("/var/log/sdfs/", os.ModePerm)
	p, err := ps.Processes()
	if err != nil {
		log.Errorf("error while trying to check processes %v", err)
	}
	if len(p) <= 0 {
		log.Errorf("should have processes during check but none found")
	}
	fndct := 0
	for _, p1 := range p {
		if p1.Executable() == "sdfs-proxy" || p1.Executable() == "sdfs.proxy-s.exe" {
			fndct++
		}
	}
	if fndct > 1 {
		log.Errorf("sdfs-proxy already started %d times", fndct-1)
		os.Exit(14)
	}

	if !standalone && runtime.GOOS != "windows" {
		pidFile := "/var/run/sdfs/portforwarder-" + strings.ReplaceAll(port, ":", "-") + ".pid"
		logFile := "/var/log/sdfs/portforwarder-" + strings.ReplaceAll(port, ":", "-") + ".log"
		mcntxt := &daemon.Context{
			PidFileName: pidFile,
			PidFilePerm: 0644,
			LogFileName: logFile,
			LogFilePerm: 0640,
			WorkDir:     "/var/run/",
			Umask:       027,
			Args:        args,
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
	} else {
		api.StartServer(pf.Cmp, port, enableAuth, pf.Dd, false, debug, lpwd, pf, remoteTls)
	}

	return nil

}
