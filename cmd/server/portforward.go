package main

import (
	"os"
	"runtime"
	"strings"

	"github.com/opendedup/sdfs-proxy/api"
	"github.com/sevlyar/go-daemon"
	log "github.com/sirupsen/logrus"
)

func NewPortForward(filepath string, enableAuth, standalone bool, port string, debug bool, lpwd string, args []string, remoteTls bool) error {
	pf := api.NewPortRedirector(filepath)
	args = append(args, "-s")

	os.MkdirAll("/var/run/sdfs/", os.ModePerm)
	os.MkdirAll("/var/log/sdfs/", os.ModePerm)
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

		api.StartServer(pf.Cmp, port, enableAuth, pf.Dd, false, debug, lpwd, pf, remoteTls)
	} else {
		api.StartServer(pf.Cmp, port, enableAuth, pf.Dd, false, debug, lpwd, pf, remoteTls)
	}

	return nil

}
