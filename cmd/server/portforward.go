package main

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/opendedup/sdfs-proxy/api"
	"github.com/sevlyar/go-daemon"
	log "github.com/sirupsen/logrus"
)

func NewPortForward(configFilepath string, enableAuth, standalone bool, port string, debug bool, lpwd string, args []string, remoteTls bool, logPath string, cachesize, cachage int) error {

	args = append(args, "-s")

	os.MkdirAll("/var/run/sdfs/", os.ModePerm)
	os.MkdirAll(logPath, os.ModePerm)
	var fn = -1
	for i, arg := range args {
		if arg == "-listen-port" {
			fn = i
			break
		}
	}
	if fn >= 0 {
		args = removeIndex(args, fn)
		args = removeIndex(args, fn)
	}
	port, _ = testPort(port)
	args = append(args, "-listen-port")
	args = append(args, port)
	log.Debugf("print %v", args)
	log.Infof("Listening on : %s", port)
	if !standalone && runtime.GOOS != "windows" {
		pidFile := "/var/run/sdfs/portforwarder-" + strings.ReplaceAll(port, ":", "-") + ".pid"
		logFile := logPath + "portforwarder-" + strings.ReplaceAll(port, ":", "-") + ".log"
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
		if runtime.GOOS == "windows" {
			lockFile := fmt.Sprintf("%s\\LOCK", filepath.Dir(configFilepath))
			if _, err := os.Stat(lockFile); err == nil {
				log.Errorf("Looks like port forwarder is already started %s exists", lockFile)
				os.Exit(20)
			}
			d1 := []byte(fmt.Sprintf("%d", os.Getpid()))
			err := os.WriteFile(lockFile, d1, 0644)
			if err != nil {
				log.Errorf("Unable to write lock file: %s %v \n", lockFile, err)
			}
			c := make(chan os.Signal, 1)
			signal.Notify(c, os.Interrupt)
			go func() {
				for sig := range c {
					do_exit(lockFile, sig)
				}
			}()

		}
		pf := api.NewPortRedirector(configFilepath, port, false, nil)
		api.StartServer(pf.Cmp, port, enableAuth, pf.Dd, false, debug, lpwd, pf, remoteTls)
	}

	return nil

}

func testPort(addr string) (string, error) {
	ps := strings.Split(addr, ":")
	if strings.Contains(ps[1], "-") {
		pts := strings.Split(ps[1], "-")
		sp, err := strconv.Atoi(pts[0])
		if err != nil {
			log.Errorf("failed to parse: %s %v", pts[0], err)
			os.Exit(8)
		}
		ep, err := strconv.Atoi(pts[1])
		if err != nil {
			log.Errorf("failed to parse: %s %v", pts[1], err)
			os.Exit(9)
		}
		for i := sp; i < ep+1; i++ {
			lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", ps[0], i))
			if err != nil {
				lis.Close()
				log.Warnf("failed to listen on %d : %v", i, err)
			} else {
				lis.Close()
				lis, err = net.Listen("tcp", fmt.Sprintf("%s:%d", "localhost", i))
				if err == nil {
					lis.Close()
					port := fmt.Sprintf("%s:%d", ps[0], i)
					return port, nil
				}
				log.Warnf("failed to listen on localhost %d : %v", i, err)
			}
			if i == ep {
				log.Errorf("Unable to find open port")
				os.Exit(10)
			}
		}
		return "", fmt.Errorf("port not found in range %s", addr)
	} else {
		lis, err := net.Listen("tcp", addr)
		if err != nil {
			log.Errorf("failed to listen at %s %v", addr, err)
			os.Exit(-11)
		}
		lis.Close()
		lis, err = net.Listen("tcp", fmt.Sprintf("%s:%s", "localhost", ps[1]))
		if err == nil {
			lis.Close()
			port := fmt.Sprintf("%s:%s", ps[0], ps[1])
			return port, nil
		}
		log.Errorf("failed to listen at %s %v", addr, err)
		os.Exit(-11)

	}
	return addr, nil
}

func do_exit(lockFile string, sig os.Signal) {
	os.Remove(lockFile)
	os.Exit(0)
}

func removeIndex(s []string, index int) []string {
	return append(s[:index], s[index+1:]...)
}
