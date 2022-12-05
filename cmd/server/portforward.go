package main

import (
	"fmt"
	"math"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"

	"github.com/opendedup/sdfs-proxy/api"
	"github.com/sevlyar/go-daemon"
	"github.com/shirou/gopsutil/v3/process"
	log "github.com/sirupsen/logrus"
)

func NewPortForward(configFilepath string, enableAuth, standalone bool, port string, debug bool, lpwd string, args []string, remoteTls bool, logPath string, cachesize, cachage int) error {
	log.SetOutput(os.Stdout)
	log.SetReportCaller(true)
	args = append(args, "-s")
	if runtime.GOOS != "windows" {
		os.MkdirAll("/var/run/sdfs/", os.ModePerm)
		os.MkdirAll(logPath, os.ModePerm)
	}
	p, err := process.Processes()
	if err != nil {
		log.Errorf("error while trying to list processes %v", err)
	}
	if len(p) <= 0 {
		log.Errorf("should have processes during check but none found")
	}
	fndct := 0
	ctime := int64(math.MaxInt64)
	var cmdline string
	for _, p1 := range p {
		exe, err := p1.Exe()

		if err != nil {
			log.Debugf("error while trying to check exe %v", err)
		}
		nc, err := p1.Cmdline()
		if err != nil {
			log.Debugf("error while trying to check commandline %v", err)
		}
		if runtime.GOOS == "windows" {
			if strings.Contains(exe, "sdfs-proxy-s") && strings.Contains(nc, "-pf-config") {
				fndct++
				log.Infof("Found SDFS Proxy %s ct = %d", exe, fndct)
				log.Infof("SDFS Command Line is %s", nc)
				ct, _ := p1.CreateTime()
				if ct < ctime {
					ctime = ct
					cmdline = nc
				}
			}
		} else if strings.Contains(exe, "sdfs-proxy") && strings.Contains(nc, "-pf-config") {
			fndct++
			log.Infof("Found SDFS Proxy %s ct = %d", exe, fndct)
			log.Infof("SDFS Command Line is %s", nc)
			ct, _ := p1.CreateTime()
			if ct < ctime {
				ctime = ct
				cmdline = nc

			}
		}
	}
	if fndct > 1 {
		log.Errorf("sdfs-proxy already started %d times", fndct-1)
		log.Errorf("cmd %s", cmdline)
		os.Exit(14)
	}
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
		pf := api.NewPortRedirector(configFilepath, port, false, nil, debug)
		api.StartServer(pf.Cmp, pf.Cmppool, port, enableAuth, pf.Dd, false, debug, lpwd, pf, remoteTls)
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
				if lis != nil {
					lis.Close()
				}
				log.Warnf("failed to listen on %d : %v", i, err)
			} else {
				if lis != nil {
					lis.Close()
				}
				lis, err = net.Listen("tcp", fmt.Sprintf("%s:%d", "localhost", i))
				if err == nil {
					if lis != nil {
						lis.Close()
					}
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
		if lis != nil {
			lis.Close()
		}
		lis, err = net.Listen("tcp", fmt.Sprintf("%s:%s", "localhost", ps[1]))
		if err == nil {
			if lis != nil {
				lis.Close()
			}
			port := fmt.Sprintf("%s:%s", ps[0], ps[1])
			return port, nil
		}
		if lis != nil {
			lis.Close()
		}
		log.Errorf("failed to listen at %s %v", addr, err)
		os.Exit(-11)

	}
	return addr, nil
}

func removeIndex(s []string, index int) []string {
	return append(s[:index], s[index+1:]...)
}
