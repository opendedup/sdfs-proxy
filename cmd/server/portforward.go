package main

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"

	ps "github.com/mitchellh/go-ps"
	"github.com/opendedup/sdfs-proxy/api"
	"github.com/sevlyar/go-daemon"
	log "github.com/sirupsen/logrus"
)

func NewPortForward(filepath string, enableAuth, standalone bool, port string, debug bool, lpwd string, args []string, remoteTls bool, logPath string, cachesize, cachage int) error {

	args = append(args, "-s")

	os.MkdirAll("/var/run/sdfs/", os.ModePerm)
	os.MkdirAll(logPath, os.ModePerm)
	p, err := ps.Processes()
	if err != nil {
		log.Errorf("error while trying to check processes %v", err)
	}
	if len(p) <= 0 {
		log.Errorf("should have processes during check but none found")
	}
	fndct := 0
	for _, p1 := range p {
		log.Debugf("process [%s] %v %v", p1, p1.Executable() == "sdfs-proxy-s.exe", p1.Executable() == "sdfs-proxy")
		if p1.Executable() == "sdfs-proxy" || p1.Executable() == "sdfs-proxy-s.exe" {
			fndct++
			log.Debugf("Found SDFS Proxy %s ct = %d", p1.Executable(), fndct)
		}
	}
	if fndct > 1 {
		log.Errorf("sdfs-proxy already started %d times", fndct-1)
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
		if runtime.GOOS == "windows" {
			sigs := make(chan os.Signal, 1)
			signal.Notify(sigs, syscall.SIGUSR1, syscall.SIGUSR2, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGQUIT)
			// go func() {
			// sig := <-sigs
			// fmt.Println(sig)
			// done <- true
			// }()
			go func() {
				for s := range sigs {
					switch s {
					case syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGQUIT:
						fmt.Println("got signal and try to exit: ", s)
						do_exit()
					case syscall.SIGUSR1:
						fmt.Println("usr1: ", s)
					case syscall.SIGUSR2:
						fmt.Println("usr2: ", s)
					default:
						fmt.Println("other: ", s)
					}
				}
			}()
		}
		pf := api.NewPortRedirector(filepath, port, false, nil)
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
			log.Errorf("failed to listen: %v", err)
			os.Exit(-11)
		}
		lis.Close()
		return addr, nil
	}
}

func do_exit() {
	fmt.Println("try do some clear jobs")
	fmt.Println("run done")
	os.Exit(0)
}

func removeIndex(s []string, index int) []string {
	return append(s[:index], s[index+1:]...)
}
