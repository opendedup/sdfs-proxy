package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"strings"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	pb "github.com/opendedup/sdfs-client-go/api"
	"github.com/opendedup/sdfs-proxy/api"
	"github.com/sevlyar/go-daemon"
)

var Version = "development"
var BuildDate = "NAN"

func main() {
	pwd := flag.String("p", "Password", "The Password to authenticate to the remote Volume")
	user := flag.String("u", "Admin", "The Username to authenticate to the remote Volume")
	lpwd := flag.String("local-auth", "admin", "Sets the local volume to authenticate to the given password")
	address := flag.String("address", "sdfss://localhost:6442", "The address for the Remote Volume")
	disableTrust := flag.Bool("trust-all", false, "Trust Self Signed TLS Certs")
	version := flag.Bool("version", false, "Get the version number")
	trustCert := flag.Bool("trust-cert", false, "Trust the certificate for url specified. This will download and store the certificate in $HOME/.sdfs/keys")
	port := flag.String("listen-port", "localhost:16442", "The Port to listen on for proxy requests. To try a range of ports use syntax hostame:startport-endport e.g"+
		" localhost:16442:16445 .")
	stls := flag.Bool("server-tls", false, "Use TLS for listening port. This will use the certs located in $HOME/.sdfs/keys/[server.crt,server.key,server.crt]"+
		"unless otherwise specified")
	mstls := flag.Bool("server-mtls", false, "Use MTLS for listening port. This will use the certs located in $HOME/.sdfs/keys/[server.crt,server.key,server.crt]"+
		"unless otherwise specified")
	smtlsca := flag.String("server-root-ca", "", "The path the CA cert used to sign the MTLS Cert. This defaults to $HOME/.sdfs/keys/ca.crt")
	smtlskey := flag.String("server-tls-key", "", "The path the private key used for TLS. This defaults to $HOME/.sdfs/keys/server.key")
	smtlscert := flag.String("server-tls-cert", "", "The path the server cert used for TLS. This defaults to $HOME/.sdfs/keys/server.crt")
	rtls := flag.Bool("server-tls-remotercert", false, "Use Remote Server Cert")
	mtls := flag.Bool("mtls", false, "Use Mutual TLS. This will use the certs located in $HOME/.sdfs/keys/[client.crt,client.key,ca.crt]"+
		"unless otherwise specified")
	mtlsca := flag.String("root-ca", "", "The path the CA cert used to sign the MTLS Cert. This defaults to $HOME/.sdfs/keys/ca.crt")
	mtlskey := flag.String("mtls-key", "", "The path the private used for mutual TLS. This defaults to $HOME/.sdfs/keys/client.key")
	mtlscert := flag.String("mtls-cert", "", "The path the client cert used for mutual TLS. This defaults to $HOME/.sdfs/keys/client.crt")
	nocompress := flag.Bool("nocompress", true, "Compress api traffic")
	dedupe := flag.Bool("dedupe", false, "Enable Client Side Dedupe")
	debug := flag.Bool("debug", false, "Debug to stdout")
	standalone := flag.Bool("s", false, "do not daemonize mount")
	pfConfig := flag.String("pf-config", "", "The location of the Port forward Config")
	flag.Parse()
	enableAuth := false
	log.Infof("read %v", os.Args)
	log.Infof(" config %s", *pfConfig)
	if *version {
		fmt.Printf("Version : %s\n", Version)
		fmt.Printf("Build Date: %s\n", BuildDate)
		os.Exit(0)
	}

	if *trustCert {
		err := pb.AddTrustedCert(*address)
		if err != nil {
			log.Errorf("Unable to download cert from (%s): %v\n", *address, err)
			os.Exit(6)
		}
	}
	if *disableTrust {
		fmt.Println("TLS Verification Disabled")
		pb.DisableTrust = *disableTrust
	}
	if isFlagPassed("p") {
		pb.UserName = *user
		pb.Password = *pwd

	}
	if isFlagPassed("root-ca") {
		pb.MtlsCACert = *mtlsca
	}
	if isFlagPassed("local-auth") {
		enableAuth = true
	}
	if isFlagPassed("mtls-key") {
		pb.MtlsKey = *mtlskey
	}
	if isFlagPassed("mtls-cert") {
		pb.MtlsCert = *mtlscert
	}
	if isFlagPassed("server-root-ca") {
		api.ServerCACert = *smtlsca
	}
	if isFlagPassed("server-tls-key") {
		api.ServerKey = *smtlskey
	}
	if isFlagPassed("server-tls-cert") {
		api.ServerCert = *smtlscert
	}
	if *stls {
		api.ServerTls = *stls

	}
	if *mstls {
		api.ServerMtls = *mstls
		api.AnyCert = true

	}
	if *mtls {
		//fmt.Println("Using Mutual TLS")
		pb.Mtls = *mtls
	}

	if isFlagPassed("pf-config") {
		fmt.Printf("Reading %s\n", *pfConfig)
		NewPortForward(*pfConfig, enableAuth, *standalone, *port, *debug, *lpwd, os.Args, *rtls)

	} else {
		Connection, err := pb.NewConnection(*address, *dedupe, !*nocompress, -1)
		fmt.Printf("Connected to %s\n", *address)
		if err != nil {
			log.Errorf("Unable to connect to %s: %v\n", *address, err)
			os.Exit(4)
		}
		os.MkdirAll("/var/run/sdfs/", os.ModePerm)
		os.MkdirAll("/var/log/sdfs/", os.ModePerm)
		if !*standalone && runtime.GOOS != "windows" {
			args := os.Args
			args = append(args, "-s")
			pidFile := "/var/run/sdfs/proxy-" + strings.ReplaceAll(*port, ":", "-") + ".pid"
			logFile := "/var/log/sdfs/proxy-" + strings.ReplaceAll(*port, ":", "-") + ".log"
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
				return
			}
			defer mcntxt.Release()
			cmp := make(map[int64]*grpc.ClientConn)
			cmp[Connection.Volumeid] = Connection.Clnt
			dd := make(map[int64]api.ForwardEntry)
			dd[Connection.Volumeid] = api.ForwardEntry{
				Address:       *address,
				Dedupe:        *dedupe,
				DedupeThreads: 1,
				DedupeBuffer:  4,
				CompressData:  !*nocompress,
			}
			api.StartServer(cmp, *port, enableAuth, dd, true, *debug, *lpwd, nil, false)
		} else {
			cmp := make(map[int64]*grpc.ClientConn)
			cmp[Connection.Volumeid] = Connection.Clnt
			dd := make(map[int64]api.ForwardEntry)
			dd[Connection.Volumeid] = api.ForwardEntry{
				Address:       *address,
				Dedupe:        *dedupe,
				DedupeThreads: 1,
				DedupeBuffer:  4,
			}
			api.StartServer(cmp, *port, enableAuth, dd, true, *debug, *lpwd, nil, false)
		}
	}
}

//StartServer starts the grpc service

func isFlagPassed(name string) bool {
	found := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}
