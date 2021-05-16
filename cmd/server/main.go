package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	pb "github.com/opendedup/sdfs-client-go/api"
	"github.com/opendedup/sdfs-proxy/api"
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
	port := flag.String("listen-port", "localhost:16442", "The Port to listen on for proxy requests")
	mtls := flag.Bool("mtls", false, "Use Mutual TLS. This will use the certs located in $HOME/.sdfs/keys/[client.crt,client.key,ca.crt]"+
		"unless otherwise specified")
	mtlsca := flag.String("root-ca", "", "The path the CA cert used to sign the MTLS Cert. This defaults to $HOME/.sdfs/keys/ca.crt")
	mtlskey := flag.String("mtls-key", "", "The path the private used for mutual TLS. This defaults to $HOME/.sdfs/keys/client.key")
	mtlscert := flag.String("mtls-cert", "", "The path the client cert used for mutual TLS. This defaults to $HOME/.sdfs/keys/client.crt")
	dedupe := flag.Bool("dedupe", false, "Enable Client Side Dedupe")
	debug := flag.Bool("debug", false, "Debug to stdout")
	flag.Parse()
	enableAuth := false
	if *version {
		fmt.Printf("Version : %s\n", Version)
		fmt.Printf("Build Date: %s\n", BuildDate)
		os.Exit(0)
	}
	if *trustCert {
		err := pb.AddTrustedCert(*address)
		if err != nil {
			log.Fatalf("Unable to download cert from (%s): %v\n", *address, err)
		}
	}
	if *disableTrust {
		fmt.Println("TLS Verification Disabled")
		pb.DisableTrust = *disableTrust
	}
	if isFlagPassed("pwd") {
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
	if *mtls {
		//fmt.Println("Using Mutual TLS")
		pb.Mtls = *mtls
	}
	//fmt.Printf("Connecting to %s\n", *address)
	Connection, err := pb.NewConnection(*address, *dedupe)
	if err != nil {
		log.Fatalf("Unable to connect to %s: %v\n", *address, err)
	}

	api.StartServer(Connection, *port, enableAuth, *dedupe, *debug, *lpwd)

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
