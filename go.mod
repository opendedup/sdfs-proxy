module github.com/opendedup/sdfs-proxy

go 1.16

//replace github.com/opendedup/sdfs-client-go => /home/samsilverberg/git/sdfs-client-go

require (
	github.com/Azure/azure-storage-blob-go v0.15.0
	github.com/dgrijalva/jwt-go v3.2.0+incompatible
	github.com/docker/cli v20.10.17+incompatible
	github.com/docker/docker v20.10.17+incompatible
	github.com/docker/docker-credential-helpers v0.6.4 // indirect
	github.com/docker/go-connections v0.4.0
	github.com/fvbommel/sortorder v1.0.2 // indirect
	github.com/kardianos/osext v0.0.0-20190222173326-2bc1f35cddc0 // indirect
	github.com/moby/sys/mount v0.3.3 // indirect
	github.com/opendedup/sdfs-client-go v0.1.37-0.20220707040926-8a628a548e77
	github.com/sevlyar/go-daemon v0.1.5
	github.com/shirou/gopsutil/v3 v3.22.1
	github.com/sirupsen/logrus v1.8.1
	github.com/stretchr/testify v1.7.1
	github.com/theupdateframework/notary v0.7.0 // indirect
	golang.org/x/crypto v0.0.0-20220511200225-c6db032c6c88
	google.golang.org/grpc v1.40.1
)
