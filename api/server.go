package api

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"os/user"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/dgrijalva/jwt-go"
	sdfs "github.com/opendedup/sdfs-client-go/sdfs"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var server *grpc.Server
var authenticate bool
var password string
var NOSHUTDOWN bool
var ServerMtls bool
var AnyCert bool
var ServerTls bool
var ServerKey string
var ServerCACert string
var ServerCert string
var serverConfigLock sync.RWMutex
var remoteTLS bool
var ecc []sdfs.EncryptionServiceClient

func StartServer(Connections map[int64]*grpc.ClientConn, port string, enableAuth bool, dedupe map[int64]ForwardEntry, proxy, debug bool, pwd string, pr *PortRedictor, remoteServerCert bool) {
	log.Debug("in")
	defer log.Debug("out")
	password = pwd
	authenticate = enableAuth
	if debug {
		log.SetLevel(log.DebugLevel)
	}
	log.SetOutput(os.Stdout)
	log.SetReportCaller(true)
	fc, err := NewFileIOProxy(Connections, dedupe, proxy, debug)
	if err != nil {
		log.Errorf("Unable to initialize dedupe enging while starting proxy server %v\n", err)
		os.Exit(7)
	}
	if remoteServerCert {
		remoteTLS = true
		var i int
		ecc = make([]sdfs.EncryptionServiceClient, len(Connections))
		for _, clnt := range Connections {
			ecc[i] = sdfs.NewEncryptionServiceClient(clnt)
			i++
		}

	}
	vc := NewVolumeProxy(Connections, pwd, proxy, debug)
	ec := NewEventProxy(Connections, proxy, debug)
	sc := NewStorageService(Connections, proxy, debug)
	if pr != nil {
		pr.iop = fc
		pr.ep = ec
		pr.vp = vc
		pr.sp = sc
	}
	ps := strings.Split(port, ":")
	var lis net.Listener
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
			lis, err = net.Listen("tcp", fmt.Sprintf("%s:%d", ps[0], i))
			if err != nil {
				log.Warnf("failed to listen on %d : %v", i, err)
			} else {
				port = fmt.Sprintf("%s:%d", ps[0], i)
				break
			}
			if i == ep {
				log.Errorf("Unable to find open port")
				os.Exit(10)
			}
		}
	} else {
		lis, err = net.Listen("tcp", port)
		if err != nil {
			log.Errorf("failed to listen: %v", err)
			os.Exit(-11)
		}
	}
	maxMsgSize := 240 * 1024 * 1024 //240 MB
	if ServerTls || ServerMtls {
		cc, err := LoadKeyPair(ServerMtls, AnyCert, remoteServerCert)
		if err != nil {
			log.Errorf("failed to load certs: %v", err)
			os.Exit(12)
		}
		server = grpc.NewServer(grpc.Creds(*cc), grpc.UnaryInterceptor(serverInterceptor), grpc.StreamInterceptor(serverStreamInterceptor),
			grpc.MaxRecvMsgSize(maxMsgSize), grpc.MaxSendMsgSize(maxMsgSize), grpc.WriteBufferSize(0), grpc.ReadBufferSize(0))
	} else {
		server = grpc.NewServer(grpc.UnaryInterceptor(serverInterceptor), grpc.StreamInterceptor(serverStreamInterceptor),
			grpc.MaxRecvMsgSize(maxMsgSize), grpc.MaxSendMsgSize(maxMsgSize))
	}

	sdfs.RegisterVolumeServiceServer(server, vc)
	sdfs.RegisterFileIOServiceServer(server, fc)
	sdfs.RegisterSDFSEventServiceServer(server, ec)
	sdfs.RegisterStorageServiceServer(server, sc)
	sdfs.RegisterPortRedirectorServiceServer(server, pr)
	fmt.Printf("Listening on %s auth enabled %v, dedupe enabled %v\n", port, enableAuth, dedupe)
	log.Infof("Listening on %s auth enabled %v, dedupe enabled %v\n", port, enableAuth, dedupe)
	fmt.Println("proxy ready")
	if err := server.Serve(lis); err != nil {
		log.Errorf("failed to serve: %v", err)
		os.Exit(13)
	}
}

func ReloadEncryptionClient(conn []*grpc.ClientConn) error {
	serverConfigLock.Lock()
	defer serverConfigLock.Unlock()
	ecc = make([]sdfs.EncryptionServiceClient, len(conn))
	for i, clnt := range conn {
		ecc[i] = sdfs.NewEncryptionServiceClient(clnt)
	}
	return nil
}

func LoadKeyPair(mtls, anycert bool, rtls bool) (*credentials.TransportCredentials, error) {
	var certificate tls.Certificate
	if rtls {
		var err error

		for _, clnt := range ecc {
			ctx, cancel := context.WithCancel(context.Background())
			ms, err := clnt.ExportServerCertificate(ctx, &sdfs.ExportServerCertRequest{})
			cancel()
			if ms.GetErrorCode() > 0 {
				log.Errorf("unable to validate cert %d %s", ms.ErrorCode, ms.Error)
				return nil, fmt.Errorf("unable to validate cert %d %s", ms.ErrorCode, ms.Error)
			} else {
				certificate, err = tls.X509KeyPair(ms.CertChain, ms.PrivateKey)
				if err != nil {
					return nil, err
				}
			}
		}
		if err != nil {
			return nil, err
		}
	} else {
		user, err := user.Current()
		if err != nil {
			return nil, err
		}
		if len(ServerCert) == 0 {
			ServerCert = user.HomeDir + "/.sdfs/server.crt"
		}
		if len(ServerKey) == 0 {
			ServerKey = user.HomeDir + "/.sdfs/server.key"
		}
		if len(ServerCACert) == 0 {
			ServerCACert = user.HomeDir + "/.sdfs/ca.crt"
		}
		certificate, err = tls.LoadX509KeyPair(ServerCert, ServerKey)
		if err != nil {
			return nil, err
		}
	}

	tlsConfig := &tls.Config{
		ClientAuth:   tls.NoClientCert,
		Certificates: []tls.Certificate{certificate},
	}
	if mtls {

		if anycert {
			tlsConfig.ClientAuth = tls.RequireAnyClientCert
			tlsConfig.VerifyPeerCertificate = customVerify
		} else {
			data, err := ioutil.ReadFile(ServerCACert)
			if err != nil {
				return nil, err
			}
			capool := x509.NewCertPool()
			if !capool.AppendCertsFromPEM(data) {
				return nil, err
			}
			tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
			tlsConfig.ClientCAs = capool

		}

	}
	cr := credentials.NewTLS(tlsConfig)
	return &cr, nil
}

func customVerify(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
	log.Debug("in")
	defer log.Debug("out")
	serverConfigLock.RLock()
	defer serverConfigLock.RUnlock()
	for i := 0; i < len(rawCerts); i++ {
		cert, err := x509.ParseCertificate(rawCerts[i])

		if err != nil {
			log.Error("Error: ", err)
			continue
		}

		hash := sha256.Sum256(rawCerts[i])
		hashs := hex.EncodeToString(hash[:])
		if remoteTLS {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var accept *sdfs.EncryptionKeyVerifyResponse
			for _, cl := range ecc {
				accept, err = cl.ValidateCertificate(ctx, &sdfs.EncryptionKeyVerifyRequest{Hash: hashs})
				if err == nil {
					break
				}
			}
			log.Debugf("Fingerprint: %s, accept: %v", hashs, accept)

			log.Debug(cert.DNSNames, cert.Subject)
			if err != nil {
				log.Errorf("error while getting cert %v", err)
				return err
			} else if accept.GetErrorCode() > 0 {
				log.Errorf("unable to validate cert %d %s", accept.ErrorCode, accept.Error)
				return fmt.Errorf("unable to validate cert %d %s", accept.ErrorCode, accept.Error)
			} else if !accept.Accept {
				log.Warnf("certificate not accepted %s %s", cert.DNSNames, cert.Subject)
				return fmt.Errorf("certificate not accepted %s %s", cert.DNSNames, cert.Subject)
			}
		} else {
			log.Debugf("Fingerprint: %s", hashs)

			log.Debug(cert.DNSNames, cert.Subject)
		}
	}
	return nil
}

func serverInterceptor(ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler) (interface{}, error) {
	log.Debug("Intercepted Call = %s", info.FullMethod)
	if authenticate {
		// Skip authorize when GetJWT is requested

		if info.FullMethod != "/org.opendedup.grpc.VolumeService/AuthenticateUser" {
			if err := authorize(ctx); err != nil {
				//log.Printf("error %v", err)
				return nil, err
			}
		}
	}

	// Calls the handler
	h, err := handler(ctx, req)

	return h, err
}

func serverStreamInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	//log.Infof("Interceptor = %s", info.FullMethod)
	if authenticate {
		if info.FullMethod != "/org.opendedup.grpc.VolumeService/AuthenticateUser" {
			if err := authorize(ss.Context()); err != nil {
				return err
			}
		}
	}
	err := handler(srv, ss)
	return err

}

func authorize(ctx context.Context) error {

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Errorf(codes.InvalidArgument, "Retrieving metadata is failed")
	}

	authHeader, ok := md["authorization"]
	if !ok {
		return status.Errorf(codes.Unauthenticated, "Authorization token is not supplied")
	}

	ah := authHeader[0]
	tokens := strings.Split(ah, " ")
	if len(tokens) == 2 && strings.ToLower(tokens[0]) == "bearer" {
		jwtoken := tokens[1]
		token, err := jwt.ParseWithClaims(
			jwtoken,
			&customClaims{},
			func(token *jwt.Token) (interface{}, error) {
				return []byte(password), nil
			},
		)
		if err != nil {
			//log.Printf("unable to load jwt %v token: %s", err, jwtoken)
			return status.Errorf(codes.Unauthenticated, "invalid token")
		}
		claims, ok := token.Claims.(*customClaims)
		if !ok {
			return status.Errorf(codes.Unauthenticated, "invalid token")
		}
		if claims.ExpiresAt < time.Now().UTC().Unix() {
			return status.Errorf(codes.Unauthenticated, "invalid token")
		}

	} else {
		return status.Errorf(codes.Unauthenticated, "missing metadata")
	}

	// validateToken function validates the token

	return nil
}

func StopServer() {
	server.Stop()
}
