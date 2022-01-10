package api

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net"
	"os/user"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/dgrijalva/jwt-go"
	sdfs "github.com/opendedup/sdfs-client-go/sdfs"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
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

func StartServer(Connections map[int64]*grpc.ClientConn, port string, enableAuth bool, dedupe map[int64]bool, proxy, debug bool, pwd string, pr *PortRedictor) {
	password = pwd
	authenticate = enableAuth
	fc, err := NewFileIOProxy(Connections, dedupe, proxy, debug)
	if err != nil {
		log.Fatalf("Unable to initialize dedupe enging while starting proxy server %v\n", err)
	}
	vc := NewVolumeProxy(Connections, pwd, proxy)
	ec := NewEventProxy(Connections, proxy)
	if pr != nil {
		pr.iop = fc
		pr.ep = ec
		pr.vp = vc
	}
	ps := strings.Split(port, ":")
	var lis net.Listener
	if strings.Contains(ps[1], "-") {
		pts := strings.Split(ps[1], "-")
		sp, err := strconv.Atoi(pts[0])
		if err != nil {
			log.Fatalf("failed to parse: %s %v", pts[0], err)
		}
		ep, err := strconv.Atoi(pts[1])
		if err != nil {
			log.Fatalf("failed to parse: %s %v", pts[1], err)
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
				log.Fatalf("Unable to find open port")
			}
		}
	} else {
		lis, err = net.Listen("tcp", port)
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}
	}
	maxMsgSize := 2097152 * 40
	if ServerTls || ServerMtls {
		cc, err := LoadKeyPair(ServerMtls, AnyCert)
		if err != nil {
			log.Fatalf("failed to load certs: %v", err)
		}
		server = grpc.NewServer(grpc.Creds(*cc), grpc.UnaryInterceptor(serverInterceptor), grpc.StreamInterceptor(serverStreamInterceptor),
			grpc.MaxRecvMsgSize(maxMsgSize), grpc.MaxSendMsgSize(maxMsgSize))
	} else {
		server = grpc.NewServer(grpc.UnaryInterceptor(serverInterceptor), grpc.StreamInterceptor(serverStreamInterceptor),
			grpc.MaxRecvMsgSize(maxMsgSize), grpc.MaxSendMsgSize(maxMsgSize))
	}

	sdfs.RegisterVolumeServiceServer(server, vc)
	sdfs.RegisterFileIOServiceServer(server, fc)
	sdfs.RegisterSDFSEventServiceServer(server, ec)
	if pr != nil {
		sdfs.RegisterPortRedirectorServiceServer(server, pr)
	}
	fmt.Printf("Listening on %s auth enabled %v, dedupe enabled %v\n", port, enableAuth, dedupe)
	log.Infof("Listening on %s auth enabled %v, dedupe enabled %v\n", port, enableAuth, dedupe)
	fmt.Println("proxy ready")
	if err := server.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

}

func LoadKeyPair(mtls, anycert bool) (*credentials.TransportCredentials, error) {
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
	certificate, err := tls.LoadX509KeyPair(ServerCert, ServerKey)
	if err != nil {
		return nil, err
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
	log.Info("Verify certs")
	fmt.Print("Verify certs")
	for i := 0; i < len(rawCerts); i++ {
		cert, err := x509.ParseCertificate(rawCerts[i])

		if err != nil {
			log.Error("Error: ", err)
			continue
		}

		hash := sha256.Sum256(rawCerts[i])
		log.Infof("Fingerprint: %x", hash)

		log.Info(cert.DNSNames, cert.Subject)
	}
	return nil
}

func serverInterceptor(ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler) (interface{}, error) {
	log.Infof("Interceptor = %s", info.FullMethod)
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
	log.Infof("Interceptor = %s", info.FullMethod)
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
