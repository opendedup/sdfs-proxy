package api

import (
	"context"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"github.com/dgrijalva/jwt-go"
	pb "github.com/opendedup/sdfs-client-go/api"
	sdfs "github.com/opendedup/sdfs-client-go/sdfs"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var server *grpc.Server
var authenticate bool
var password string
var NOSHUTDOWN bool

func StartServer(Connection *pb.SdfsConnection, port string, enableAuth, dedupe, debug bool, pwd string) {
	password = pwd
	authenticate = enableAuth
	fc, err := NewFileIOProxy(Connection.Clnt, dedupe, debug)
	if err != nil {
		log.Fatalf("Unable to initialize dedupe enging while starting proxy server %v\n", err)
	}
	vc := NewVolumeProxy(Connection.Clnt, pwd)
	ec := NewEventProxy(Connection.Clnt)
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	server = grpc.NewServer(grpc.UnaryInterceptor(serverInterceptor), grpc.StreamInterceptor(serverStreamInterceptor))
	sdfs.RegisterVolumeServiceServer(server, vc)
	sdfs.RegisterFileIOServiceServer(server, fc)
	sdfs.RegisterSDFSEventServiceServer(server, ec)
	if err := server.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
	fmt.Printf("Listening on %s auth enabled %v, dedupe enabled %v", port, enableAuth, dedupe)
	fmt.Printf("proxy ready\n")
}

func serverInterceptor(ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler) (interface{}, error) {
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
