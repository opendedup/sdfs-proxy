package api

import (
	"context"

	spb "github.com/opendedup/sdfs-client-go/sdfs"
	"google.golang.org/grpc"
)

type SDFSEventProxy struct {
	spb.UnimplementedSDFSEventServiceServer
	evt spb.SDFSEventServiceClient
}

func (s *SDFSEventProxy) GetEvent(ctx context.Context, req *spb.SDFSEventRequest) (*spb.SDFSEventResponse, error) {
	return s.evt.GetEvent(ctx, req)
}

func (s *SDFSEventProxy) ListEvents(ctx context.Context, req *spb.SDFSEventListRequest) (*spb.SDFSEventListResponse, error) {
	return s.evt.ListEvents(ctx, req)

}

func (s *SDFSEventProxy) SubscribeEvent(req *spb.SDFSEventRequest, stream spb.SDFSEventService_SubscribeEventServer) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fi, err := s.evt.SubscribeEvent(ctx, req)
	if err != nil {
		return err
	}
	for {
		event, err := fi.Recv()
		if err != nil {
			return err

		}
		if err := stream.Send(event); err != nil {
			return err
		}
	}
}

func NewEventProxy(clnt *grpc.ClientConn) *SDFSEventProxy {
	evt := spb.NewSDFSEventServiceClient(clnt)
	sc := &SDFSEventProxy{evt: evt}
	return sc

}
