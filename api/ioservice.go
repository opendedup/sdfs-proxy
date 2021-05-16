package api

import (
	"context"
	"log"

	"github.com/opendedup/sdfs-client-go/dedupe"
	spb "github.com/opendedup/sdfs-client-go/sdfs"
	"google.golang.org/grpc"
)

type FileIOProxy struct {
	spb.UnimplementedFileIOServiceServer
	fc            spb.FileIOServiceClient
	dedupe        *dedupe.DedupeEngine
	dedupeEnabled bool
}

func (s *FileIOProxy) GetXAttrSize(ctx context.Context, req *spb.GetXAttrSizeRequest) (*spb.GetXAttrSizeResponse, error) {
	return s.fc.GetXAttrSize(ctx, req)

}

func (s *FileIOProxy) Fsync(ctx context.Context, req *spb.FsyncRequest) (*spb.FsyncResponse, error) {
	if s.dedupeEnabled {
		s.dedupe.SyncFile(req.Path)
	}
	return s.fc.Fsync(ctx, req)

}

func (s *FileIOProxy) SetXAttr(ctx context.Context, req *spb.SetXAttrRequest) (*spb.SetXAttrResponse, error) {
	return s.fc.SetXAttr(ctx, req)

}

func (s *FileIOProxy) RemoveXAttr(ctx context.Context, req *spb.RemoveXAttrRequest) (*spb.RemoveXAttrResponse, error) {
	return s.fc.RemoveXAttr(ctx, req)

}

func (s *FileIOProxy) GetXAttr(ctx context.Context, req *spb.GetXAttrRequest) (*spb.GetXAttrResponse, error) {
	return s.fc.GetXAttr(ctx, req)

}

func (s *FileIOProxy) Utime(ctx context.Context, req *spb.UtimeRequest) (*spb.UtimeResponse, error) {
	return s.fc.Utime(ctx, req)
}

func (s *FileIOProxy) Truncate(ctx context.Context, req *spb.TruncateRequest) (*spb.TruncateResponse, error) {
	if s.dedupeEnabled {
		s.dedupe.SyncFile(req.Path)
	}
	return s.fc.Truncate(ctx, req)

}
func (s *FileIOProxy) SymLink(ctx context.Context, req *spb.SymLinkRequest) (*spb.SymLinkResponse, error) {
	return s.fc.SymLink(ctx, req)

}
func (s *FileIOProxy) GetAttr(ctx context.Context, req *spb.StatRequest) (*spb.StatResponse, error) {
	if s.dedupeEnabled {
		s.dedupe.SyncFile(req.Path)
	}
	return s.fc.GetAttr(ctx, req)

}

func (s *FileIOProxy) ReadLink(ctx context.Context, req *spb.LinkRequest) (*spb.LinkResponse, error) {
	return s.fc.ReadLink(ctx, req)
}

func (s *FileIOProxy) Flush(ctx context.Context, req *spb.FlushRequest) (*spb.FlushResponse, error) {
	if s.dedupeEnabled {
		s.dedupe.Sync(req.Fd)
	}
	return s.fc.Flush(ctx, req)

}
func (s *FileIOProxy) Chown(ctx context.Context, req *spb.ChownRequest) (*spb.ChownResponse, error) {
	return s.fc.Chown(ctx, req)

}
func (s *FileIOProxy) MkDir(ctx context.Context, req *spb.MkDirRequest) (*spb.MkDirResponse, error) {
	return s.fc.MkDir(ctx, req)

}
func (s *FileIOProxy) RmDir(ctx context.Context, req *spb.RmDirRequest) (*spb.RmDirResponse, error) {
	return s.fc.RmDir(ctx, req)

}
func (s *FileIOProxy) Unlink(ctx context.Context, req *spb.UnlinkRequest) (*spb.UnlinkResponse, error) {
	return s.fc.Unlink(ctx, req)

}

func (s *FileIOProxy) Write(ctx context.Context, req *spb.DataWriteRequest) (*spb.DataWriteResponse, error) {
	if s.dedupeEnabled {
		err := s.dedupe.Write(req.FileHandle, req.Start, req.Data, req.Len)
		if err != nil {
			return nil, err
		} else {
			return &spb.DataWriteResponse{}, nil
		}
	}
	return s.fc.Write(ctx, req)
}
func (s *FileIOProxy) Read(ctx context.Context, req *spb.DataReadRequest) (*spb.DataReadResponse, error) {
	if s.dedupeEnabled {
		s.dedupe.Sync(req.FileHandle)
	}
	return s.fc.Read(ctx, req)

}
func (s *FileIOProxy) Release(ctx context.Context, req *spb.FileCloseRequest) (*spb.FileCloseResponse, error) {
	if s.dedupeEnabled {
		s.dedupe.Close(req.FileHandle)
	}
	return s.fc.Release(ctx, req)

}
func (s *FileIOProxy) Mknod(ctx context.Context, req *spb.MkNodRequest) (*spb.MkNodResponse, error) {
	return s.fc.Mknod(ctx, req)

}
func (s *FileIOProxy) Open(ctx context.Context, req *spb.FileOpenRequest) (*spb.FileOpenResponse, error) {
	rsp, err := s.fc.Open(ctx, req)
	if err != nil {
		return rsp, err
	} else if rsp.ErrorCode > 0 {
		return rsp, err
	}
	if s.dedupeEnabled {
		s.dedupe.Open(req.Path, rsp.FileHandle)
	}
	return rsp, nil

}
func (s *FileIOProxy) GetFileInfo(ctx context.Context, req *spb.FileInfoRequest) (*spb.FileMessageResponse, error) {
	if s.dedupeEnabled {
		s.dedupe.SyncFile(req.FileName)
	}
	return s.fc.GetFileInfo(ctx, req)

}
func (s *FileIOProxy) CreateCopy(ctx context.Context, req *spb.FileSnapshotRequest) (*spb.FileSnapshotResponse, error) {
	if s.dedupeEnabled {
		s.dedupe.SyncFile(req.Src)
		s.dedupe.SyncFile(req.Dest)
	}
	return s.fc.CreateCopy(ctx, req)

}
func (s *FileIOProxy) FileExists(ctx context.Context, req *spb.FileExistsRequest) (*spb.FileExistsResponse, error) {
	return s.fc.FileExists(ctx, req)

}
func (s *FileIOProxy) MkDirAll(ctx context.Context, req *spb.MkDirRequest) (*spb.MkDirResponse, error) {
	return s.fc.MkDirAll(ctx, req)

}
func (s *FileIOProxy) Stat(ctx context.Context, req *spb.FileInfoRequest) (*spb.FileMessageResponse, error) {
	if s.dedupeEnabled {
		s.dedupe.SyncFile(req.FileName)
	}
	return s.fc.Stat(ctx, req)

}
func (s *FileIOProxy) Rename(ctx context.Context, req *spb.FileRenameRequest) (*spb.FileRenameResponse, error) {
	if s.dedupeEnabled {
		s.dedupe.CloseFile(req.Src)
		s.dedupe.CloseFile(req.Dest)
	}
	return s.fc.Rename(ctx, req)

}
func (s *FileIOProxy) CopyExtent(ctx context.Context, req *spb.CopyExtentRequest) (*spb.CopyExtentResponse, error) {
	if s.dedupeEnabled {
		s.dedupe.SyncFile(req.SrcFile)
		s.dedupe.SyncFile(req.DstFile)
	}
	return s.fc.CopyExtent(ctx, req)

}
func (s *FileIOProxy) SetUserMetaData(ctx context.Context, req *spb.SetUserMetaDataRequest) (*spb.SetUserMetaDataResponse, error) {
	return s.fc.SetUserMetaData(ctx, req)

}
func (s *FileIOProxy) GetCloudFile(ctx context.Context, req *spb.GetCloudFileRequest) (*spb.GetCloudFileResponse, error) {
	return s.fc.GetCloudFile(ctx, req)

}
func (s *FileIOProxy) GetCloudMetaFile(ctx context.Context, req *spb.GetCloudFileRequest) (*spb.GetCloudFileResponse, error) {
	return s.fc.GetCloudFile(ctx, req)

}
func (s *FileIOProxy) StatFS(ctx context.Context, req *spb.StatFSRequest) (*spb.StatFSResponse, error) {

	return s.fc.StatFS(ctx, req)

}

func NewFileIOProxy(clnt *grpc.ClientConn, dedupeEnabled, debug bool) (*FileIOProxy, error) {
	fc := spb.NewFileIOServiceClient(clnt)
	sc := &FileIOProxy{fc: fc, dedupeEnabled: dedupeEnabled}

	if dedupeEnabled {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		de, err := dedupe.NewDedupeEngine(ctx, clnt, 4, 8, debug)
		if err != nil {
			log.Printf("error initializing dedupe connection: %v\n", err)
			return nil, err
		}
		sc.dedupe = de
	}
	return sc, nil

}
