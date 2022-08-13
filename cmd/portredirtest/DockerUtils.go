package test

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/docker/cli/cli/command"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
	network "github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/archive"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/docker/docker/pkg/system"
	natting "github.com/docker/go-connections/nat"
	log "github.com/sirupsen/logrus"
)

var sdfsimagename = "gcr.io/hybrics/hybrics:dp3"

func copyToContainer(ctx context.Context, container, srcPath, dstPath string) (err error) {
	client, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	client.NegotiateAPIVersion(ctx)
	dstInfo := archive.CopyInfo{Path: dstPath}
	dstStat, err := client.ContainerStatPath(ctx, container, dstPath)
	if err == nil && dstStat.Mode&os.ModeSymlink != 0 {
		linkTarget := dstStat.LinkTarget
		if !system.IsAbs(linkTarget) {
			dstParent, _ := archive.SplitPathDirEntry(dstPath)
			linkTarget = filepath.Join(dstParent, linkTarget)
		}

		dstInfo.Path = linkTarget
		dstStat, err = client.ContainerStatPath(ctx, container, linkTarget)
	}

	if err := command.ValidateOutputPathFileMode(dstStat.Mode); err != nil {
		return fmt.Errorf(`destination "%s:%s" must be a directory or a regular file error:%v`, container, dstPath, err)
	}
	if err == nil {
		dstInfo.Exists, dstInfo.IsDir = true, dstStat.Mode.IsDir()
	}
	var (
		content         io.ReadCloser
		resolvedDstPath string
	)

	if srcPath == "-" {
		content = os.Stdin
		resolvedDstPath = dstInfo.Path
		if !dstInfo.IsDir {
			return fmt.Errorf("destination \"%s:%s\" must be a directory", container, dstPath)
		}
	} else {
		// Prepare source copy info.
		srcInfo, err := archive.CopyInfoSourcePath(srcPath, false)
		if err != nil {
			return err
		}

		srcArchive, err := archive.TarResource(srcInfo)
		if err != nil {
			return err
		}
		defer srcArchive.Close()
		dstDir, preparedArchive, err := archive.PrepareArchiveCopy(srcArchive, srcInfo, dstInfo)
		if err != nil {
			return err
		}
		defer preparedArchive.Close()

		resolvedDstPath = dstDir
		content = preparedArchive
	}

	options := types.CopyToContainerOptions{
		AllowOverwriteDirWithFile: false,
		CopyUIDGID:                false,
	}

	return client.CopyToContainer(ctx, container, resolvedDstPath, content, options)
}

func RunContainer(ctx context.Context, cfg *containerConfig) (string, error) {
	log.Debugf(" config is %v", cfg)
	client, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	client.NegotiateAPIVersion(ctx)
	newport, err := natting.NewPort("tcp", cfg.containerPort)
	if err != nil {
		fmt.Println("Unable to create docker port")
		return "", err
	}
	sw := int64(-1)

	// Configured hostConfig:
	// https://godoc.org/github.com/docker/docker/api/types/container#HostConfig
	hostConfig := &container.HostConfig{
		NetworkMode: "testnw",
		PortBindings: natting.PortMap{
			newport: []natting.PortBinding{
				{
					HostIP:   "0.0.0.0",
					HostPort: cfg.hostPort,
				},
			},
		},
		RestartPolicy: container.RestartPolicy{
			Name: "always",
		},
		Resources: container.Resources{
			MemorySwappiness: &sw,
		},

		LogConfig: container.LogConfig{
			Type:   "json-file",
			Config: map[string]string{},
		},
	}
	if cfg.attachProfiler {
		profilingPort, err := natting.NewPort("tcp", "8849")
		if err != nil {
			fmt.Println("Unable to create docker port")
			return "", err
		}

		hostConfig.PortBindings[profilingPort] = []natting.PortBinding{
			{
				HostIP:   "0.0.0.0",
				HostPort: "8849",
			},
		}
	}
	if cfg.memory > 0 {
		hostConfig.Resources.Memory = cfg.memory
		hostConfig.Resources.MemorySwap = cfg.memory
	}
	if cfg.cpu > 0 {
		hostConfig.Resources.NanoCPUs = cfg.cpu * 1000000000
	}
	if cfg.mountstorage {
		hostConfig.Mounts = []mount.Mount{
			{
				Type:   mount.TypeBind,
				Source: "/opt/",
				Target: "/opt/",
			},
		}
	}

	// Define Network config (why isn't PORT in here...?:
	// https://godoc.org/github.com/docker/docker/api/types/network#NetworkingConfig
	networkConfig := &network.NetworkingConfig{
		EndpointsConfig: map[string]*network.EndpointSettings{},
	}
	gatewayConfig := &network.EndpointSettings{
		Gateway: "gatewayname",
	}
	networkConfig.EndpointsConfig["testnw"] = gatewayConfig

	// Define ports to be exposed (has to be same as hostconfig.portbindings.newport)
	exposedPorts := map[natting.Port]struct{}{
		newport: {},
	}

	// Configuration
	// https://godoc.org/github.com/docker/docker/api/types/container#Config
	config := &container.Config{
		Image:        cfg.imagename,
		Env:          cfg.inputEnv,
		ExposedPorts: exposedPorts,
		Hostname:     cfg.containername,
	}
	if len(cfg.cmd) > 0 {
		config.Cmd = cfg.cmd
	}

	// Creating the actual container. This is "nil,nil,nil" in every example.
	cont, err := client.ContainerCreate(
		context.Background(),
		config,
		hostConfig,
		networkConfig, nil,
		cfg.containername,
	)
	if err != nil {
		log.Error(err)
		return "", err
	}
	val, present := os.LookupEnv("SDFS_COPY_FILE_TO")
	if present && cfg.copyFile {
		s := strings.Split(val, ":")
		log.Debugf("copy data %s to %s:%s", s[0], cfg.containername, s[1])
		err = copyToContainer(ctx, cfg.containername, s[0], s[1])
		if err != nil {
			log.Error(err)
			return "", err
		}
	}

	// Run the actual container
	err = client.ContainerStart(context.Background(), cont.ID, types.ContainerStartOptions{})
	if err != nil {
		log.Println(err)
		return "", err
	}
	log.Debugf("Container %s is created", cont.ID)
	go func() {
		reader, err := client.ContainerLogs(context.Background(), cont.ID, types.ContainerLogsOptions{
			ShowStdout: true,
			ShowStderr: true,
			Follow:     true,
			Timestamps: false,
		})
		if err != nil {
			log.Fatal(err)
		}
		defer reader.Close()
		path := "logs"
		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			err := os.Mkdir(path, os.ModePerm)
			if err != nil {
				log.Println(err)
			}
		}
		fn := fmt.Sprintf("logs/%s-%s.log", cfg.containername, cont.ID)
		f, err := os.Create(fn)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		w := bufio.NewWriter(f)
		scanner := bufio.NewScanner(reader)
		for scanner.Scan() {
			p := scanner.Bytes()
			if len(p) > 8 {
				_, err := w.WriteString(fmt.Sprintln(string(p[8:])))
				if err != nil {
					log.Errorf("unable to write to %s", fn)
					log.Error(err)
				}
				err = w.Flush()
				if err != nil {
					log.Errorf("unable to flush to %s", fn)
					log.Error(err)
				}
			}
		}
		log.Debugf("exiting reader for %s", fn)
	}()

	if err != nil {
		log.Fatal(err)
	}

	return cont.ID, nil
}

func CreateAzureSetup(ctx context.Context, cfg *containerConfig) (*testRun, error) {
	credential, err := azblob.NewSharedKeyCredential(os.Getenv("AZURE_ACCESS_KEY"), os.Getenv("AZURE_SECRET_KEY"))
	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})
	u, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net", os.Getenv("AZURE_ACCESS_KEY")))
	serviceURL := azblob.NewServiceURL(*u, p)
	containerURL := serviceURL.NewContainerURL(os.Getenv("AZURE_BUCKET_NAME"))
	for marker := (azblob.Marker{}); marker.NotDone(); { // The parens around Marker{} are required to avoid compiler error.
		// Get a result segment starting with the blob indicated by the current Marker.
		listBlob, err := containerURL.ListBlobsFlatSegment(ctx, marker, azblob.ListBlobsSegmentOptions{})
		if err != nil {
			log.Fatal(err)
		}
		// IMPORTANT: ListBlobs returns the start of the next segment; you MUST use this to get
		// the next segment (after processing the current result segment).
		marker = listBlob.NextMarker

		// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, blobInfo := range listBlob.Segment.BlobItems {
			blobURL := containerURL.NewBlockBlobURL(blobInfo.Name)
			_, err = blobURL.Delete(ctx, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	if err != nil {
		log.Fatal(err)
	}
	cfg.imagename = sdfsimagename

	cfg.inputEnv = []string{"TYPE=AZURE", "BACKUP_VOLUME=true", fmt.Sprintf("ACCESS_KEY=%s", os.Getenv("AZURE_ACCESS_KEY")), fmt.Sprintf("BUCKET_NAME=%s", os.Getenv("AZURE_BUCKET_NAME")), fmt.Sprintf("ACCESS_KEY=%s", os.Getenv("AZURE_ACCESS_KEY")), fmt.Sprintf("SECRET_KEY=%s", os.Getenv("AZURE_SECRET_KEY")), fmt.Sprintf("CAPACITY=%s", "1TB")}
	cfg.inputEnv = append(cfg.inputEnv, "DISABLE_TLS=true")
	if cfg.encrypt {
		cfg.inputEnv = append(cfg.inputEnv, "EXTENDED_CMD=--hashtable-rm-threshold=1000 --chunk-store-encrypt=true")
	} else {
		cfg.inputEnv = append(cfg.inputEnv, "EXTENDED_CMD=--hashtable-rm-threshold=1000")
	}
	cfg.cmd = []string{}
	cfg.copyFile = true
	cfg.containerPort = "6442"
	_, err = RunContainer(ctx, cfg)
	if err != nil {
		fmt.Printf("Unable to create docker client %v", err)
		return nil, fmt.Errorf("Unable to create docker client %v", err)
	}
	aztr := &testRun{url: fmt.Sprintf("sdfs://localhost:%s", cfg.hostPort), name: "azurestorage", cfg: cfg, cloudVol: true}
	return aztr, nil
}

func CreateS3Setup(ctx context.Context, cfg *containerConfig) (*testRun, error) {
	mcfg := &containerConfig{
		containername: "minio",
		imagename:     "docker.io/minio/minio:latest",
		hostPort:      "9000",
		containerPort: "9000",
		inputEnv:      []string{fmt.Sprintf("MINIO_ROOT_USER=%s", "MINIO"), fmt.Sprintf("MINIO_ROOT_PASSWORD=%s", "MINIO1234")},
		cmd:           []string{"server", "/data"},
	}
	_, err := RunContainer(ctx, mcfg)
	if err != nil {
		return nil, err
	}
	s3bucket := "pool0"
	cfg.imagename = sdfsimagename
	cfg.containerPort = "6442"

	cfg.inputEnv = []string{fmt.Sprintf("CAPACITY=%s", "1TB"), "BACKUP_VOLUME=true", "EXTENDED_CMD=--hashtable-rm-threshold=1000 --aws-disable-dns-bucket=true --minio-enabled",
		fmt.Sprintf("TYPE=%s", "AWS"), fmt.Sprintf("URL=%s", "http://minio:9000"), fmt.Sprintf("BUCKET_NAME=%s", s3bucket),
		fmt.Sprintf("ACCESS_KEY=%s", "MINIO"), fmt.Sprintf("SECRET_KEY=%s", "MINIO1234")}
	cfg.inputEnv = append(cfg.inputEnv, "DISABLE_TLS=true")
	if cfg.encrypt {
		cfg.inputEnv = append(cfg.inputEnv, "EXTENDED_CMD=--hashtable-rm-threshold=1000 --aws-disable-dns-bucket=true --minio-enabled --chunk-store-encrypt=true")
	} else {
		cfg.inputEnv = append(cfg.inputEnv, "EXTENDED_CMD=--hashtable-rm-threshold=1000 --aws-disable-dns-bucket=true --minio-enabled")
	}
	cfg.cmd = []string{}
	cfg.copyFile = true
	_, err = RunContainer(ctx, cfg)
	if err != nil {
		return nil, err
	}
	s3tr := &testRun{url: fmt.Sprintf("sdfs://localhost:%s", cfg.hostPort), name: "s3storage", cfg: cfg, cloudVol: true}
	return s3tr, nil

}

func CreateBlockSetup(ctx context.Context, cfg *containerConfig) (*testRun, error) {
	cfg.inputEnv = []string{"BACKUP_VOLUME=true", fmt.Sprintf("CAPACITY=%s", "1TB")}
	cfg.inputEnv = append(cfg.inputEnv, "DISABLE_TLS=true")
	if cfg.attachProfiler {
		cfg.inputEnv = append(cfg.inputEnv, "JAVA_EXT_CMD=-agentpath:/opt/jprofiler13/bin/linux-x64/libjprofilerti.so=port=8849,nowait")
	}
	if cfg.encrypt {
		cfg.inputEnv = append(cfg.inputEnv, "EXTENDED_CMD=--hashtable-rm-threshold=1000 --chunk-store-encrypt=true")
	} else {
		cfg.inputEnv = append(cfg.inputEnv, "EXTENDED_CMD=--hashtable-rm-threshold=1000 --io-chunk-size=20480 --io-max-file-write-buffers=40")
	}
	cfg.imagename = sdfsimagename
	cfg.containerPort = "6442"
	cfg.copyFile = true
	cfg.cmd = []string{}
	_, err := RunContainer(ctx, cfg)
	if err != nil {
		return nil, err
	}
	btr := &testRun{url: fmt.Sprintf("sdfs://localhost:%s", cfg.hostPort), name: "blockstorage", cfg: cfg, cloudVol: false}
	log.Infof("config=%v", cfg)
	return btr, nil
}

func StopAndRemoveContainer(ctx context.Context, containername string) error {
	client, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		log.Fatal(err)
	}
	client.NegotiateAPIVersion(ctx)

	defer client.Close()
	log.Debugf("Stopping container %s", containername)
	if err := client.ContainerStop(ctx, containername, nil); err != nil {
		log.Debugf("Unable to stop container %s: %s", containername, err)
	}

	removeOptions := types.ContainerRemoveOptions{
		RemoveVolumes: true,
		Force:         true,
	}

	if err := client.ContainerRemove(ctx, containername, removeOptions); err != nil {
		log.Debugf("Unable to remove container: %s", err)
		return err
	}
	return nil
}

type ExecResult struct {
	ExitCode  int
	outBuffer *bytes.Buffer
	errBuffer *bytes.Buffer
}

func DockerExec(ctx context.Context, id string, cmd []string) (ExecResult, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()
	cli.NegotiateAPIVersion(ctx)
	// prepare exec
	execConfig := types.ExecConfig{
		AttachStdout: true,
		AttachStderr: true,
		Cmd:          cmd,
	}
	cresp, err := cli.ContainerExecCreate(ctx, id, execConfig)
	if err != nil {
		return ExecResult{}, err
	}
	execID := cresp.ID

	// run it, with stdout/stderr attached
	aresp, err := cli.ContainerExecAttach(ctx, execID, types.ExecStartCheck{})
	if err != nil {
		return ExecResult{}, err
	}
	defer aresp.Close()

	// read the output
	var outBuf, errBuf bytes.Buffer
	outputDone := make(chan error)

	go func() {
		// StdCopy demultiplexes the stream into two buffers
		_, err = stdcopy.StdCopy(&outBuf, &errBuf, aresp.Reader)
		outputDone <- err
	}()

	select {
	case err := <-outputDone:
		if err != nil {
			return ExecResult{}, err
		}
		break

	case <-ctx.Done():
		return ExecResult{}, ctx.Err()
	}

	// get the exit code
	iresp, err := cli.ContainerExecInspect(ctx, execID)
	if err != nil {
		return ExecResult{}, err
	}

	return ExecResult{ExitCode: iresp.ExitCode, outBuffer: &outBuf, errBuffer: &errBuf}, nil
}
