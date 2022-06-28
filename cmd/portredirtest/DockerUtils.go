package test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/docker/cli/cli/command"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	network "github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/archive"
	"github.com/docker/docker/pkg/system"
	natting "github.com/docker/go-connections/nat"
	log "github.com/sirupsen/logrus"
)

func CopyToContainer(ctx context.Context, container, srcPath, dstPath string) (err error) {
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

func RunContainer(ctx context.Context, imagename string, containername string, hostPort, port string, inputEnv []string, cmd []string, copyFile bool) (string, error) {
	client, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	client.NegotiateAPIVersion(ctx)
	newport, err := natting.NewPort("tcp", port)
	if err != nil {
		fmt.Println("Unable to create docker port")
		return "", err
	}

	// Configured hostConfig:
	// https://godoc.org/github.com/docker/docker/api/types/container#HostConfig
	hostConfig := &container.HostConfig{
		NetworkMode: "testnw",
		PortBindings: natting.PortMap{
			newport: []natting.PortBinding{
				{
					HostIP:   "0.0.0.0",
					HostPort: hostPort,
				},
			},
		},
		RestartPolicy: container.RestartPolicy{
			Name: "always",
		},
		LogConfig: container.LogConfig{
			Type:   "json-file",
			Config: map[string]string{},
		},
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
		Image:        imagename,
		Env:          inputEnv,
		ExposedPorts: exposedPorts,
		Hostname:     containername,
	}
	if len(cmd) > 0 {
		config.Cmd = cmd
	}

	// Creating the actual container. This is "nil,nil,nil" in every example.
	cont, err := client.ContainerCreate(
		context.Background(),
		config,
		hostConfig,
		networkConfig, nil,
		containername,
	)
	if err != nil {
		log.Error(err)
		return "", err
	}
	val, present := os.LookupEnv("SDFS_COPY_FILE_TO")
	if present && copyFile {
		s := strings.Split(val, ":")
		log.Infof("copy data %s to %s:%s", s[0], containername, s[1])
		err = CopyToContainer(ctx, containername, s[0], s[1])
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

	log.Printf("Container %s is created", cont.ID)
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
		fn := fmt.Sprintf("logs/%s-%s.log", containername, cont.ID)
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
		log.Infof("exiting reader for %s", fn)
	}()

	if err != nil {
		log.Fatal(err)
	}

	return cont.ID, nil
}

func StopAndRemoveContainer(ctx context.Context, containername string) error {
	client, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		log.Fatal(err)
	}
	client.NegotiateAPIVersion(ctx)

	defer client.Close()
	log.Printf("Stopping container %s", containername)
	if err := client.ContainerStop(ctx, containername, nil); err != nil {
		log.Printf("Unable to stop container %s: %s", containername, err)
	}

	removeOptions := types.ContainerRemoveOptions{
		RemoveVolumes: true,
		Force:         true,
	}

	if err := client.ContainerRemove(ctx, containername, removeOptions); err != nil {
		log.Printf("Unable to remove container: %s", err)
		return err
	}

	return nil
}
