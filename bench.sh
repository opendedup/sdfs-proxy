export AZURE_ACCESS_KEY=sdfs1
export AZURE_BUCKET_NAME=sdfstest
export AZURE_SECRET_KEY=BKsLJBhSlCb/WeylwXabq7prtO1s1WpVhCpoMf+ZqFidGmAMKEINptva0+mUhzTVC4+4IVNWy9Rb+AStl1xDcA==
export SDFS_COPY_FILE_TO=/home/samsilverberg/git/sdfs/target/sdfs-master.jar:/usr/share/sdfs/lib/sdfs.jar
rm cmd/portredirtest/logs/*.log
sudo rm -rf /opt/sdfs/volumes
docker rm -f azure-6442 block-6442 s3-6442 minio
go test -benchmem -run=^$ -bench BenchmarkWrites//BLOCK/  -timeout 900m github.com/opendedup/sdfs-proxy/cmd/portredirtest
