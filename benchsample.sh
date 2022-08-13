export AZURE_ACCESS_KEY=<akey>
export AZURE_BUCKET_NAME=<bucketname>
export AZURE_SECRET_KEY=<secret_key>
export SDFS_COPY_FILE_TO=<from>:<to>
rm cmd/portredirtest/logs/*.log
sudo rm -rf /opt/sdfs/volumes
docker rm -f azure-6442 block-6442 s3-6442 minio eblock-6442
go test -benchmem -run=^$ -bench BenchmarkWrites////1GB//parallelBenchmarkUpload1024  -timeout 900m github.com/opendedup/sdfs-proxy/cmd/portredirtest
#go test -benchmem -run=^$ -bench BenchmarkWrites/PROXY/BLOCK/0PercentUnique/100GB/WriteThreads1/parallelBenchmarkWrite1024 -timeout 900m github.com/opendedup/sdfs-proxy/cmd/portredirtest