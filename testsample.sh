export AZURE_ACCESS_KEY=<acces_key>
export AZURE_BUCKET_NAME=<bucket>
export AZURE_SECRET_KEY=<secret_key>
export SDFS_COPY_FILE_TO=<from>:<to>
rm cmd/portredirtest/logs/*.log
docker rm -f azure-6442 block-6442 s3-6442 minio
go test -v -timeout 90m -run TestMatrix//S3/testReconcileCloudMetadata github.com/opendedup/sdfs-proxy/cmd/portredirtest