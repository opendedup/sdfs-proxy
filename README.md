# sdfs-proxy

SDFS Proxy is a proxy server that intercepts sdfs client api calls and proxies them to the backend SDFS volume. It provides Client Side Dedupe and access to most apis available on the server.

## Example Usage

### Connecting to a local volume listening on localhost

``` bash
sdfs-proxy -listen-port localhost:16442
```

### Connecting to a remote volume

```bash
sdfs-proxy -listen-port localhost:16442 -address sdfss://remoteserver:6442 -trust-all
```


### Enabling Client Side Dedupe

```bash
sdfs-proxy -listen-port localhost:16442 -dedupe
```

## Command Line Options

|Option | Description | Default value|
|-------|--------------|-------------|
|-address|The address for the Remote Volume|sdfss://localhost:6442|
|-debug| Debug to stdout| disabled|
|-dedupe|Enable Client Side Dedupe|disabled|
|-listen-port|  The Port to listen on for proxy requests|localhost:16442|
|-local-auth| Sets the local volume to authenticate to the given password| admin (disabled by default)|
|-mtls|Use Mutual TLS| disabled|
|-mtls-cert|The path the client cert used for mutual TLS.| $HOME/.sdfs/keys/client.crt|
|-mtls-key|The path the private used for mutual TLS.|$HOME/.sdfs/keys/client.key|
|-p|The Password to authenticate to the remote Volume|Password|
|-root-ca|The path the CA cert used to sign the MTLS Cert.| $HOME/.sdfs/keys/ca.crt|
|-trust-all|Trust Self Signed TLS Certs|disabled|
|-trust-cert|Trust the certificate for url specified.|$HOME/.sdfs/keys|
|-u|The Username to authenticate to the remote Volume |Admin|
|-version| Get the version number||

## Building 

``` bash
git clone https://github.com/opendedup/sdfs-proxy.git
cd sdfs-proxy
make clean
make
cd build
./sdfs-proxy -h
```


## Unsupported Operations

1. Client to proxy TLS encryption
2. Passthru authentication to the Target volume. the client only authenticates to the proxy itself. The proxy authenticates independenly to the remote SDFS volume.



## Supported APIs

### FileIOServiceClient
* GetXAttrSize
* Fsync
* SetXAttr
* RemoveXAttr
* GetXAttr
* Utime
* Truncate
* SymLink (linux only)
* GetAttr
* ReadLink
* Flush
* Chown
* MkDir
* RmDir
* Unlink
* Write - With deduplication writes will be cached locally and flushed to the target volume as buffer fills and on close.
* Read
* Release
* Mknod
* Open
* GetFileInfo
* CreateCopy
* FileExists
* MkDirAll
* Stat
* Rename
* CopyExtent
* SetUserMetaData
* GetCloudFile
* GetCloudMetaFile
* StatFS

### VolumeServiceClient
* AuthenticateUser - The user is authenticated to the proxy by using the -local-auth switch
* SetMaxAge
* GetVolumeInfo
* ShutdownVolume
* CleanStore
* DeleteCloudVolume
* DSEInfo
* SystemInfo
* SetVolumeCapacity
* GetConnectedVolumes
* GetGCSchedule
* SetCacheSize
* SetReadSpeed
* SetWriteSpeed
* SyncFromCloudVolume
* SyncCloudVolume

### SDFSEventServiceClient
* GetEvent
* ListEvents
* SubscribeEvent
