FROM golang:1.13-alpine

LABEL maintainer="Sam Silverberg  <sam.silverberg@gmail.com>"

ENV GOPATH /go
ENV CGO_ENABLED 0
ENV GO111MODULE on
COPY ./ /go/sdfs-proxy

RUN  \
     apk add --no-cache git build-base && \
     cd /go/sdfs-proxy && \
     mkdir -p /go/sdfs-proxy/build
WORKDIR /go/sdfs-client-go/
RUN make clean && make build
