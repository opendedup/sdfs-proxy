FROM golang:alpine3.15

LABEL maintainer="Sam Silverberg  <sam.silverberg@gmail.com>"

ENV GOPATH /go
ENV CGO_ENABLED 0
ENV GO111MODULE on
COPY ./ /go/sdfs-proxy

RUN  \
     apk add --no-cache git build-base && \
     cd /go/sdfs-proxy && \
     mkdir -p /go/sdfs-proxy/build
WORKDIR /go/sdfs-proxy/
RUN go mod tidy
RUN make clean && make build
