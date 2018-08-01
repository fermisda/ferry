# Must run 
# git clone ssh://p-ferment@cdcvs.fnal.gov/cvs/projects/ferment ferry/ferment
# first in this dir with your kerberos credentials
FROM golang:1.8 as builder
WORKDIR /go/src/app
COPY . .
ENV GOPATH $PWD
RUN go get github.com/lib/pq
RUN go get github.com/gorilla/mux
RUN go get github.com/fsnotify/fsnotify
RUN go get github.com/sirupsen/logrus
RUN go get github.com/spf13/viper
WORKDIR /go/src/app/ferment/API
RUN go build -o bin/main src/*.go 

FROM centos
RUN groupadd --gid 8816 ferry
RUN adduser --gid 8816 --uid 54136 ferry
RUN rpm -Uvh --nosignature https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
RUN rpm -Uvh --nosignature https://repo.opensciencegrid.org/osg/3.4/osg-3.4-el7-release-latest.rpm
RUN yum -y --nogpgcheck install osg-ca-certs
RUN yum -y --nogpgcheck install net-tools
RUN yum -y --nogpgcheck install bind-utils
WORKDIR /ferry
COPY default.yaml .
COPY hostcert.pem .
COPY hostkey.pem  .
COPY myDN.list .
COPY --from=builder /go/src/app/ferment/API/bin/main .

RUN chown ferry.ferry *

USER ferry

CMD ./main

