FROM golang:1.8 as builder
WORKDIR /go/src/app
COPY API /go/src/app/API
ENV GOPATH $PWD
RUN go get github.com/lib/pq
RUN go get github.com/gorilla/mux
RUN go get github.com/fsnotify/fsnotify
RUN go get github.com/sirupsen/logrus
RUN go get github.com/spf13/viper
WORKDIR /go/src/app/API
RUN go build -o ferry_svc src/*.go 

FROM centos
RUN groupadd --gid 8816 ferry
RUN adduser --gid 8816 --uid 54136 ferry
RUN rpm -Uvh --nosignature https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
RUN rpm -Uvh --nosignature https://repo.opensciencegrid.org/osg/3.4/osg-3.4-el7-release-latest.rpm
RUN yum -y --nogpgcheck install osg-ca-certs
RUN yum -y --nogpgcheck install net-tools
RUN yum -y --nogpgcheck install bind-utils
WORKDIR /ferry
COPY .env ./default.yaml 
COPY API/myDN.list .
COPY --from=builder /go/src/app/API/ferry_svc .

RUN chown ferry.ferry *

USER ferry

CMD ./ferry_svc

