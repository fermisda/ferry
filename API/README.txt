K. Herner 2017-08-31
Some background information on the Go-based API for FERRY

Installation

1) Clone the git repo: git clone ssh://p-ferment@cdcvs.fnal.gov/cvs/projects/ferment

2) cd API

3) go get github.com/gorrila/mux ; go get github.com/lib/pq

4) mkdir bin

5) go build -o bin/main src/main.go src/userAPI.go src/groupAPI.go src/miscAPI.go src/unitAPI.go

Deployment

6) You have an executable called main in the bin directory. Go ahead and try it out. It contains its own server. Right now there's no authentication and it will probably be accessible only on the local host. 

Example usage:  $ curl -i "http://localhost:8080/getgroups?uname=YOUR_USERNAME"

