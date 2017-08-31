package main
import (
//"fmt"
//"log"
 _ "github.com/lib/pq"
"net/http"
//"encoding/json"
)

func getPasswdFile(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query() 
//	collabunit := q.Get("unitname")
//	resource := q.Get("resourcename")
	NotDoneYet(w)
}
func getGroupFile(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query()
//	collabunit := q.Get("unitname")
	NotDoneYet(w)
}
func getGridmapFile(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query()
//	collabunit := q.Get("unitname")
	NotDoneYet(w)
}
func getVORoleMapFile(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query()
	NotDoneYet(w)
}

func getUserUID(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query() 
//	uname := q.Get("username")
	NotDoneYet(w)
}
func getUserUname(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query() 
//	uid := int(q.Get("uid"))
	NotDoneYet(w)
}
func getGroupGID(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query() 
//	groupname := q.Get("groupname")
//	inc_gid := bool(q.Get("include_gid"))
	NotDoneYet(w)
}
func getGroupName(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query() 
//	gid := int(q.Get("gid"))
	NotDoneYet(w)
}
func lookupCertificateDN(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query() 
//	certDN := q.Get("certificatedn")
	NotDoneYet(w)
}
