package main
import (
//"fmt"
//"log"
 _ "github.com/lib/pq"
"net/http"
//"encoding/json"
)

func createCollaborationUnit(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query()
//	unitname := q.Get("unitname")
//	voms_url := q.Get("voms_url")
//	alternative_name := q.Get("alternative_name")
	NotDoneYet(w)
	//requires auth
}
func removeCollaborationUnit(w http.ResponseWriter, r *http.Request) { 
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query()
//	unitname := q.Get("unitname")	
	NotDoneYet(w)
	//requires auth
}

func setCollaborationUnitInfo(w http.ResponseWriter, r *http.Request) { 
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query()
//	unitname := q.Get("unitname")
//	voms_url := q.Get("voms_url")
//	alternative_name := q.Get("alternative_name")
	NotDoneYet(w)
	/// Requires AUTH
}

func getCollaborationUnitMembers(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query()
//	collabunit := q.Get("unitname")
	NotDoneYet(w)
}

func getGroupsInCollaborationUnit(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query() 
//	collabunit := q.Get("unitname")
	NotDoneYet(w)
}

func getGroupLeadersinCollaborationUnit(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query()
//	collabunit := q.Get("unitname")
	NotDoneYet(w)
}

func getCollaborationUnitStorageResources(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query()
//	collabunit := q.Get("unitname")
	NotDoneYet(w)
}

func getCollaborationUnitComputeResources(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query()
//	collabunit := q.Get("unitname")
	NotDoneYet(w)
}

func createFQAN(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query()
//	fqan := q.Get("fqan")
//	mapuser := q.Get("mapped_user")
//	mapgroup := q.Get("mapped_group")
	NotDoneYet(w)
}

func removeFQAN(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query()
//	fqan := q.Get("fqan")
	NotDoneYet(w)
}

func setFQANMappings(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query()
//	fqan := q.Get("fqan")
//	mapuser := q.Get("mapped_user")
//	mapgroup := q.Get("mapped_group")
	NotDoneYet(w)
}
