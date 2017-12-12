package main
import (
	"strings"
	"database/sql"
	"fmt"
	"log"
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
	q := r.URL.Query()

	fqan := q.Get("fqan")
	mGroup := q.Get("mapped_group")
	var mUser sql.NullString

	if fqan == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No fqan specified in http query.")
		fmt.Fprintf(w,"{ \"error\": \"No fqan specified.\" }")
		return
	}
	if mGroup == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No mapped_group specified in http query.")
		fmt.Fprintf(w,"{ \"error\": \"No mapped_group specified.\" }")
		return
	}
	if q.Get("mapped_user") != "" {
		mUser.Scan(q.Get("mapped_user"))
	}

	cKey, err := DBtx.Start(DBptr)
	if err != nil {
		log.Fatal(err)
	}

	_, err = DBtx.Exec("insert into grid_fqan (fqan, mapped_user, mapped_group, last_updated) values ($1, $2, $3, NOW())", fqan, mUser, mGroup)
	if err == nil {
		fmt.Fprintf(w,"{ \"status\": \"success\" }")
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		if strings.Contains(err.Error(), `violates foreign key constraint "fk_experiment_fqan_users"`) {
			fmt.Fprintf(w,"{ \"error\": \"User doesn't exist.\" }")
		} else if strings.Contains(err.Error(), `violates foreign key constraint "fk_experiment_fqan_groups"`) {
			fmt.Fprintf(w,"{ \"error\": \"Group doesn't exist.\" }")
		} else if strings.Contains(err.Error(), `duplicate key value violates unique constraint`) {
			fmt.Fprintf(w,"{ \"error\": \"FQAN already exists.\" }")
		} else {
			log.Print(err.Error())
		}
	}

	DBtx.Commit(cKey)
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
