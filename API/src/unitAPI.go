package main
import (
	"strings"
	"database/sql"
	"fmt"
	"log"
	_ "github.com/lib/pq"
	"net/http"
	"encoding/json"
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

func getGroupsInAffiliationUnit(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	unitName := q.Get("unitname")

	if unitName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No unit name specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No unitname specified.\" }")
		return	
	}
	
	var unitId int
	checkerr := DBptr.QueryRow(`select unitid from affiliation_units where name=$1`,unitName).Scan(&unitId)
	switch {
	case checkerr == sql.ErrNoRows: 
		// set the header for success since we are already at the desired result
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "{ \"error\": \"Affiliation unit does not exist.\" }")
		log.Print("unit " + unitName + " not found in DB.")
		return	
	case checkerr != nil:
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "{ \"error\": \"Database error.\" }")
		log.Print("deleteUser: Error querying DB for unit " + unitName + ".")
		return	
	default:
		
		rows, err := DBptr.Query(`select gid, groups.name from affiliation_unit_group as aug join groups on aug.groupid = groups.groupid where aug.unitid=$1`, unitId)
		if err != nil {
			defer log.Print(err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "{ \"error\": \"Error in DB query.\" }")
			return
		}
		
		defer rows.Close()
		type jsonout struct {
			GId int  `json:"gid"`
			GName string `json:"name"`
		}
		var Entry jsonout
		var Out []jsonout
		
		for rows.Next() {
			var tmpGID int
			var tmpGname string
			rows.Scan(&tmpGID,&tmpGname)
			Entry.GId = tmpGID
			Entry.GName = tmpGname
			Out = append(Out, Entry)
		}
		var output interface{}
		if len(Out) == 0 {
			w.WriteHeader(http.StatusNotFound)
			type jsonerror struct {
				Error string `json:"error"`
			}
			var queryErr []jsonerror
			queryErr = append(queryErr, jsonerror{"This affiliation unit has no groups."})
			output = queryErr
		} else {
			output = Out
		}
		jsonoutput, err := json.Marshal(output)
		if err != nil {
			log.Print(err.Error())
		}
		fmt.Fprintf(w, string(jsonoutput))
	}
	
}

func getGroupLeadersinAffiliationUnit(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	unitName := q.Get("unitname")
	if unitName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No unit name specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No unitname specified.\" }")
		return	
	}
	
	rows, err := DBptr.Query(`select groups.name, user_group.uid, users.uname  from user_group join users on users.uid = user_group.uid join groups on groups.groupid = user_group.groupid where is_leader=TRUE and user_group.groupid in (select groupid from affiliation_unit_group left outer join affiliation_units as au on affiliation_unit_group.unitid= au.unitid where au.name=$1) order by groups.name`,unitName)
	if err != nil {
		defer log.Print(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "{ \"error\": \"Error in DB query.\" }")
		return
	}
	defer rows.Close()	
	type jsonout struct {
		GName string `json:"groupname"`
		UName string `json:"username"`
		UID int  `json:"uid"`
	}
		var Entry jsonout
		var Out []jsonout
		
		for rows.Next() {
			var (
				tmpUID int
				tmpUname,tmpGname string
			)
			rows.Scan(&tmpGname,&tmpUID,&tmpUname)
			Entry.GName = tmpGname
			Entry.UID = tmpUID
			Entry.UName = tmpUname
			Out = append(Out, Entry)
		}
		var output interface{}
		if len(Out) == 0 {
			w.WriteHeader(http.StatusNotFound)
			type jsonerror struct {
				Error string `json:"error"`
			}
			var queryErr []jsonerror
			queryErr = append(queryErr, jsonerror{"This affiliation unit has no groups with assigned leaders."})
			output = queryErr
		} else {
			output = Out
		}
		jsonoutput, err := json.Marshal(output)
		if err != nil {
			log.Print(err.Error())
		}
		fmt.Fprintf(w, string(jsonoutput))
		
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
