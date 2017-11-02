package main
import (
	"strconv"
	"log"
	"encoding/json"
	"fmt"
//"fmt"
//"log"
 _ "github.com/lib/pq"
"net/http"
//"encoding/json"
)

func NotDoneYet(w http.ResponseWriter) {
	fmt.Fprintf(w, `{"error": "This function is not done yet!"}`)
}

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
	q := r.URL.Query()
	gName := q.Get("groupname")
	var iGid bool
	if gName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No groupname specified in http query.")
		fmt.Fprintf(w,"{ \"error\": \"No groupname specified.\" }")
		return
	}
	if q.Get("include_gid") != "" {
		var err error
		iGid, err = strconv.ParseBool(q.Get("include_gid"))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			log.Print("Invalid include_gid specified in http query.")
			fmt.Fprintf(w,"{ \"error\": \"Invalid include_gid specified.\" }")
			return
		}
	}

	pingerr := DBptr.Ping()
	if pingerr != nil {
		log.Fatal(pingerr)
	}
	
	rows, err := DBptr.Query(`select groupid, gid from groups where name=$1`, gName)
	if err != nil {
		log.Fatal(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"Error in DB query\n")	
	} else {	
		defer rows.Close()

		type jsonout struct {
			Groupid int `json:"groupid"`;
			Gid int `json:"gid,omitempty"`;
		}
		var Out jsonout
		
		idx := 0
		for rows.Next() {
			if idx == 0 {
				fmt.Fprintf(w,"[ ")
			} else {
				fmt.Fprintf(w,",")
			}
			rows.Scan(&Out.Groupid, &Out.Gid)
			if !iGid {
				Out.Gid = 0
			}
			outline, jsonerr := json.Marshal(Out)
			if jsonerr != nil {
				log.Fatal(jsonerr)
			}
			fmt.Fprintf(w,string(outline))
			idx++
		}
		if idx == 0 {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w, `{ "error": "Group does not exist." }`)
		} else {
			fmt.Fprintf(w," ]")
		}		
	}
}

func getGroupName(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	gid := q.Get("gid")
	if gid == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No gid specified in http query.")
		fmt.Fprintf(w,"{ \"error\": \"No gid specified.\" }")
		return
	} else if _, err := strconv.Atoi(gid); err != nil  {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("Invalid gid specified in http query.")
		fmt.Fprintf(w,"{ \"error\": \"Invalid gid specified.\" }")
		return
	}

	pingerr := DBptr.Ping()
	if pingerr != nil {
		log.Fatal(pingerr)
	}
	
	rows, err := DBptr.Query(`select name from groups where gid=$1`, gid)
	if err != nil {
		log.Fatal(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"Error in DB query\n")	
	} else {	
		defer rows.Close()

		type jsonout struct {
			Groupname string `json:"groupname"`
		}
		var Out jsonout
		
		idx := 0
		for rows.Next() {
			if idx == 0 {
				fmt.Fprintf(w,"[ ")
			} else {
				fmt.Fprintf(w,",")
			}
			rows.Scan(&Out.Groupname)
			outline, jsonerr := json.Marshal(Out)
			if jsonerr != nil {
				log.Fatal(jsonerr)
				}
			fmt.Fprintf(w,string(outline))
			idx++
			}
		if idx == 0 {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w, `{ "error": "Group does not exist." }`)
		} else {
			fmt.Fprintf(w," ]")
		}		
	}
}

func lookupCertificateDN(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query() 
//	certDN := q.Get("certificatedn")
	NotDoneYet(w)
}
