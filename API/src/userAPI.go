package main
import (
"fmt"
"log"
 _ "github.com/lib/pq"
"net/http"
"encoding/json"
)

func getDN(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	uname := q.Get("uname")
	expt := q.Get("experiment")
	if uname == "" {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"error\": \"No such user.\" }")
		//		http.Error(w, "No uname specified", 404)
		return
	}
	if expt == "" {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"error\": \"No such experiment.\" }")	
		return
	}
	
	rows, err := DBptr.Query(`select user_certificate.dn, user_certificate.issuer_ca from user_certificate INNER JOIN users on (user_certificate.uid = users.uid) INNER JOIN collaboration_unit on (user_certificate.unitid = collaboration_unit.unitid) where users.uname=$1 and collaboration_unit.unit_name=$2`,uname,expt)
	if err != nil {
		defer log.Fatal(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"error\": \"Error in DB duery.\" }")
//		http.Error(w,"Error in DB query",404)
	} else {
		
		defer rows.Close()
	
		idx := 0

		type jsonout struct {
			DN string `json:"dn"`
			Issuer string `json:"issuer_ca"`
		}

		var Out jsonout
		
		for rows.Next() {
			if idx == 0 {
				fmt.Fprintf(w,"{ [ ")
			} else {
				fmt.Fprintf(w,",")
			}
			rows.Scan(&Out.DN,&Out.Issuer)
//			fmt.Println(Out.Gid,Out.Groupname)
			outline, jsonerr := json.Marshal(Out)
			if jsonerr != nil {
				log.Fatal(jsonerr)
				}
			fmt.Fprintf(w,string(outline))
			idx += 1
			}
		if idx == 0 {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w,`{ "error": "No DN found." }`)
		} else {
			fmt.Fprintf(w,"] }")
		}		
	}
	
}
func getGroups(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	uname := q.Get("uname")
	if uname == "" {
		w.WriteHeader(http.StatusNotFound)
		log.Print("No username specified in http query.")
		fmt.Fprintf(w,"{ \"error\": \"No username specified.\" }")
		return
	}
	pingerr := DBptr.Ping()
	if pingerr != nil {
		log.Fatal(pingerr)
	}
	rows, err := DBptr.Query(`select groups.gid, groups.group_name from groups INNER JOIN user_group on (groups.groupid = user_group.groupid) INNER JOIN users on (user_group.uid = users.uid) where users.uname=$1`,uname)
	if err != nil {
		log.Fatal(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"Error in DB query\n")

		
	} else {
		
		defer rows.Close()
	
		idx := 0

		type jsonout struct {
			Gid int `json:"gid"`
			Groupname string `json:"groupname"`
		}

		var Out jsonout
		
		for rows.Next() {
			if idx == 0 {
				fmt.Fprintf(w,"{ [ ")
			} else {
				fmt.Fprintf(w,",")
			}
			rows.Scan(&Out.Gid,&Out.Groupname)
			outline, jsonerr := json.Marshal(Out)
			if jsonerr != nil {
				log.Fatal(jsonerr)
				}
			fmt.Fprintf(w,string(outline))
			idx += 1
			}
		if idx == 0 {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w, `{ "error": "User not found." }`)		
		} else {
			fmt.Fprintf(w,"] }")
		}
		
	}
}
