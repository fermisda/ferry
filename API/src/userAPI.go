package main
import (
	"strconv"
	"strings"
"time"
"fmt"
"log"
 _ "github.com/lib/pq"
"net/http"
"encoding/json"
)

func getUserCertificateDNs(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	uname := q.Get("username")
	expt := q.Get("experimentname")
	if uname == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No username specified in http query.")
		fmt.Fprintf(w,"{ \"error\": \"No username specified.\" }")
		return
	}
	if expt == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No experiment name specified in http query.")
		fmt.Fprintf(w,"{ \"error\": \"No experimentname specified.\" }")
		return
	}
	
	rows, err := DBptr.Query(`select affiliation_units.name, user_certificate.dn, user_certificate.issuer_ca from user_certificate INNER JOIN users on (user_certificate.uid = users.uid) INNER JOIN affiliation_units on (user_certificate.unitid = affiliation_units.unitid) where users.uname=$1 and affiliation_units.name=$2`,uname,expt)
	if err != nil {
		defer log.Fatal(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"error\": \"Error in DB query.\" }")
//		http.Error(w,"Error in DB query",404)
		return
	}

	defer rows.Close()

	idx := 0

	type jsonout struct {
		UnitName string `json:"unit_name"`
		DN string `json:"dn"`
		Issuer string `json:"issuer_ca"`
	}

	var Out jsonout
	
	output := "[ "
	
	for rows.Next() {
		if idx != 0 {
			output += ","
		}
		rows.Scan(&Out.UnitName,&Out.DN,&Out.Issuer)
//			fmt.Println(Out.Gid,Out.Groupname)
		outline, jsonerr := json.Marshal(Out)
		if jsonerr != nil {
			log.Fatal(jsonerr)
			}
		output += string(outline)
		idx += 1
	}
	if idx == 0 {
		rows, err := DBptr.Query(`select 'user' from users where uname=$1 union select 'experiment' from affiliation_units where name=$2`,uname,expt)
		if err != nil {
			defer log.Fatal(err)
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w,"{ \"error\": \"Error in DB query.\" }")
	//		http.Error(w,"Error in DB query",404)
			return
		}
		userExists := false
		exptExists := false
		for rows.Next() {
			var item string
			rows.Scan(&item)
			if item == `user` {
				userExists = true
			}
			if item == `experiment` {
				exptExists = true
			}
		}
		w.WriteHeader(http.StatusNotFound)

		if !userExists {
			output += `"error": "User does not exist.",`
		}
		if !exptExists {
			output += `"error": "Experiment does not exist.",`
		}
		output += `"error": "User does not have any certifcates registered."`
	}

	output += " ]"
	fmt.Fprintf(w,output)
}

func getUserFQANs(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	uname := q.Get("username")
	expt := q.Get("experimentname")
	if uname == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No username specified in http query.")
		fmt.Fprintf(w,"{ \"error\": \"No username specified.\" }")
		return
	}
	if expt == "" {
		expt = "%"
	}
	
	rows, err := DBptr.Query(`select T2.name, T1.fqan from
		                     (select fq.fqan, gf.groupid from grid_fqan as fq left join groups as gf on fq.mapped_group=gf.name where mapped_user=$1) as T1 left join
		                     (select au.name, ag.groupid from affiliation_units as au left join affiliation_unit_group as ag on au.unitid=ag.unitid) as T2
		                      on T1.groupid=T2.groupid where T2.name like $2 order by T2.name`,uname,expt)
	if err != nil {
		defer log.Fatal(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"error\": \"Error in DB query.\" }")
//		http.Error(w,"Error in DB query",404)
		return
	}
	defer rows.Close()

	type jsonout struct {
		UnitName string `json:"unit_name"`
		Fqan string `json:"fqan"`
	}
	var Out jsonout
	
	idx := 0
	output := "[ "
	for rows.Next() {
		if idx != 0 {
			output += ","
		}
		rows.Scan(&Out.UnitName,&Out.Fqan)
//			fmt.Println(Out.Gid,Out.Groupname)
		outline, jsonerr := json.Marshal(Out)
		if jsonerr != nil {
			log.Fatal(jsonerr)
			}
		output += string(outline)
		idx += 1
	}
	if idx == 0 {
		rows, err := DBptr.Query(`select 'user' from users where uname=$1 union select 'experiment' from affiliation_units where name=$2`,uname,expt)
		if err != nil {
			defer log.Fatal(err)
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w,"{ \"error\": \"Error in DB query.\" }")
	//		http.Error(w,"Error in DB query",404)
			return
		}
		userExists := false
		exptExists := false
		for rows.Next() {
			var item string
			rows.Scan(&item)
			if item == `user` {
				userExists = true
			}
			if item == `experiment` {
				exptExists = true
			}
		}
		w.WriteHeader(http.StatusNotFound)

		if !userExists {
			output += `"error": "User does not exist.",`
		}
		if !exptExists {
			output += `"error": "Experiment does not exist.",`
		}
		output += `"error": "User do not have any assigned FQANs."`
	}

	output += " ]"
	fmt.Fprintf(w,output)
}

func getSuperUserList(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	expt := q.Get("experimentname")
	if expt == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No experimentname specified in http query.")
		fmt.Fprintf(w,"{ \"error\": \"No username specified.\" }")
		return
	}
	
	rows, err := DBptr.Query(`select distinct us.uname from users as us
							  right join grid_access as ga on us.uid=ga.uid
							  left join affiliation_units as au on ga.unitid = au.unitid
							  where ga.is_superuser=true and au.name=$1`,expt)
	if err != nil {
		defer log.Fatal(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"error\": \"Error in DB query.\" }")
//		http.Error(w,"Error in DB query",404)
		return
	}
	defer rows.Close()

	type jsonout struct {
		UnitName string `json:"unit_name"`
	}
	var Out jsonout
	
	idx := 0
	output := "[ "
	for rows.Next() {
		if idx != 0 {
			output += ","
		}
		rows.Scan(&Out.UnitName)
//			fmt.Println(Out.Gid,Out.Groupname)
		outline, jsonerr := json.Marshal(Out)
		if jsonerr != nil {
			log.Fatal(jsonerr)
			}
		output += string(outline)
		idx += 1
	}
	if idx == 0 {
		rows, err := DBptr.Query(`select 'experiment' from affiliation_units where name=$1`,expt)
		if err != nil {
			defer log.Fatal(err)
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w,"{ \"error\": \"Error in DB query.\" }")
	//		http.Error(w,"Error in DB query",404)
			return
		}
		exptExists := false
		for rows.Next() {
			var item string
			rows.Scan(&item)
			if item == `experiment` {
				exptExists = true
			}
		}
		w.WriteHeader(http.StatusNotFound)

		if !exptExists {
			output += `"error": "Experiment does not exist.",`
		}
		output += `"error": "No super users found,"`
	}

	output += " ]"
	fmt.Fprintf(w,output)
}

func getUserGroups(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	uname := q.Get("username")
	if uname == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No username specified in http query.")
		fmt.Fprintf(w,"{ \"error\": \"No username specified.\" }")
		return
	}
	pingerr := DBptr.Ping()
	if pingerr != nil {
		log.Fatal(pingerr)
	}
	rows, err := DBptr.Query(`select groups.gid, groups.name from groups INNER JOIN user_group on (groups.groupid = user_group.groupid) INNER JOIN users on (user_group.uid = users.uid) where users.uname=$1`,uname)
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
				fmt.Fprintf(w,"[ ")
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
			fmt.Fprintf(w, `{ "error": "User does not exist." }`)		
		} else {
			fmt.Fprintf(w," ]")
		}		
	}
}

func getUserInfo(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	uname := q.Get("username")
	if uname == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No username specified in http query.")
		fmt.Fprintf(w,"{ \"error\": \"No username specified.\" }")
		return
	}
	pingerr := DBptr.Ping()
	if pingerr != nil {
		log.Fatal(pingerr)
	}
	rows, err := DBptr.Query(`select full_name, uid, status, expiration_date from users where uname=$1`,uname)
	if err != nil {
		log.Fatal(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"Error in DB query\n")	
	} else {
		defer rows.Close()
	
		idx := 0

		type jsonout struct {
			FullName string `json:"full_name"`
			Uid int `json:"uid"`
			Status bool `json:"status"`
			ExpDate time.Time `json:"expiration_date"`
		}

		var Out jsonout
		
		for rows.Next() {
			if idx == 0 {
				fmt.Fprintf(w,"[ ")
			} else {
				fmt.Fprintf(w,",")
			}
			rows.Scan(&Out.FullName,&Out.Uid,&Out.Status,&Out.ExpDate)
			outline, jsonerr := json.Marshal(Out)
			if jsonerr != nil {
				log.Fatal(jsonerr)
				}
			fmt.Fprintf(w,string(outline))
			idx += 1
			}
		if idx == 0 {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w, `{ "error": "User does not exist." }`)		
		} else {
			fmt.Fprintf(w," ]")
		}
	}
}

func addUserToGroup(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	uName := q.Get("username")
	gName := q.Get("groupname")
	isLeader := false

	if uName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No username specified in http query.")
		fmt.Fprintf(w,"{ \"error\": \"No username specified.\" }")
		return
	}
	if gName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No groupname specified in http query.")
		fmt.Fprintf(w,"{ \"error\": \"No groupname specified.\" }")
		return
	}
	if q.Get("is_leader") != "" {
		var err error
		isLeader, err = strconv.ParseBool(q.Get("is_leader"))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			log.Print("Invalid is_leader specified in http query.")
			fmt.Fprintf(w,"{ \"error\": \"Invalid is_leader specified.\" }")
			return
		}
	}

	cKey, err := DBtx.Start(DBptr)
	if err != nil {
		log.Fatal(err)
	}

	var uid int
	rows, err := DBtx.Query(`select uid from users where uname=$1`, uName)
	if err != nil {
		log.Fatal(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"Error in DB query\n")	
	} else {
		rows.Next()
		rows.Scan(&uid)
		rows.Close()
	}
	var groupid int
	rows, err = DBtx.Query(`select groupid from groups where name=$1`, gName)
	if err != nil {
		log.Fatal(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"Error in DB query\n")	
	} else {
		rows.Next()
		rows.Scan(&groupid)
		rows.Close()
	}
	_, err = DBtx.Exec("insert into user_group (uid, groupid, is_leader, last_updated) values ($1, $2, $3, NOW())", uid, groupid, isLeader)
	if err == nil {
		fmt.Fprintf(w,"{ \"status\": \"success\" }")
	} else {
		if strings.Contains(err.Error(), `duplicate key value violates unique constraint`) {
			fmt.Fprintf(w,"{ \"error\": \"User already belongs to this group.\" }")
		} else if strings.Contains(err.Error(), `violates foreign key constraint "fk_user_group_users"`) {
			fmt.Fprintf(w,"{ \"error\": \"User does not exist.\" }")
		} else if strings.Contains(err.Error(), `violates foreign key constraint "fk_user_group_groups"`) {
			fmt.Fprintf(w,"{ \"error\": \"Group does not exist.\" }")
		} else {
			log.Print(err.Error())
		}
	}

	DBtx.Commit(cKey)
}