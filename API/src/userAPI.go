package main
import (
	"database/sql"
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

	authorized,authout := authorize(r,AuthorizedDNs)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w,"{ \"error\": \"" + authout + "not authorized.\" }")
		return
	}
	
	rows, err := DBptr.Query(`select t3.name, t1.dn, t1.issuer_ca, c.user_exists, c.unit_exists
							  from (select 1 as key, uid, dn, unitid, issuer_ca from user_certificate) as t1
							  join (select uid from users where uname = $1) as t2 on t1.uid = t2.uid
							  join (select unitid, name from affiliation_units where name = $2) as t3 on t1.unitid = t3.unitid
							  right join (select 1 as key,
							       $1 in (select uname from users) as user_exists, 
							       $2 in (select name from affiliation_units) as unit_exists) as c on c.key = t1.key`,uname,expt)
	if err != nil {
		defer log.Fatal(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"error\": \"Error in DB query.\" }")
//		http.Error(w,"Error in DB query",404)
		return
	}

	defer rows.Close()

	idx := 0
	var userExists, exptExists bool

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
		var tmpUnitName, tmpDN, tmpIssuer sql.NullString
		rows.Scan(&tmpUnitName,&tmpDN,&tmpIssuer,&userExists,&exptExists)
		if tmpDN.Valid {
			Out.UnitName, Out.DN, Out.Issuer = tmpUnitName.String, tmpDN.String, tmpIssuer.String
			outline, jsonerr := json.Marshal(Out)
			if jsonerr != nil {
				log.Fatal(jsonerr)
			}
			output += string(outline)
			idx ++
		}
	}
	if idx == 0 {
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
	
	rows, err := DBptr.Query(`select T2.name, T1.fqan, c.user_exists, c.unit_exists
				  from       (select 1 as key, fq.fqan, gf.groupid from grid_fqan as fq left join groups as gf on fq.mapped_group=gf.name where mapped_user=$1) as T1 
				  join       (select au.name, ag.groupid from affiliation_units as au left join affiliation_unit_group as ag on au.unitid=ag.unitid where name like $2) as T2 on T1.groupid=T2.groupid
				  right join (select 1 as key, $1 in (select uname from users) as user_exists, $2 in (select name from affiliation_units) as unit_exists) as c on c.key = t1.key order by T2.name;`,uname,expt)
	if err != nil {
		defer log.Fatal(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"error\": \"Error in DB query.\" }")
//		http.Error(w,"Error in DB query",404)
		return
	}
	defer rows.Close()

	var userExists, exptExists bool

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
		var tmpUnitName, tmpFqan sql.NullString
		rows.Scan(&tmpUnitName, &tmpFqan, &userExists, &exptExists)
		if tmpFqan.Valid {
			Out.UnitName, Out.Fqan = tmpUnitName.String, tmpFqan.String
			outline, jsonerr := json.Marshal(Out)
			if jsonerr != nil {
				log.Fatal(jsonerr)
				}
			output += string(outline)
			idx ++
		}
	}
	if idx == 0 {
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
	
	rows, err := DBptr.Query(`select t1.uname, c.unit_exists from 
		                     (select distinct 1 as key, us.uname from users as us right join grid_access as ga on us.uid=ga.uid
							  left join affiliation_units as au on ga.unitid = au.unitid where ga.is_superuser=true and au.name=$1) as t1
							  right join (select 1 as key, $1 in (select name from affiliation_units) as unit_exists) as c on c.key = t1.key`,expt)
	if err != nil {
		defer log.Fatal(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"error\": \"Error in DB query.\" }")
//		http.Error(w,"Error in DB query",404)
		return
	}
	defer rows.Close()

	var exptExists bool

	type jsonout struct {
		Uname string `json:"uname"`
	}
	var Out jsonout
	
	idx := 0
	output := "[ "
	for rows.Next() {
		if idx != 0 {
			output += ","
		}

		var tmpUname sql.NullString
		rows.Scan(&tmpUname, &exptExists)
		if tmpUname.Valid {
			Out.Uname = tmpUname.String
			outline, jsonerr := json.Marshal(Out)
			if jsonerr != nil {
				log.Fatal(jsonerr)
			}
			output += string(outline)
			idx ++
		}
	}
	if idx == 0 {
		w.WriteHeader(http.StatusNotFound)

		if !exptExists {
			output += `"error": "Experiment does not exist.",`
		}
		output += `"error": "No super users found."`
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
	isLeader := q.Get("is_leader")

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
	if isLeader == "" {
		isLeader = "false"
	} else {
		_, err := strconv.ParseBool(q.Get("is_leader"))
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

	_, err = DBtx.Exec(fmt.Sprintf(`do $$
										declare uid int;
										declare groupid int;
									begin
										select u.uid into uid from users as u where uname = '%s';
										select g.groupid into groupid from groups as g where name = '%s';
										
										insert into user_group (uid, groupid, is_leader, last_updated)
														values (uid, groupid, %s, NOW());
									end $$;`, uName, gName, isLeader))

	if err == nil {
		fmt.Fprintf(w,"{ \"status\": \"success\" }")
	} else {
		if strings.Contains(err.Error(), `duplicate key value violates unique constraint`) {
			fmt.Fprintf(w,"{ \"error\": \"User already belongs to this group.\" }")
		} else if strings.Contains(err.Error(), `null value in column "uid" violates not-null constraint`) {
			fmt.Fprintf(w,"{ \"error\": \"User does not exist.\" }")
		} else if strings.Contains(err.Error(), `null value in column "groupid" violates not-null constraint`) {
			fmt.Fprintf(w,"{ \"error\": \"Group does not exist.\" }")
		} else {
			log.Print(err.Error())
			fmt.Fprintf(w,"{ \"error\": \"Something went wrong.\" }")
		}
	}

	DBtx.Commit(cKey)
}

func setUserExperimentFQAN(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	uName := q.Get("username")
	fqan  := q.Get("fqan")
	eName := q.Get("experimentname")

	if uName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No username specified in http query.")
		fmt.Fprintf(w,"{ \"error\": \"No username specified.\" }")
		return
	}
	if fqan == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No fqan specified in http query.")
		fmt.Fprintf(w,"{ \"error\": \"No fqan specified.\" }")
		return
	}
	if eName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No experimentname specified in http query.")
		fmt.Fprintf(w,"{ \"error\": \"No experimentname specified.\" }")
		return
	}

	cKey, err := DBtx.Start(DBptr)
	if err != nil {
		log.Fatal(err)
	}

	_, err = DBtx.Exec(fmt.Sprintf(`do $$
										declare unitid int;
										declare uid int;
										declare fqanid int;
									begin
										select a.unitid into unitid from affiliation_units as a where name = '%s';
										select u.uid into uid from users as u where uname = '%s';
										select f.fqanid into fqanid from grid_fqan as f where fqan = '%s';
										
										insert into grid_access (unitid, uid, fqanid, is_superuser, is_banned, last_updated)
														 values (unitid, uid, fqanid, false, false, NOW());
									end $$;`, eName, uName, fqan))
	if err == nil {
		fmt.Fprintf(w,"{ \"status\": \"success\" }")
	} else {
		if strings.Contains(err.Error(), `null value in column "uid" violates not-null constraint`) {
			fmt.Fprintf(w,"{ \"error\": \"User does not exist.\" }")
		} else if strings.Contains(err.Error(), `null value in column "fqanid" violates not-null constraint`) {
			fmt.Fprintf(w,"{ \"error\": \"FQAN does not exist.\" }")
		} else if strings.Contains(err.Error(), `null value in column "unitid" violates not-null constraint`) {
			fmt.Fprintf(w,"{ \"error\": \"Experiment does not exist.\" }")
		} else if strings.Contains(err.Error(), `duplicate key value violates unique constraint "idx_grid_access"`) {
			fmt.Fprintf(w,"{ \"error\": \"This association already exists.\" }")
		} else {
			log.Print(err.Error())
			fmt.Fprintf(w,"{ \"error\": \"Something went wrong.\" }")
		}
	}

	DBtx.Commit(cKey)
}