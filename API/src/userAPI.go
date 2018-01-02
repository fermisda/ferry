package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"net/http"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

func getUserCertificateDNs(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	uname := q.Get("username")
	expt := q.Get("experimentname")
	if uname == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r)).Error("No username specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No username specified.\" }")
		return
	}
	if expt == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No experiment name specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No experimentname specified.\" }")
		return
	}

	authorized, authout := authorize(r, AuthorizedDNs)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"error\": \""+authout+"not authorized.\" }")
		return
	}

	rows, err := DBptr.Query(`select t3.name, t1.dn, t1.issuer_ca, c.user_exists, c.unit_exists
							  from (select 1 as key, uid, dn, unitid, issuer_ca from user_certificate) as t1
							  join (select uid from users where uname = $1) as t2 on t1.uid = t2.uid
							  join (select unitid, name from affiliation_units where name = $2) as t3 on t1.unitid = t3.unitid
							  right join (select 1 as key,
							       $1 in (select uname from users) as user_exists, 
							       $2 in (select name from affiliation_units) as unit_exists) as c on c.key = t1.key`, uname, expt)
	if err != nil {
		defer log.Fatal(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"error\": \"Error in DB query.\" }")
		//		http.Error(w,"Error in DB query",404)
		return
	}

	defer rows.Close()

	idx := 0
	var userExists, exptExists bool

	type jsonout struct {
		UnitName string `json:"unit_name"`
		DN       string `json:"dn"`
		Issuer   string `json:"issuer_ca"`
	}
	var Out jsonout

	output := "[ "

	for rows.Next() {
		if idx != 0 {
			output += ","
		}
		var tmpUnitName, tmpDN, tmpIssuer sql.NullString
		rows.Scan(&tmpUnitName, &tmpDN, &tmpIssuer, &userExists, &exptExists)
		if tmpDN.Valid {
			Out.UnitName, Out.DN, Out.Issuer = tmpUnitName.String, tmpDN.String, tmpIssuer.String
			outline, jsonerr := json.Marshal(Out)
			if jsonerr != nil {
				log.Fatal(jsonerr)
			}
			output += string(outline)
			idx++
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
	fmt.Fprintf(w, output)
}

func getUserFQANs(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	uname := q.Get("username")
	expt := q.Get("experimentname")
	if uname == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No username specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No username specified.\" }")
		return
	}
	if expt == "" {
		expt = "%"
	}

	rows, err := DBptr.Query(`select T2.name, T1.fqan, c.user_exists, c.unit_exists
				  from       (select 1 as key, fq.fqan, gf.groupid from grid_fqan as fq left join groups as gf on fq.mapped_group=gf.name where mapped_user=$1) as T1 
				  join       (select au.name, ag.groupid from affiliation_units as au left join affiliation_unit_group as ag on au.unitid=ag.unitid where name like $2) as T2 on T1.groupid=T2.groupid
				  right join (select 1 as key, $1 in (select uname from users) as user_exists, $2 in (select name from affiliation_units) as unit_exists) as c on c.key = t1.key order by T2.name;`, uname, expt)
	if err != nil {
		defer log.Fatal(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"error\": \"Error in DB query.\" }")
		//		http.Error(w,"Error in DB query",404)
		return
	}
	defer rows.Close()

	var userExists, exptExists bool

	type jsonout struct {
		UnitName string `json:"unit_name"`
		Fqan     string `json:"fqan"`
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
			idx++
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
	fmt.Fprintf(w, output)
}

func getSuperUserList(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	expt := q.Get("experimentname")
	if expt == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No experimentname specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No username specified.\" }")
		return
	}

	rows, err := DBptr.Query(`select t1.uname, c.unit_exists from 
		                     (select distinct 1 as key, us.uname from users as us right join grid_access as ga on us.uid=ga.uid
							  left join affiliation_units as au on ga.unitid = au.unitid where ga.is_superuser=true and au.name=$1) as t1
							  right join (select 1 as key, $1 in (select name from affiliation_units) as unit_exists) as c on c.key = t1.key`, expt)
	if err != nil {
		defer log.Fatal(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"error\": \"Error in DB query.\" }")
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
			idx++
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
	fmt.Fprintf(w, output)
}

func setSuperUser(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

	//call authorize function
	authorized, authout := authorize(r, AuthorizedDNs)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"error\": \""+authout+"not authorized.\" }")
		return
	}

	q := r.URL.Query()
	uName := q.Get("username")
	unitName := q.Get("unitname")
	if uName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No user name specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No username specified.\" }")
		return
	}
	if unitName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No unit name specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No unitname specified.\" }")
		return
	}

	cKey, err := DBtx.Start(DBptr)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprintf(w, "{ \"error\": \"Unable to start database transaction.\" }")
		return
	}
	_, err = DBtx.Exec(fmt.Sprintf(`do $$
										declare Userid int;
										declare Unitid int;
									begin
										select u.uid into Userid from users as u where uname = '%s';
										select au.unitid into Unitid from affiliation_units as au where name = '%s';
										if Userid is null then raise 'User does not exist'; end if;
										if Unitid is null then raise 'Unit does not exist'; end if;
										update grid_access set is_superuser=true, last_updated =  NOW())
                                                                                where uid=Userid and unitid=Unitid;
									end $$;`, uName, unitName))
	if err == nil {
		fmt.Fprintf(w, "{ \"status\": \"success\" }")
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		if strings.Contains(err.Error(), `User does not exist`) {
			fmt.Fprintf(w, "{ \"error\": \"Uner does not exist.\" }")
		} else if strings.Contains(err.Error(), `Unit does not exist`) {
			fmt.Fprintf(w, "{ \"error\": \"Unit does not exist.\" }")
		} else {
			log.Print(err.Error())
			fmt.Fprintf(w, "{ \"error\": \"Something went wrong.\" }")
		}
	}

	DBtx.Commit(cKey)
}

func getUserGroups(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	uname := q.Get("username")
	if uname == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No username specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No username specified.\" }")
		return
	}
	pingerr := DBptr.Ping()
	if pingerr != nil {
		log.Fatal(pingerr)
	}
	rows, err := DBptr.Query(`select groups.gid, groups.name from groups INNER JOIN user_group on (groups.groupid = user_group.groupid) INNER JOIN users on (user_group.uid = users.uid) where users.uname=$1`, uname)
	if err != nil {
		log.Fatal(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "Error in DB query\n")
	} else {
		defer rows.Close()

		idx := 0

		type jsonout struct {
			Gid       int    `json:"gid"`
			Groupname string `json:"groupname"`
		}

		var Out jsonout

		for rows.Next() {
			if idx == 0 {
				fmt.Fprintf(w, "[ ")
			} else {
				fmt.Fprintf(w, ",")
			}
			rows.Scan(&Out.Gid, &Out.Groupname)
			outline, jsonerr := json.Marshal(Out)
			if jsonerr != nil {
				log.Fatal(jsonerr)
			}
			fmt.Fprintf(w, string(outline))
			idx += 1
		}
		if idx == 0 {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w, `{ "error": "User does not exist." }`)
		} else {
			fmt.Fprintf(w, " ]")
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
		fmt.Fprintf(w, "{ \"error\": \"No username specified.\" }")
		return
	}
	pingerr := DBptr.Ping()
	if pingerr != nil {
		log.Fatal(pingerr)
	}
	rows, err := DBptr.Query(`select full_name, uid, status, expiration_date from users where uname=$1`, uname)
	if err != nil {
		log.Fatal(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "Error in DB query\n")
	} else {
		defer rows.Close()

		idx := 0

		type jsonout struct {
			FullName string    `json:"full_name"`
			Uid      int       `json:"uid"`
			Status   bool      `json:"status"`
			ExpDate  time.Time `json:"expiration_date"`
		}

		var Out jsonout

		for rows.Next() {
			if idx == 0 {
				fmt.Fprintf(w, "[ ")
			} else {
				fmt.Fprintf(w, ",")
			}
			rows.Scan(&Out.FullName, &Out.Uid, &Out.Status, &Out.ExpDate)
			outline, jsonerr := json.Marshal(Out)
			if jsonerr != nil {
				log.Fatal(jsonerr)
			}
			fmt.Fprintf(w, string(outline))
			idx += 1
		}
		if idx == 0 {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w, `{ "error": "User does not exist." }`)
		} else {
			fmt.Fprintf(w, " ]")
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
		fmt.Fprintf(w, "{ \"error\": \"No username specified.\" }")
		return
	}
	if gName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No groupname specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No groupname specified.\" }")
		return
	}
	if isLeader == "" {
		isLeader = "false"
	} else {
		_, err := strconv.ParseBool(q.Get("is_leader"))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			log.Print("Invalid is_leader specified in http query.")
			fmt.Fprintf(w, "{ \"error\": \"Invalid is_leader specified.\" }")
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
		fmt.Fprintf(w, "{ \"status\": \"success\" }")
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		if strings.Contains(err.Error(), `duplicate key value violates unique constraint`) {
			fmt.Fprintf(w, "{ \"error\": \"User already belongs to this group.\" }")
		} else if strings.Contains(err.Error(), `null value in column "uid" violates not-null constraint`) {
			fmt.Fprintf(w, "{ \"error\": \"User does not exist.\" }")
		} else if strings.Contains(err.Error(), `null value in column "groupid" violates not-null constraint`) {
			fmt.Fprintf(w, "{ \"error\": \"Group does not exist.\" }")
		} else {
			log.Print(err.Error())
			fmt.Fprintf(w, "{ \"error\": \"Something went wrong.\" }")
		}
	}

	DBtx.Commit(cKey)
}

func setUserExperimentFQAN(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	uName := q.Get("username")
	fqan := q.Get("fqan")
	eName := q.Get("experimentname")

	if uName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No username specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No username specified.\" }")
		return
	}
	if fqan == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No fqan specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No fqan specified.\" }")
		return
	}
	if eName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No experimentname specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No experimentname specified.\" }")
		return
	}

	cKey, err := DBtx.Start(DBptr)
	if err != nil {
		log.Fatal(err)
	}

	_, err = DBtx.Exec(fmt.Sprintf(`do $$
										declare vUnitid int;
										declare vUid int;
										declare vFqanid int;
									begin
										select unitid into vUnitid from affiliation_units where name = '%s';
										select uid into vUid from users where uname = '%s';
										select fqanid into vFqanid from grid_fqan where fqan = '%s';
										
										insert into grid_access (unitid, uid, fqanid, is_superuser, is_banned, last_updated)
														 values (vUnitid, vUid, vFqanid, false, false, NOW());
									end $$;`, eName, uName, fqan))
	if err == nil {
		fmt.Fprintf(w, "{ \"status\": \"success\" }")
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		if strings.Contains(err.Error(), `null value in column "uid" violates not-null constraint`) {
			fmt.Fprintf(w, "{ \"error\": \"User does not exist.\" }")
		} else if strings.Contains(err.Error(), `null value in column "fqanid" violates not-null constraint`) {
			fmt.Fprintf(w, "{ \"error\": \"FQAN does not exist.\" }")
		} else if strings.Contains(err.Error(), `null value in column "unitid" violates not-null constraint`) {
			fmt.Fprintf(w, "{ \"error\": \"Experiment does not exist.\" }")
		} else if strings.Contains(err.Error(), `duplicate key value violates unique constraint "idx_grid_access"`) {
			fmt.Fprintf(w, "{ \"error\": \"This association already exists.\" }")
		} else {
			log.Print(err.Error())
			fmt.Fprintf(w, "{ \"error\": \"Something went wrong.\" }")
		}
	}

	DBtx.Commit(cKey)
}

func setUserShellAndHomeDir(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	rName := q.Get("resourcename")
	uName := q.Get("username")
	shell := q.Get("shell")
	hDir := q.Get("homedir")

	if rName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No resourcename specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No resourcename specified.\" }")
		return
	}
	if uName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No username specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No username specified.\" }")
		return
	}
	if shell == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No shell specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No shell specified.\" }")
		return
	}
	if hDir == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No homedir specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No homedir specified.\" }")
		return
	}

	cKey, err := DBtx.Start(DBptr)
	if err != nil {
		log.Fatal(err)
	}

	_, err = DBtx.Exec(fmt.Sprintf(`do $$
										declare vCompid int;
										declare vUid int;
									begin
										select compid into vCompid from compute_resources where name = '%s';
										select uid into vUid from users where uname = '%s';

										if vCompid is null then raise 'Resource does not exist.'; end if;
										if vUid is null then raise 'User does not exist.'; end if;
										
										update compute_access set shell = '%s', home_dir = '%s', last_updated = NOW()
										where compid = vCompid and uid = vUid;
									end $$;`, rName, uName, shell, hDir))
	if err == nil {
		fmt.Fprintf(w, "{ \"status\": \"success\" }")
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		if strings.Contains(err.Error(), `User does not exist.`) {
			fmt.Fprintf(w, "{ \"error\": \"User does not exist.\" }")
		} else if strings.Contains(err.Error(), `Resource does not exist.`) {
			fmt.Fprintf(w, "{ \"error\": \"Resource does not exist.\" }")
		} else {
			log.Print(err.Error())
			fmt.Fprintf(w, "{ \"error\": \"Something went wrong.\" }")
		}
	}

	DBtx.Commit(cKey)
}

func getUserShellAndHomeDir(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	comp := q.Get("resourcename")
	user := q.Get("username")
	if comp == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No resourcename specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No resourcename specified.\" }")
		return
	}
	if user == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No username specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No username specified.\" }")
		return
	}

	rows, err := DBptr.Query(`select t1.shell, t1.home_dir, c.resource_exists, c.user_exists from
							 (select 1 as key, ca.shell, ca.home_dir from compute_access as ca
							  left join compute_resources as cr on ca.compid = cr.compid
							  left join users as us on ca.uid = us.uid 
							  where cr.name=$1 and us.uname=$2) as t1
							  right join (select 1 as key, $1 in (select name from compute_resources) as resource_exists,
														   $2 in (select uname from users) as user_exists)
							  as c on c.key = t1.key`, comp, user)
	if err != nil {
		defer log.Fatal(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"error\": \"Error in DB query.\" }")
		//		http.Error(w,"Error in DB query",404)
		return
	}
	defer rows.Close()

	var compExists bool
	var userExists bool

	type jsonout struct {
		Shell   string `json:"shell"`
		HomeDir string `json:"homedir"`
	}
	var Out jsonout

	idx := 0
	output := "[ "
	for rows.Next() {
		if idx != 0 {
			output += ","
		}

		var tmpShell, tmpHomeDir sql.NullString
		rows.Scan(&tmpShell, &tmpHomeDir, &compExists, &userExists)
		if tmpShell.Valid {
			Out.Shell = tmpShell.String
			Out.HomeDir = tmpHomeDir.String
			outline, jsonerr := json.Marshal(Out)
			if jsonerr != nil {
				log.Fatal(jsonerr)
			}
			output += string(outline)
			idx++
		}
	}
	if idx == 0 {
		w.WriteHeader(http.StatusNotFound)

		if !compExists {
			output += `"error": "Resource does not exist.",`
		}
		if !userExists {
			output += `"error": "User does not exist.",`
		}
		output += `"error": "No super users found."`
	}

	output += " ]"
	fmt.Fprintf(w, output)
}
func getUserStorageQuota(w http.ResponseWriter, r *http.Request) {

	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	rName := q.Get("resourcename")
	uName := q.Get("username")
	unitName := q.Get("unitname")

	if rName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No resource name specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No resourcename specified.\" }")
		return

	}
	if uName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No user name specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No username specified.\" }")
		return
	}
	if unitName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No unit name specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No unitname specified.\" }")
		return
	}

	rows, err := DBptr.Query(`select sq.path,sq.value, sq.unit, sq.valid_until from storage_quota sq INNER JOIN affiliation_units on affiliation_units.unitid = sq.unitid INNER JOIN storage_resources on storage_resources.storageid = sq.storageid INNER JOIN users on users.uid = sq.uid where affiliation_units.name=$1 AND storage_resources.type=$2 and users.uname=$3`, unitName, rName, uName)
	if err != nil {
		defer log.Fatal(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"error\": \"Error in DB query.\" }")

		return
	}

	defer rows.Close()
	idx := 0
	output := ""
	type jsonout struct {
		Path       string `json:"path"`
		Value      string `json:"value"`
		Unit       string `json:"unit"`
		ValidUntil string `json:"valid_until"`
	}
	var Out jsonout
	for rows.Next() {
		if idx != 0 {
			output += ","
		}
		var tmpPath, tmpValue, tmpUnit, tmpValid sql.NullString
		rows.Scan(&tmpPath, &tmpValue, &tmpUnit, &tmpValid)
		if tmpValue.Valid {
			Out.Path, Out.Value, Out.Unit, Out.ValidUntil = tmpPath.String, tmpValue.String, tmpUnit.String, tmpValid.String
			outline, jsonerr := json.Marshal(Out)
			if jsonerr != nil {
				log.Fatal(jsonerr)
			}
			output += string(outline)
			idx++
		}
	}
	if idx == 0 {
		w.WriteHeader(http.StatusNotFound)
		output += `{"error": "User has no quotas registered."}`
	}
	fmt.Fprintf(w, output)

}

func setUserStorageQuota(w http.ResponseWriter, r *http.Request) {

	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

	//call authorize function
	authorized, authout := authorize(r, AuthorizedDNs)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"error\": \""+authout+"not authorized.\" }")
		return
	}

	q := r.URL.Query()
	quota := q.Get("quota")
	uName := q.Get("username")
	unitName := q.Get("unitname")
	unit := q.Get("unit")
	rName := strings.ToUpper(q.Get("resourcename"))
	isgrp := strings.ToLower(q.Get("isGroup"))
	validtime := q.Get("valid_until")

	var isGroup bool
	if isgrp == "" || isgrp == "false" {
		isGroup = false
	} else {
		isGroup = true
	}
	if quota == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No quota value specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No quota specified.\" }")
		return
	}
	if validtime != "" {
		validtime = "valid_until = " + validtime + ","
	}
	if uName == "" && isGroup == false {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No user name given and isGroup was set to false.")
		fmt.Fprintf(w, "{ \"error\": \"No username provided.\" }")
		return
	}
	if rName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No resource type given.")
		fmt.Fprintf(w, "{ \"error\": \"No resourcename provided.\" }")
		return
	}
	if unitName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No affiliation unit given.")
		fmt.Fprintf(w, "{ \"error\": \"No unitname provided.\" }")
		return
	}
	//set a default unit of "B" for bytes
	if unit == "" {
		unit = "B"
	}

	cKey, err := DBtx.Start(DBptr)
	if err != nil {
		log.Fatal(err)
	}

	_, err = DBtx.Exec(fmt.Sprintf(`do $$
							declare vSid int;
							declare vUid int;
                                                        declare vUnitid int; 
							begin
								select storageid into vSid from storage_resources where name = '%s';
								select uid into vUid from users where uname = '%s';
								select unitid into vUnitid from affiliation_units where name = '%s';

								if vSid is null then raise 'Resource does not exist.'; end if;
								if vUid is null then raise 'User does not exist.'; end if;
								if vUnitid is null then raise 'Unit does not exist.'; end if;
										
								update storage_quota set value = '%s', unit = '%s', %s last_updated = NOW()
								where storageid = vSid and uid = vUid and unitid = vUnitid;
							end $$;`, rName, uName, unitName, quota, unit, validtime))
	if err == nil {
		fmt.Fprintf(w, "{ \"status\": \"success\" }")
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		if strings.Contains(err.Error(), `User does not exist.`) {
			fmt.Fprintf(w, "{ \"error\": \"User does not exist.\" }")
		} else if strings.Contains(err.Error(), `Resource does not exist.`) {
			fmt.Fprintf(w, "{ \"error\": \"Resource does not exist.\" }")
		} else {
			log.Print(err.Error())
			fmt.Fprintf(w, "{ \"error\": \"Something went wrong.\" }")
		}
	}

	DBtx.Commit(cKey)
}
func setUserExternalAffiliationAttribute(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	uName := q.Get("username")
	attribute := q.Get("attribute")
	value := q.Get("value")

	if uName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No username specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No username specified.\" }")
		return
	}
	if attribute == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No attribute specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No attribute specified.\" }")
		return
	}
	if value == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No value specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No value specified.\" }")
		return
	}

	cKey, err := DBtx.Start(DBptr)
	if err != nil {
		log.Fatal(err)
	}

	_, err = DBtx.Exec(fmt.Sprintf(`do $$
									declare v_uid int;
									
									declare c_uname text = '%s';
									declare c_attribute text = '%s';
									declare c_value text = '%s';

									begin
										select uid into v_uid from users where uname = c_uname;
										if v_uid is null then
											raise 'uname does not exist';
										end if;

										if (v_uid, c_attribute) not in (select uid, attribute from external_affiliation_attribute) then
											insert into external_affiliation_attribute (uid, attribute, value)
											values (v_uid, c_attribute, c_value);
										else
											update external_affiliation_attribute set
												value = c_value,
												last_updated = NOW()
											where uid = v_uid and attribute = c_attribute;
										end if;
									end $$;`, uName, attribute, value))

	if err == nil {
		fmt.Fprintf(w, "{ \"status\": \"success\" }")
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		if strings.Contains(err.Error(), `uname does not exist`) {
			fmt.Fprintf(w, "{ \"error\": \"User does not exist.\" }")
		} else {
			log.Print(err.Error())
			fmt.Fprintf(w, "{ \"error\": \"Something went wrong.\" }")
		}
	}

	DBtx.Commit(cKey)
}
func removeUserExternalAffiliationAttribute(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	uName := q.Get("username")
	attribute := q.Get("attribute")

	if uName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No username specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No username specified.\" }")
		return
	}
	if attribute == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No attribute specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No attribute specified.\" }")
		return
	}

	cKey, err := DBtx.Start(DBptr)
	if err != nil {
		log.Fatal(err)
	}

	_, err = DBtx.Exec(fmt.Sprintf(`do $$
									declare v_uid int;
									
									declare c_uname text = '%s';
									declare c_attribute text = '%s';

									begin
										select uid into v_uid from users where uname = c_uname;
										if v_uid is null then
											raise 'uname does not exist';
										end if;

										if (v_uid, c_attribute) not in (select uid, attribute from external_affiliation_attribute) then
											raise 'attribute does not exist';
										end if;

										delete from external_affiliation_attribute where uid = v_uid and attribute = c_attribute;
									end $$;`, uName, attribute))

	if err == nil {
		fmt.Fprintf(w, "{ \"status\": \"success\" }")
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		if strings.Contains(err.Error(), `uname does not exist`) {
			fmt.Fprintf(w, "{ \"error\": \"User does not exist.\" }")
		} else if strings.Contains(err.Error(), `attribute does not exist`) {
			fmt.Fprintf(w, "{ \"error\": \"External affiliation attribute does not exist.\" }")
		} else {
			log.Print(err.Error())
			fmt.Fprintf(w, "{ \"error\": \"Something went wrong.\" }")
		}
	}

	DBtx.Commit(cKey)
}
func getUserExternalAffiliationAttributes(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	user := q.Get("username")

	if user == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No username specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No username specified.\" }")
		return
	}

	rows, err := DBptr.Query(`select attribute, value, user_exists from
							 (select 1 as key, a.attribute, a.value, u.uname from external_affiliation_attribute as a 
							  left join users as u on a.uid = u.uid where uname = $1) as t right join
							 (select 1 as key, $1 in (select uname from users) as user_exists) as c on t.key = c.key;`, user)

	if err != nil {
		defer log.Fatal(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"error\": \"Error in DB query.\" }")
		return
	}
	defer rows.Close()

	var userExists bool

	type jsonentry struct {
		Attribute string `json:"attribute"`
		Value     string `json:"value"`
	}
	var Entry jsonentry
	var Out []jsonentry

	for rows.Next() {
		var tmpAttribute, tmpValue sql.NullString
		rows.Scan(&tmpAttribute, &tmpValue, &userExists)

		if tmpAttribute.Valid {
			Entry.Attribute = tmpAttribute.String
			Entry.Value = tmpValue.String
			Out = append(Out, Entry)
		}
	}

	var output interface{}
	if len(Out) == 0 {
		w.WriteHeader(http.StatusNotFound)
		type jsonerror struct {
			Error string `json:"error"`
		}
		var Err []jsonerror
		if !userExists {
			Err = append(Err, jsonerror{"User does not exist."})
		} else {
			Err = append(Err, jsonerror{"User does not have external affiliation attributes"})
		}
		output = Err
	} else {
		output = Out
	}
	jsonout, err := json.Marshal(output)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Fprintf(w, string(jsonout))

}

func addCertDNtoUser(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

	authorized, authout := authorize(r, AuthorizedDNs)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"error\": \""+authout+"not authorized.\" }")
		return
	}

	q := r.URL.Query()
	uName := q.Get("username")
	unitName := q.Get("unitname")
	subjDN := q.Get("dn")
	issuer := q.Get("issuer_ca")
	if uName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No username specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No username specified.\" }")
		return
	}
	if unitName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No unit name specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No unitname specified.\" }")
		return
	}
	if subjDN == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No DN specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No dn specified.\" }")
		return
	}
	if issuer == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No issuer specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No issuer specified.\" }")
		return
	}

	cKey, err := DBtx.Start(DBptr)
	if err != nil {
		log.Fatal(err)
	}

	_, err = DBtx.Exec(fmt.Sprintf(`do $$ 	
declare  u_uid int;
declare au_unitid int;
u_dn constant text := '%s';
u_issuer constant text := '%s';
u_uname constant text := '%s';
begin
select uid into u_uid from users where uname=u_uname;
select unitid into au_unitid from affiliation_units where name = '%s';
if (u_dn, u_issuer) not in (select dn, issuer_ca from user_certificate as uc join users as u on uc.uid = u.uid join affiliation_units as au on uc.unitid = au.unitid where u.uname = u_uname and au.unitid=au_unitid and uc.dn=u_dn and issuer_ca=u_issuer) then insert into user_certificate (uid, dn, issuer_ca, last_updated, unitid) values (u_uid, u_dn, u_issuer, NOW(), au_unitid);
else  raise 'DN and issuer already exist for this user and affiliation unit'; end if;
end $$;`, subjDN, issuer, uName, unitName))
	if err == nil {
		fmt.Fprintf(w, "{ \"status\": \"success\" }")
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		if strings.Contains(err.Error(), `DN and issuer already exist`) {
			fmt.Fprintf(w, "{ \"status\": \"DN and issuer already exist.\" }")
		} else {
			log.Print(err.Error())
			fmt.Fprintf(w, "{ \"error\": \"Something went wrong.\" }")
		}

	}

	DBtx.Commit(cKey)
}

func removeUserCertificateDN(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

	authorized, authout := authorize(r, AuthorizedDNs)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"error\": \""+authout+"not authorized.\" }")
		return
	}

	q := r.URL.Query()
	uName := q.Get("username")
	subjDN := q.Get("dn")
	if uName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No username specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No username specified.\" }")
		return
	}
	if subjDN == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No DN specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No dn specified.\" }")
		return
	}

	cKey, err := DBtx.Start(DBptr)
	if err != nil {
		log.Fatal(err)
	}

	_, err = DBtx.Exec(fmt.Sprintf(`do $$ 	
declare  u_uid int;
u_dn constant text := '%s';
u_uname constant text := '%s';

begin
select uid into u_uid from users where uname=u_uname;
delete from user_certificate where dn=u_dn and uid=u_uid;
end $$;`, subjDN, uName))
	if err == nil {
		fmt.Fprintf(w, "{ \"status\": \"success\" }")
		DBtx.Commit(cKey)
	} else {
		log.Print(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "{ \"error\": \""+err.Error()+"\" }")
	}
}

func setUserInfo(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	uid := q.Get("uid")
	uName := q.Get("username")
	fName := q.Get("fullname")
	status := q.Get("status")
	eDate := q.Get("expiration_date")

	if uid == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No uid specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No uid specified.\" }")
		return
	}
	if uName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No username specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No username specified.\" }")
		return
	}
	if fName == "" {
		fName = "null"
	} else {
		fName = fmt.Sprintf("'%s'", fName)
	}
	if status == "" {
		status = "null"
	} else {
		_, err := strconv.ParseBool(status)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			log.Print("Invalid is_leader specified in http query.")
			fmt.Fprintf(w, "{ \"error\": \"Invalid is_leader specified.\" }")
			return
		}
	}

	if eDate == "" {
		eDate = "null"
	} else {
		eDate = fmt.Sprintf("'%s'", eDate)
	}
	cKey, err := DBtx.Start(DBptr)
	if err != nil {
		log.Fatal(err)
	}

	_, err = DBtx.Exec(fmt.Sprintf(`do $$
									declare c_uid constant int := %s;

									begin
										if c_uid not in (select uid from users) then
											raise 'uid does not exist';
										end if;

										update users set
											uname = '%s',
											full_name = coalesce(%s, full_name),
											status = coalesce(%s, status),
											expiration_date = coalesce(%s, expiration_date),
											last_updated = NOW()
										where uid = c_uid;
									end $$;`, uid, uName, fName, status, eDate))

	if err == nil {
		fmt.Fprintf(w, "{ \"status\": \"success\" }")
		DBtx.Commit(cKey)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		if strings.Contains(err.Error(), `uid does not exist`) {
			fmt.Fprintf(w, "{ \"error\": \"User does not exist.\" }")
		} else if strings.Contains(err.Error(), `invalid input syntax for type date`) ||
			strings.Contains(err.Error(), `date/time field value out of range`) {
			fmt.Fprintf(w, "{ \"error\": \"Invalid expiration date.\" }")
		} else {
			log.Print(err.Error())
			fmt.Fprintf(w, "{ \"error\": \"Something went wrong.\" }")
		}
	}
}

func createUser(w http.ResponseWriter, r *http.Request) {

	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

	authorized, authout := authorize(r, AuthorizedDNs)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"error\": \""+authout+"not authorized.\" }")
		return
	}

	q := r.URL.Query()
	uid := q.Get("uid")
	uName := q.Get("username")
	firstName := q.Get("firstname")
	lastName := q.Get("lastname")
	status, err := strconv.ParseBool(q.Get("status"))
	expdate := q.Get("expirationdate")

	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("Invalid status specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"Invalid status value. Must be true or false.\" }")
		return
	}
	if uName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No username specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No username specified.\" }")
		return
	}
	if uid == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No UID specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No uid specified.\" }")
		return
	}
	if firstName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No first name specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No firstname specified.\" }")
		return
	}
	if lastName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No last name specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No lastname specified.\" }")
		return
	}

	fullname := firstName + " " + lastName

	var checkExist string
	checkerr := DBptr.QueryRow(`select uname from users where uname=$1 and uid=$2 and full_name=$3`, uName, uid, fullname).Scan(&checkExist)
	switch {
	case checkerr == sql.ErrNoRows:
		cKey, err := DBtx.Start(DBptr)
		if err != nil {
			log.Fatal(err)
		}
		//actually insert
		_, err = DBtx.Exec(`insert into users (uname, uid, full_name, status, expiration_date, last_updated) values $1,$2,$3,$4,$5,NOW()`, uName, uid, fullname, status, expdate)

		if err == nil {
			fmt.Fprintf(w, "{ \"status\": \"success\" }")
			DBtx.Commit(cKey)
			return
		} else {
			log.Print(err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "{ \"error\": \""+err.Error()+"\" }")
		}
	case checkerr != nil:

		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "{ \"error\": \""+checkerr.Error()+"\" }")
	default:
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "{ \"error\": \"user "+uName+" already exists.\"}")
	}

}

func getMemberAffiliations(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	type jsonerror struct {
		Error string `json:"error"`
	}
	var inputErr []jsonerror

	user := q.Get("username")
	expOnly := false

	if user == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No username specified in http query.")
		inputErr = append(inputErr, jsonerror{"No username specified."})
	}
	if q.Get("experimentsonly") != "" {
		var err error
		if expOnly, err = strconv.ParseBool(q.Get("experimentsonly")); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			log.Print("Invalid experimentsonly specified in http query.")
			inputErr = append(inputErr, jsonerror{"Invalid experimentsonly specified."})
		}
	}

	if len(inputErr) > 0 {
		jsonout, err := json.Marshal(inputErr)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Fprintf(w, string(jsonout))
		return
	}

	rows, err := DBptr.Query(`select name, alternative_name, user_exists from (
									select 1 as key, * from (
										select distinct * from (select au.name, au.alternative_name from affiliation_units as au
																right join grid_access as ga on au.unitid = ga.unitid left join users as u on ga.uid = u.uid
																where u.uname = $1 and ((voms_url is not null = $2) or not $2)) as u

										union                  (select au.name, au.alternative_name from affiliation_units as au
																right join user_certificate as uc on au.unitid = uc.unitid left join users as u on uc.uid = u.uid
																where u.uname = $1 and ((voms_url is not null = $2) or not $2))

										union                  (select au.name, au.alternative_name from affiliation_units as au
																right join affiliation_unit_group as ag on au.unitid = ag.unitid join user_group as ug on ag.groupid = ug.groupid
																left join users as u on ug.uid = u.uid
																where u.uname = $1 and ((voms_url is not null = $2) or not $2))
									) as t
									right join (select 1 as key, $1 in (select uname from users) as user_exists) as c on key = c.key
							 ) as r;`, user, expOnly)

	if err != nil {
		defer log.Fatal(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"error\": \"Error in DB query.\" }")
		return
	}
	defer rows.Close()

	var userExists bool

	type jsonentry struct {
		Unit  string `json:"unitname"`
		Aname string `json:"alternativename"`
	}
	var Entry jsonentry
	var Out []jsonentry

	for rows.Next() {
		var tmpUnit, tmpAname sql.NullString
		rows.Scan(&tmpUnit, &tmpAname, &userExists)

		if tmpUnit.Valid {
			Entry.Unit = tmpUnit.String
			Entry.Aname = tmpAname.String
			Out = append(Out, Entry)
		}
	}

	var output interface{}
	if len(Out) == 0 {
		w.WriteHeader(http.StatusNotFound)
		var queryErr []jsonerror
		if !userExists {
			queryErr = append(queryErr, jsonerror{"User does not exist."})
		} else {
			queryErr = append(queryErr, jsonerror{"User does not belong to any affiliation unit or experiment."})
		}
		output = queryErr
	} else {
		output = Out
	}
	jsonout, err := json.Marshal(output)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Fprintf(w, string(jsonout))
}

func getUserUID(w http.ResponseWriter, r *http.Request) {

	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	uName := q.Get("username")
	if uName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No username specified in http query.")
		fmt.Fprintf(w,"{ \"error\": \"No username specified.\" }")
		return
	}
	var uid int
	checkerr := DBptr.QueryRow(`select uid from users where uname=$1`, uName).Scan(&uid)
	
	switch {
	case checkerr == sql.ErrNoRows: 
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"error\": \"User does not exist.\" }")
		log.Print("user " + uName + " not found in DB.")
		return
		
	case checkerr != nil: 
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "{ \"error\": \"Error in DB query.\" }")
		log.Print("Error in DB query for " + uName + ": " + checkerr.Error())
		return
	default:
		fmt.Fprintf(w, "{ \"uid\": " + strconv.Itoa(uid) + " }")	
		return
	}
}

func getUserUname(w http.ResponseWriter, r *http.Request) {

	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	uidstr := q.Get("uid")
	uid,err := strconv.Atoi(uidstr)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("Invalid uid specified (either missing or not an integer).")
		fmt.Fprintf(w,"{ \"error\": \"Invalid uid specified.\" }")
		return	
	}
	if uidstr == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No uid specified in http query.")
		fmt.Fprintf(w,"{ \"error\": \"No uid specified.\" }")
		return
	}
	var uname string
	checkerr := DBptr.QueryRow(`select uid from users where uid=$1`, uid).Scan(&uname)
	
	switch {
	case checkerr == sql.ErrNoRows: 
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"error\": \"User does not exist.\" }")
		log.Print("user ID " + uidstr + " not found in DB.")
		return
		
	case checkerr != nil:
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "{ \"error\": \"Error in DB query.\" }")
		log.Print("Error in DB query for " + uidstr + ": " + checkerr.Error())
		return
	default:
		fmt.Fprintf(w, "{ \"uname\": \"" + uname  + "\" }")	
		return
	}
}

