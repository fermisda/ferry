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
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	uname := q.Get("username")
	expt := q.Get("experimentname")
	if uname == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Error("No username specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No username specified.\" }")
		return
	}
	if expt == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Error("No experiment name specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No experimentname specified.\" }")
		return
	}

	authorized, authout := authorize(r, AuthorizedDNs)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"error\": \""+authout+"not authorized.\" }")
		return
	}

	rows, err := DBptr.Query(`select name, dn, issuer_ca, user_exists, unit_exists from (
								select 1 as key, name, uc.dn, issuer_ca from affiliation_unit_user_certificate as ac
								left join user_certificates as uc on ac.dn = uc.dn
								left join users as u on uc.uid = u.uid
								left join affiliation_units as au on ac.unitid = au.unitid
								where uname = $1 and name = $2
							) as t right join (
								select 1 as key,
								$1 in (select uname from users) as user_exists,
								$2 in (select name from affiliation_units) as unit_exists
							) as c on t.key = c.key;`, uname, expt)
	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Fatal(err)
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
				log.WithFields(QueryFields(r, startTime)).Fatal(jsonerr)
			}
			output += string(outline)
			idx++
		}
	}
	if idx == 0 {
		w.WriteHeader(http.StatusNotFound)

		if !userExists {
			output += `"error": "User does not exist.",`
			log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
		}
		if !exptExists {
			output += `"error": "Experiment does not exist.",`
			log.WithFields(QueryFields(r, startTime)).Error("Experiment does not exist.")
		}
		output += `"error": "User does not have any certifcates registered."`
		log.WithFields(QueryFields(r, startTime)).Error("User does not have any certifcates registered.")
	} else {
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
	}

	output += " ]"
	fmt.Fprintf(w, output)
}

func getAllUsersCertificateDNs(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	type jsonerror struct {
		Error string `json:"error"`
	}
	var inputErr []jsonerror

	expt := q.Get("experimentname")
	if expt == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Error("No experiment name specified in http query.")
		inputErr = append(inputErr, jsonerror{"No experiment name specified in http query."})
	}
	if len(inputErr) > 0 {
		jsonout, err := json.Marshal(inputErr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Fatal(err)
		}
		fmt.Fprintf(w, string(jsonout))
		return
	}

	authorized, authout := authorize(r, AuthorizedDNs)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"error\": \""+authout+"not authorized.\" }")
		return
	}

	rows, err := DBptr.Query(`select uname, name, dn, issuer_ca, unit_exists from (
								select 1 as key, uname, name, uc.dn, issuer_ca from affiliation_unit_user_certificate as ac
								left join user_certificates as uc on ac.dn = uc.dn
								left join users as u on uc.uid = u.uid
								left join affiliation_units as au on ac.unitid = au.unitid
								where name = $1 order by uname
							) as t right join (
								select 1 as key,
								$1 in (select name from affiliation_units) as unit_exists
							) as c on t.key = c.key;`, expt)
	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Fatal(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"error\": \"Error in DB query.\" }")
		return
	}
	defer rows.Close()

	var exptExists bool
	type jsoncert struct {
		UnitName string `json:"unit_name"`
		DN       string `json:"dn"`
		Issuer   string `json:"issuer_ca"`
	}
	type jsonuser struct {
		Uname string `json:"username"`
		Certs []jsoncert `json:"certificates"`
	}
	var Out []jsonuser

	prevUname := ""
	for rows.Next() {
		var tmpUname, tmpUnitName, tmpDN, tmpIssuer sql.NullString
		rows.Scan(&tmpUname, &tmpUnitName, &tmpDN, &tmpIssuer, &exptExists)
		if tmpUname.Valid {
			if prevUname != tmpUname.String {
				Out = append(Out, jsonuser{tmpUname.String, make([]jsoncert, 0)})
				prevUname = tmpUname.String
			}
			Out[len(Out)-1].Certs = append(Out[len(Out)-1].Certs, jsoncert{tmpUnitName.String, tmpDN.String, tmpIssuer.String})
		}
	}

	var output interface{}	
	if len(Out) == 0 {
		var queryErr []jsonerror
		if !exptExists {
			queryErr = append(queryErr, jsonerror{"Experiment does not exist."})
			log.WithFields(QueryFields(r, startTime)).Error("Experiment does not exist.")
		} else {
			queryErr = append(queryErr, jsonerror{"Query returned no users."})
			log.WithFields(QueryFields(r, startTime)).Error("Query returned no users.")
		}
		output = queryErr
	} else {
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		output = Out
	}
	jsonoutput, err := json.Marshal(output)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err.Error())
	}
	fmt.Fprintf(w, string(jsonoutput))
}

func getUserFQANs(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	uname := q.Get("username")
	expt := q.Get("experimentname")
	if uname == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Error("No username specified in http query.")
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
		defer log.WithFields(QueryFields(r, startTime)).Fatal(err)
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
				log.WithFields(QueryFields(r, startTime)).Fatal(jsonerr)
			}
			output += string(outline)
			idx++
		}
	}
	if idx == 0 {
		w.WriteHeader(http.StatusNotFound)

		if !userExists {
			output += `"error": "User does not exist.",`
			log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
		}
		if !exptExists {
			output += `"error": "Experiment does not exist.",`
			log.WithFields(QueryFields(r, startTime)).Error("Experiment does not exist.")
		}
		output += `"error": "User do not have any assigned FQANs."`
		log.WithFields(QueryFields(r, startTime)).Error("User do not have any assigned FQANs.")
	} else {
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
	}

	output += " ]"
	fmt.Fprintf(w, output)
}

func getSuperUserList(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	expt := q.Get("experimentname")
	if expt == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Error("No experimentname specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No username specified.\" }")
		return
	}

	rows, err := DBptr.Query(`select t1.uname, c.unit_exists from 
		                     (select distinct 1 as key, us.uname from users as us right join grid_access as ga on us.uid=ga.uid
							  left join affiliation_units as au on ga.unitid = au.unitid where ga.is_superuser=true and au.name=$1) as t1
							  right join (select 1 as key, $1 in (select name from affiliation_units) as unit_exists) as c on c.key = t1.key`, expt)
	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Fatal(err)
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
				log.WithFields(QueryFields(r, startTime)).Fatal(jsonerr)
			}
			output += string(outline)
			idx++
		}
	}
	if idx == 0 {
		w.WriteHeader(http.StatusNotFound)

		if !exptExists {
			output += `"error": "Experiment does not exist.",`
			log.WithFields(QueryFields(r, startTime)).Error("Experiment does not exist.")
		}
		output += `"error": "No super users found."`
		log.WithFields(QueryFields(r, startTime)).Error("No super users found.")
	} else {	
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
	}

	output += " ]"
	fmt.Fprintf(w, output)
}

func setSuperUser(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
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
		log.WithFields(QueryFields(r, startTime)).Error("No user name specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No username specified.\" }")
		return
	}
	if unitName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Error("No unit name specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No unitname specified.\" }")
		return
	}

	cKey, err := DBtx.Start(DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
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
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		fmt.Fprintf(w, "{ \"status\": \"success\" }")
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		if strings.Contains(err.Error(), `User does not exist`) {
			log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
			fmt.Fprintf(w, "{ \"error\": \"User does not exist.\" }")
		} else if strings.Contains(err.Error(), `Unit does not exist`) {
			log.WithFields(QueryFields(r, startTime)).Error("Unit does not exist.")
			fmt.Fprintf(w, "{ \"error\": \"Unit does not exist.\" }")
		} else {
			log.WithFields(QueryFields(r, startTime)).Error(err.Error())
			fmt.Fprintf(w, "{ \"error\": \"Something went wrong.\" }")
		}
	}

	DBtx.Commit(cKey)
}

func getUserGroups(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	uname := q.Get("username")
	if uname == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Error("No username specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No username specified.\" }")
		return
	}
	pingerr := DBptr.Ping()
	if pingerr != nil {
		log.WithFields(QueryFields(r, startTime)).Fatal(pingerr)
	}
	rows, err := DBptr.Query(`select groups.gid, groups.name from groups INNER JOIN user_group on (groups.groupid = user_group.groupid) INNER JOIN users on (user_group.uid = users.uid) where users.uname=$1`, uname)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Fatal(err)
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
				log.WithFields(QueryFields(r, startTime)).Fatal(jsonerr)
			}
			fmt.Fprintf(w, string(outline))
			idx += 1
		}
		if idx == 0 {
			w.WriteHeader(http.StatusNotFound)
			log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
			fmt.Fprintf(w, `{ "error": "User does not exist." }`)
		} else {
			log.WithFields(QueryFields(r, startTime)).Info("Success!")
			fmt.Fprintf(w, " ]")
		}
	}
}

func getUserInfo(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	uname := q.Get("username")
	if uname == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Error("No username specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No username specified.\" }")
		return
	}
	pingerr := DBptr.Ping()
	if pingerr != nil {
		log.WithFields(QueryFields(r, startTime)).Fatal(pingerr)
	}
	rows, err := DBptr.Query(`select full_name, uid, status, expiration_date from users where uname=$1`, uname)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Fatal(err)
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
				log.WithFields(QueryFields(r, startTime)).Fatal(jsonerr)
			}
			fmt.Fprintf(w, string(outline))
			idx += 1
		}
		if idx == 0 {
			w.WriteHeader(http.StatusNotFound)
			log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
			fmt.Fprintf(w, `{ "error": "User does not exist." }`)
		} else {
			
			log.WithFields(QueryFields(r, startTime)).Info("Success!")
			fmt.Fprintf(w, " ]")
		}
	}
}

func addUserToGroup(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	uName := q.Get("username")
	gName := q.Get("groupname")
	isLeader := q.Get("is_leader")

	if uName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Error("No username specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No username specified.\" }")
		return
	}
	if gName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Error("No groupname specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No groupname specified.\" }")
		return
	}
	if isLeader == "" {
		isLeader = "false"
	} else {
		_, err := strconv.ParseBool(q.Get("is_leader"))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			log.WithFields(QueryFields(r, startTime)).Error("Invalid is_leader specified in http query.")
			fmt.Fprintf(w, "{ \"error\": \"Invalid is_leader specified.\" }")
			return
		}
	}

	authorized, authout := authorize(r, AuthorizedDNs)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"error\": \""+authout+"not authorized.\" }")
		return
	}

	cKey, err := DBtx.Start(DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Fatal(err)
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
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		fmt.Fprintf(w, "{ \"status\": \"success\" }")
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		if strings.Contains(err.Error(), `duplicate key value violates unique constraint`) {
			log.WithFields(QueryFields(r, startTime)).Error("User already belongs to this group.")
			fmt.Fprintf(w, "{ \"error\": \"User already belongs to this group.\" }")
		} else if strings.Contains(err.Error(), `null value in column "uid" violates not-null constraint`) {
			log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
			fmt.Fprintf(w, "{ \"error\": \"User does not exist.\" }")
		} else if strings.Contains(err.Error(), `null value in column "groupid" violates not-null constraint`) {
			log.WithFields(QueryFields(r, startTime)).Error("Group does not exist.")
			fmt.Fprintf(w, "{ \"error\": \"Group does not exist.\" }")
		} else {
			log.WithFields(QueryFields(r, startTime)).Error(err.Error())
			fmt.Fprintf(w, "{ \"error\": \"Something went wrong.\" }")
		}
	}

	DBtx.Commit(cKey)
}

func setUserExperimentFQAN(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	uName := q.Get("username")
	fqan := q.Get("fqan")
	eName := q.Get("experimentname")

	if uName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Error("No username specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No username specified.\" }")
		return
	}
	if fqan == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Error("No fqan specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No fqan specified.\" }")
		return
	}
	if eName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Error("No experimentname specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No experimentname specified.\" }")
		return
	}

	authorized, authout := authorize(r, AuthorizedDNs)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"error\": \""+authout+"not authorized.\" }")
		return
	}

	cKey, err := DBtx.Start(DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Fatal(err)
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
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		fmt.Fprintf(w, "{ \"status\": \"success\" }")
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		if strings.Contains(err.Error(), `null value in column "uid" violates not-null constraint`) {
			log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
			fmt.Fprintf(w, "{ \"error\": \"User does not exist.\" }")
		} else if strings.Contains(err.Error(), `null value in column "fqanid" violates not-null constraint`) {
			log.WithFields(QueryFields(r, startTime)).Error("FQAN does not exist.")
			fmt.Fprintf(w, "{ \"error\": \"FQAN does not exist.\" }")
		} else if strings.Contains(err.Error(), `null value in column "unitid" violates not-null constraint`) {
			log.WithFields(QueryFields(r, startTime)).Error("Experiment does not exist.")
			fmt.Fprintf(w, "{ \"error\": \"Experiment does not exist.\" }")
		} else if strings.Contains(err.Error(), `duplicate key value violates unique constraint "idx_grid_access"`) {
			log.WithFields(QueryFields(r, startTime)).Error("This association already exists.")
			fmt.Fprintf(w, "{ \"error\": \"This association already exists.\" }")
		} else {
			log.WithFields(QueryFields(r, startTime)).Error(err.Error())
			fmt.Fprintf(w, "{ \"error\": \"Something went wrong.\" }")
		}
	}

	DBtx.Commit(cKey)
}

func setUserShellAndHomeDir(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	rName := q.Get("resourcename")
	uName := q.Get("username")
	shell := q.Get("shell")
	hDir := q.Get("homedir")

	if rName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Error("No resourcename specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No resourcename specified.\" }")
		return
	}
	if uName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Error("No username specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No username specified.\" }")
		return
	}
	if shell == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Error("No shell specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No shell specified.\" }")
		return
	}
	if hDir == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Error("No homedir specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No homedir specified.\" }")
		return
	}

	authorized, authout := authorize(r, AuthorizedDNs)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"error\": \""+authout+"not authorized.\" }")
		return
	}

	cKey, err := DBtx.Start(DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Fatal(err)
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
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		fmt.Fprintf(w, "{ \"status\": \"success\" }")
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		if strings.Contains(err.Error(), `User does not exist.`) {
			log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
			fmt.Fprintf(w, "{ \"error\": \"User does not exist.\" }")
		} else if strings.Contains(err.Error(), `Resource does not exist.`) {
			log.WithFields(QueryFields(r, startTime)).Error("Resource does not exist.")
			fmt.Fprintf(w, "{ \"error\": \"Resource does not exist.\" }")
		} else {
			log.WithFields(QueryFields(r, startTime)).Error(err.Error())
			fmt.Fprintf(w, "{ \"error\": \"Something went wrong.\" }")
		}
	}

	DBtx.Commit(cKey)
}

func getUserShellAndHomeDir(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	comp := q.Get("resourcename")
	user := q.Get("username")
	if comp == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Error("No resourcename specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No resourcename specified.\" }")
		return
	}
	if user == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Error("No username specified in http query.")
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
		defer log.WithFields(QueryFields(r, startTime)).Fatal(err)
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
				log.WithFields(QueryFields(r, startTime)).Fatal(jsonerr)
			}
			output += string(outline)
			idx++
		}
	}
	if idx == 0 {
		w.WriteHeader(http.StatusNotFound)

		if !compExists {
			output += `"error": "Resource does not exist.",`
			log.WithFields(QueryFields(r, startTime)).Error("Resource does not exist.")
		}
		if !userExists {
			output += `"error": "User does not exist.",`
			log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
		}
		output += `"error": "No super users found."`
		log.WithFields(QueryFields(r, startTime)).Error("No super users found.")
	} else {
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
	}

	output += " ]"
	fmt.Fprintf(w, output)
}
func getUserStorageQuota(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	rName := q.Get("resourcename")
	uName := q.Get("username")
	unitName := q.Get("unitname")

	if rName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Error("No resource name specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No resourcename specified.\" }")
		return

	}
	if uName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Error("No user name specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No username specified.\" }")
		return
	}
	if unitName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Error("No unit name specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No unitname specified.\" }")
		return
	}

	rows, err := DBptr.Query(`select sq.path,sq.value, sq.unit, sq.valid_until from storage_quota sq INNER JOIN affiliation_units on affiliation_units.unitid = sq.unitid INNER JOIN storage_resources on storage_resources.storageid = sq.storageid INNER JOIN users on users.uid = sq.uid where affiliation_units.name=$1 AND storage_resources.type=$2 and users.uname=$3`, unitName, rName, uName)
	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Fatal(err)
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
				log.WithFields(QueryFields(r, startTime)).Fatal(jsonerr)
			}
			output += string(outline)
			idx++
		}
	}
	if idx == 0 {
		w.WriteHeader(http.StatusNotFound)
		output += `{"error": "User has no quotas registered."}`
		log.WithFields(QueryFields(r, startTime)).Error("User has no quotas registered.")
	} else {
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
	}
	fmt.Fprintf(w, output)
}

func setUserStorageQuota(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
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
		log.WithFields(QueryFields(r, startTime)).Error("No quota value specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No quota specified.\" }")
		return
	}
	if validtime != "" {
		validtime = "valid_until = " + validtime + ","
	}
	if uName == "" && isGroup == false {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Error("No user name given and isGroup was set to false.")
		fmt.Fprintf(w, "{ \"error\": \"No username provided.\" }")
		return
	}
	if rName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Error("No resource type given.")
		fmt.Fprintf(w, "{ \"error\": \"No resourcename provided.\" }")
		return
	}
	if unitName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Error("No affiliation unit given.")
		fmt.Fprintf(w, "{ \"error\": \"No unitname provided.\" }")
		return
	}
	//set a default unit of "B" for bytes
	if unit == "" {
		unit = "B"
	}

	cKey, err := DBtx.Start(DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Fatal(err)
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
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		fmt.Fprintf(w, "{ \"status\": \"success\" }")
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		if strings.Contains(err.Error(), `User does not exist.`) {
			log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
			fmt.Fprintf(w, "{ \"error\": \"User does not exist.\" }")
		} else if strings.Contains(err.Error(), `Resource does not exist.`) {
			log.WithFields(QueryFields(r, startTime)).Error("Resource does not exist.")
			fmt.Fprintf(w, "{ \"error\": \"Resource does not exist.\" }")
		} else {
			log.WithFields(QueryFields(r, startTime)).Error(err.Error())
			fmt.Fprintf(w, "{ \"error\": \"Something went wrong.\" }")
		}
	}

	DBtx.Commit(cKey)
}
func setUserExternalAffiliationAttribute(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	uName := q.Get("username")
	attribute := q.Get("attribute")
	value := q.Get("value")

	if uName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Error("No username specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No username specified.\" }")
		return
	}
	if attribute == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Error("No attribute specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No attribute specified.\" }")
		return
	}
	if value == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Error("No value specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No value specified.\" }")
		return
	}

	authorized, authout := authorize(r, AuthorizedDNs)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"error\": \""+authout+"not authorized.\" }")
		return
	}

	cKey, err := DBtx.Start(DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Fatal(err)
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
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		fmt.Fprintf(w, "{ \"status\": \"success\" }")
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		if strings.Contains(err.Error(), `uname does not exist`) {
			log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
			fmt.Fprintf(w, "{ \"error\": \"User does not exist.\" }")
		} else {
			log.WithFields(QueryFields(r, startTime)).Error(err.Error())
			fmt.Fprintf(w, "{ \"error\": \"Something went wrong.\" }")
		}
	}

	DBtx.Commit(cKey)
}
func removeUserExternalAffiliationAttribute(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	uName := q.Get("username")
	attribute := q.Get("attribute")

	if uName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Error("No username specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No username specified.\" }")
		return
	}
	if attribute == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Error("No attribute specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No attribute specified.\" }")
		return
	}

	authorized, authout := authorize(r, AuthorizedDNs)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"error\": \""+authout+"not authorized.\" }")
		return
	}

	cKey, err := DBtx.Start(DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Fatal(err)
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
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		fmt.Fprintf(w, "{ \"status\": \"success\" }")
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		if strings.Contains(err.Error(), `uname does not exist`) {
			log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
			fmt.Fprintf(w, "{ \"error\": \"User does not exist.\" }")
		} else if strings.Contains(err.Error(), `attribute does not exist`) {
			log.WithFields(QueryFields(r, startTime)).Error("External affiliation attribute does not exist.")
			fmt.Fprintf(w, "{ \"error\": \"External affiliation attribute does not exist.\" }")
		} else {
			log.WithFields(QueryFields(r, startTime)).Error(err.Error())
			fmt.Fprintf(w, "{ \"error\": \"Something went wrong.\" }")
		}
	}

	DBtx.Commit(cKey)
}
func getUserExternalAffiliationAttributes(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	user := q.Get("username")

	if user == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Error("No username specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No username specified.\" }")
		return
	}

	rows, err := DBptr.Query(`select attribute, value, user_exists from
							 (select 1 as key, a.attribute, a.value, u.uname from external_affiliation_attribute as a 
							  left join users as u on a.uid = u.uid where uname = $1) as t right join
							 (select 1 as key, $1 in (select uname from users) as user_exists) as c on t.key = c.key;`, user)

	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Fatal(err)
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
			log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
		} else {
			Err = append(Err, jsonerror{"User does not have external affiliation attributes"})
			log.WithFields(QueryFields(r, startTime)).Error("User does not have external affiliation attributes")
		}
		output = Err
	} else {
		output = Out
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
	}
	jsonout, err := json.Marshal(output)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Fatal(err)
	}
	fmt.Fprintf(w, string(jsonout))

}

func addCertificateDNToUser(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
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
		log.WithFields(QueryFields(r, startTime)).Error("No username specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No username specified.\" }")
		return
	}
	if subjDN == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Error("No DN specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No dn specified.\" }")
		return
	}
	if issuer == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Error("No issuer specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No issuer specified.\" }")
		return
	}

	cKey, err := DBtx.Start(DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Fatal(err)
	}

	_, err = DBtx.Exec(fmt.Sprintf(`do $$ 	
										declare u_uid int;
										declare au_unitid int;
										declare new_dn bool;
										u_dn constant text := '%s';
										u_issuer constant text := '%s';
										u_uname constant text := '%s';
										au_name constant text := '%s';
									begin
										new_dn = false;
										select uid into u_uid from users where uname=u_uname;
										if u_dn not in (select dn from user_certificates) then
											new_dn = true;
											insert into user_certificates (dn, uid, issuer_ca, last_updated) values (u_dn, u_uid, u_issuer, NOW());
										end if;
										if au_name != '' then
											select unitid into au_unitid from affiliation_units where name = au_name;
											insert into affiliation_unit_user_certificate (unitid, dn, last_updated) values (au_unitid, u_dn, NOW());
										else
											if not new_dn then
												raise 'duplicated dn';
											end if;
										end if;
									end $$;`, subjDN, issuer, uName, unitName))
	if err == nil {
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		fmt.Fprintf(w, "{ \"status\": \"success\" }")
	} else {
		log.Print(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		if strings.Contains(err.Error(), `pk_affiliation_unit_user_certificate_dn`) {
			log.WithFields(QueryFields(r, startTime)).Error("DN already exists and is assigned to this affiliation unit.")
			fmt.Fprintf(w, "{ \"status\": \"DN already exists and is assigned to this affiliation unit.\" }")
		} else if strings.Contains(err.Error(), `duplicated dn`) {
			log.WithFields(QueryFields(r, startTime)).Error("DN already exists.")
			fmt.Fprintf(w, "{ \"status\": \"DN already exists.\" }")
		} else if strings.Contains(err.Error(), `"uid" violates not-null constraint`) {
			log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
			fmt.Fprintf(w, "{ \"status\": \"User does not exist.\" }")
		} else if strings.Contains(err.Error(), `"unitid" violates not-null constraint`) {
			log.WithFields(QueryFields(r, startTime)).Error("Affiliation unit does not exist.")
			fmt.Fprintf(w, "{ \"status\": \"Affiliation unit does not exist.\" }")
		} else {
			log.WithFields(QueryFields(r, startTime)).Error(err.Error())
			fmt.Fprintf(w, "{ \"error\": \"Something went wrong.\" }")
		}

	}

	DBtx.Commit(cKey)
}

func removeUserCertificateDN(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
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
		log.WithFields(QueryFields(r, startTime)).Error("No username specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No username specified.\" }")
		return
	}
	if subjDN == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Error("No DN specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No dn specified.\" }")
		return
	}

	cKey, err := DBtx.Start(DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Fatal(err)
	}

	_, err = DBtx.Exec(fmt.Sprintf(`do $$ 	
										declare  u_uid int;
										u_dn constant text := '%s';
										u_uname constant text := '%s';

									begin
										select uid into u_uid from users where uname=u_uname;
										if u_uid is null then
											raise 'uname does not exist';
										end if;
										if u_dn not in (select dn from user_certificates) then
											raise 'dn does not exist';
										end if;
										if (u_dn, u_uid) not in (select dn, uid from user_certificates) then
											raise 'dn uid association does not exist';
										end if;

										delete from affiliation_unit_user_certificate where dn=u_dn;
										delete from user_certificates where dn=u_dn and uid=u_uid;
									end $$;`, subjDN, uName))
	if err == nil {
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		fmt.Fprintf(w, "{ \"status\": \"success\" }")
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		if strings.Contains(err.Error(), `uname does not exist`) {
			log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
			fmt.Fprintf(w, "{ \"error\": \"User does not exist.\" }")
		} else if strings.Contains(err.Error(), `dn does not exist`) {
			log.WithFields(QueryFields(r, startTime)).Error("Certificate DN does not exist.")
			fmt.Fprintf(w, "{ \"error\": \"Certificate DN does not exist.\" }")
		} else if strings.Contains(err.Error(), `dn uid association does not exist`) {
			log.WithFields(QueryFields(r, startTime)).Error("DN UID association does not exist.")
			fmt.Fprintf(w, "{ \"error\": \"DN UID association does not exist.\" }")
		} else {
			log.WithFields(QueryFields(r, startTime)).Error(err.Error())
			fmt.Fprintf(w, "{ \"error\": \"Something went wrong.\" }")
		}
	}

	DBtx.Commit(cKey)
}

func setUserInfo(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	uid := q.Get("uid")
	uName := q.Get("username")
	fName := q.Get("fullname")
	status := q.Get("status")
	eDate := q.Get("expiration_date")

	if uid == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Error("No uid specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No uid specified.\" }")
		return
	}
	if uName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Error("No username specified in http query.")
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
			log.WithFields(QueryFields(r, startTime)).Error("Invalid is_leader specified in http query.")
			fmt.Fprintf(w, "{ \"error\": \"Invalid is_leader specified.\" }")
			return
		}
	}

	if eDate == "" {
		eDate = "null"
	} else {
		eDate = fmt.Sprintf("'%s'", eDate)
	}

	authorized, authout := authorize(r, AuthorizedDNs)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"error\": \""+authout+"not authorized.\" }")
		return
	}

	cKey, err := DBtx.Start(DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Fatal(err)
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
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		fmt.Fprintf(w, "{ \"status\": \"success\" }")
		DBtx.Commit(cKey)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		if strings.Contains(err.Error(), `uid does not exist`) {
			log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
			fmt.Fprintf(w, "{ \"error\": \"User does not exist.\" }")
		} else if strings.Contains(err.Error(), `invalid input syntax for type date`) ||
			strings.Contains(err.Error(), `date/time field value out of range`) {
			log.WithFields(QueryFields(r, startTime)).Error("Invalid expiration date.")
			fmt.Fprintf(w, "{ \"error\": \"Invalid expiration date.\" }")
		} else {
			log.WithFields(QueryFields(r, startTime)).Error(err.Error())
			fmt.Fprintf(w, "{ \"error\": \"Something went wrong.\" }")
		}
	}
}

func createUser(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
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
		log.WithFields(QueryFields(r, startTime)).Error("Invalid status specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"Invalid status value. Must be true or false.\" }")
		return
	}
	if uName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Error("No username specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No username specified.\" }")
		return
	}
	if uid == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Error("No UID specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No uid specified.\" }")
		return
	}
	if firstName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Error("No first name specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No firstname specified.\" }")
		return
	}
	if lastName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Error("No last name specified in http query.")
		fmt.Fprintf(w, "{ \"error\": \"No lastname specified.\" }")
		return
	}
	if expdate == "" {
		expdate = "2038-01-01"
	}
	
	fullname := firstName + " " + lastName

	var checkExist string
	checkerr := DBptr.QueryRow(`select uname from users where uname=$1 and uid=$2 and full_name=$3`, uName, uid, fullname).Scan(&checkExist)
	switch {
	case checkerr == sql.ErrNoRows:
		cKey, err := DBtx.Start(DBptr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Fatal(err)
		}
		//actually insert
//		_, err = DBtx.Exec(`insert into users (uname, uid, full_name, status, expiration_date, last_updated) values $1,$2,$3,$4,$5,NOW()`, uName, uid, fullname, status, expdate)

	//	theStmt := fmt.Sprintf("insert into users (uname, uid, full_name, status, expiration_date, last_updated) values ('%s',%d,'%s','%s','%s',NOW())", uName, uid, fullname, status, expdate)
	//	fmt.Println(theStmt)

		_, err = DBtx.Exec(fmt.Sprintf("insert into users (uname, uid, full_name, status, expiration_date, last_updated) values ('%s',%s,'%s',%t,'%s', NOW())", uName, uid, fullname, status, expdate))

		if err == nil {
			log.WithFields(QueryFields(r, startTime)).Info("Success!")
			fmt.Fprintf(w, "{ \"status\": \"success\" }")
			DBtx.Commit(cKey)
			return
		} else {
			log.WithFields(QueryFields(r, startTime)).Error(err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "{ \"error\": \""+err.Error()+"\" }")
		}
	case checkerr != nil:

		w.WriteHeader(http.StatusInternalServerError)
		log.WithFields(QueryFields(r, startTime)).Error(checkerr.Error())
		fmt.Fprintf(w, "{ \"error\": \""+checkerr.Error()+"\" }")
	default:
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Error("user "+uName+" already exists.")
		fmt.Fprintf(w, "{ \"error\": \"user "+uName+" already exists.\"}")
	}

}

func getMemberAffiliations(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
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
		log.WithFields(QueryFields(r, startTime)).Error("No username specified in http query.")
		inputErr = append(inputErr, jsonerror{"No username specified."})
	}
	if q.Get("experimentsonly") != "" {
		var err error
		if expOnly, err = strconv.ParseBool(q.Get("experimentsonly")); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			log.WithFields(QueryFields(r, startTime)).Error("Invalid experimentsonly specified in http query.")
			inputErr = append(inputErr, jsonerror{"Invalid experimentsonly specified."})
		}
	}

	if len(inputErr) > 0 {
		jsonout, err := json.Marshal(inputErr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Fatal(err)
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
																right join affiliation_unit_user_certificate as ac on au.unitid = ac.unitid
																left join user_certificates as uc on ac.dn = uc.dn left join users as u on uc.uid = u.uid
																where u.uname = $1 and ((voms_url is not null = $2) or not $2))

										union                  (select au.name, au.alternative_name from affiliation_units as au
																right join affiliation_unit_group as ag on au.unitid = ag.unitid join user_group as ug on ag.groupid = ug.groupid
																left join users as u on ug.uid = u.uid
																where u.uname = $1 and ((voms_url is not null = $2) or not $2))
									) as t
									right join (select 1 as key, $1 in (select uname from users) as user_exists) as c on key = c.key
							 ) as r;`, user, expOnly)

	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Fatal(err)
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
			log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
			queryErr = append(queryErr, jsonerror{"User does not exist."})
		} else {
			log.WithFields(QueryFields(r, startTime)).Error("User does not belong to any affiliation unit or experiment.")
			queryErr = append(queryErr, jsonerror{"User does not belong to any affiliation unit or experiment."})
		}
		output = queryErr
	} else {
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		output = Out
	}
	jsonout, err := json.Marshal(output)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Fatal(err)
	}
	fmt.Fprintf(w, string(jsonout))
}

func getUserUID(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	uName := q.Get("username")
	if uName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Error("No username specified in http query.")
		fmt.Fprintf(w,"{ \"error\": \"No username specified (use username=foo in the API query).\" }")
		return
	}
	var uid int
	checkerr := DBptr.QueryRow(`select uid from users where uname=$1`, uName).Scan(&uid)
	
	switch {
	case checkerr == sql.ErrNoRows: 
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"error\": \"User does not exist.\" }")
		log.WithFields(QueryFields(r, startTime)).Error("user " + uName + " not found in DB.")
		return
		
	case checkerr != nil: 
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "{ \"error\": \"Error in DB query.\" }")
		log.WithFields(QueryFields(r, startTime)).Error("Error in DB query for " + uName + ": " + checkerr.Error())
		return
	default:
		fmt.Fprintf(w, "{ \"uid\": " + strconv.Itoa(uid) + " }")	
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		return
	}
}

func getUserUname(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	uidstr := q.Get("uid")
	if uidstr == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Error("No uid specified in http query.")
		fmt.Fprintf(w,"{ \"error\": \"No uid specified (use uid=<number> in API query).\" }")
		return
	}
	uid,err := strconv.Atoi(uidstr)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Error("Invalid uid specified (either missing or not an integer).")
		fmt.Fprintf(w,"{ \"error\": \"Invalid uid format.\" }")
		return	
	}
	
	var uname string
	checkerr := DBptr.QueryRow(`select uname from users where uid=$1`, uid).Scan(&uname)
	
	switch {
	case checkerr == sql.ErrNoRows: 
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"error\": \"User does not exist.\" }")
		log.WithFields(QueryFields(r, startTime)).Error("user ID " + uidstr + " not found in DB.")
		return
		
	case checkerr != nil:
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "{ \"error\": \"Error in DB query.\" }")
		log.WithFields(QueryFields(r, startTime)).Error("Error in DB query for " + uidstr + ": " + checkerr.Error())
		return
	default:		
		fmt.Fprintf(w, "{ \"uname\": \"" + uname  + "\" }")	
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		return
	}
}

func deleteUser(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	uName := q.Get("username")
	if uName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Error("No username specified in http query.")
		fmt.Fprintf(w,"{ \"error\": \"No username specified.\" }")
		return		
	}

	authorized, authout := authorize(r, AuthorizedDNs)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"error\": \""+authout+"not authorized.\" }")
		return
	}

	// check if the username is already in the DB. If it is not, say so and exit since there is nothing to delete.
	var uname string
	checkerr := DBptr.QueryRow(`select uid from users where uname=$1`, uName).Scan(&uname)
	
	switch {
	case checkerr == sql.ErrNoRows: 
		// set the header for success since we are already at the desired result
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "{ \"status\": \"Nothing to delete; user does not exist.\" }")
		log.WithFields(QueryFields(r, startTime)).Info("user ID " + uName + " not found in DB.")
		return	
	case checkerr != nil:
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "{ \"error\": \"Nothing to delete; user does not exist.\" }")
		log.WithFields(QueryFields(r, startTime)).Error("deleteUser: Error querying DB for user " + uName + ".")
		return	
	default:
		// actually do the deletion now
		cKey, err := DBtx.Start(DBptr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error(err)
		}
		myStmt,myStmterr := DBptr.Prepare(fmt.Sprintf("delete from users where uname='%s'",uName))
		if myStmterr != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Error creating prepared statement for deleteUser(" + uName + ").")	
		}
		_, err = myStmt.Exec() 
		if err == nil {	
			fmt.Fprintf(w, "{ \"status\": \"success\" }")
			log.WithFields(QueryFields(r, startTime)).Info("Success!")
			DBtx.Commit(cKey)
			myStmt.Close()
			return
		} else {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "{ \"error\": \"%s\" }",err.Error())
			log.WithFields(QueryFields(r, startTime)).Error("deleteUser: Error during delete action for user " + uName + ": " + err.Error())
			myStmt.Close()
			return			
		}	
	}
}

func getUserAccessToComputeResources(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	type jsonerror struct {
		Error string `json:"error"`
	}
	var inputErr []jsonerror

	user := q.Get("username")

	if user == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Error("No username specified in http query.")
		inputErr = append(inputErr, jsonerror{"No username specified."})
	}

	if len(inputErr) > 0 {
		jsonout, err := json.Marshal(inputErr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Fatal(err)
		}
		fmt.Fprintf(w, string(jsonout))
		return
	}

	rows, err := DBptr.Query(`select  name, type, shell, home_dir, user_exists from
							(select 1 as key, u.uname, cr.name, cr.type, ca.* from
								compute_access as ca left join
								users as u on ca.uid = u.uid left join
								compute_resources as cr on ca.compid = cr.compid
								where u.uname = $1
							) as t 
							right join (
								select 1 as key, $1 in (select uname from users) as user_exists
							) as c on t.key = c.key;`, user)

	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Fatal(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"error\": \"Error in DB query.\" }")
		return
	}
	defer rows.Close()

	var userExists bool

	type jsonentry struct {
		Rname string `json:"resourcename"`
		Rtype string `json:"resourcetype"`
		Shell string `json:"shell"`
		Home  string `json:"home_dir"`
	}
	var Entry jsonentry
	var Out []jsonentry

	for rows.Next() {
		var tmpRname, tmpRtype, tmpShell, tmpHome sql.NullString
		rows.Scan(&tmpRname, &tmpRtype, &tmpShell, &tmpHome, &userExists)

		if tmpRname.Valid {
			Entry.Rname = tmpRname.String
			Entry.Rtype = tmpRtype.String
			Entry.Shell = tmpShell.String
			Entry.Home  = tmpHome.String
			Out = append(Out, Entry)
		}
	}

	var output interface{}
	if len(Out) == 0 {
		w.WriteHeader(http.StatusNotFound)
		var queryErr []jsonerror
		if !userExists {
			log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
			queryErr = append(queryErr, jsonerror{"User does not exist."})
		} else {
			log.WithFields(QueryFields(r, startTime)).Error("User does not have access to any compute resource.")
			queryErr = append(queryErr, jsonerror{"User does not have access to any compute resource."})
		}
		output = queryErr
	} else {
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		output = Out
	}
	jsonout, err := json.Marshal(output)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Fatal(err)
	}
	fmt.Fprintf(w, string(jsonout))
}

func getUserAllStorageQuotas(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	type jsonerror struct {
		Error string `json:"error"`
	}
	var inputErr []jsonerror

	user := q.Get("username")

	if user == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Error("No username specified in http query.")
		inputErr = append(inputErr, jsonerror{"No username specified."})
	}

	if len(inputErr) > 0 {
		jsonout, err := json.Marshal(inputErr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Fatal(err)
		}
		fmt.Fprintf(w, string(jsonout))
		return
	}

	rows, err := DBptr.Query(`select  name, path, value, unit, valid_until, user_exists from
							(select 1 as key, u.uname, sr.name, sr.type, sq.* from
								storage_quota as sq left join
								users as u on sq.uid = u.uid left join
								storage_resources as sr on sq.storageid = sr.storageid
								where u.uname = $1
							) as t 
							right join (
								select 1 as key, $1 in (select uname from users) as user_exists
							) as c on t.key = c.key;`, user)

	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Fatal(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"error\": \"Error in DB query.\" }")
		return
	}
	defer rows.Close()

	var userExists bool

	type jsonentry struct {
		Rname string `json:"resourcename"`
		Path  string `json:"path"`
		Value string `json:"value"`
		Unit  string `json:"unit"`
		Until string `json:"validuntil"`
	}
	var Entry jsonentry
	var Out []jsonentry

	for rows.Next() {
		var tmpRname, tmpPath, tmpValue, tmpUnit, tmpUntil sql.NullString
		rows.Scan(&tmpRname, &tmpPath, &tmpValue, &tmpUnit, &tmpUntil, &userExists)

		if tmpRname.Valid {
			Entry.Rname = tmpRname.String
			Entry.Path = tmpPath.String
			Entry.Value = tmpValue.String
			Entry.Unit  = tmpUnit.String
			Entry.Until  = tmpUntil.String
			Out = append(Out, Entry)
		}
	}

	var output interface{}
	if len(Out) == 0 {
		w.WriteHeader(http.StatusNotFound)
		var queryErr []jsonerror
		if !userExists {
			log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
			queryErr = append(queryErr, jsonerror{"User does not exist."})
		} else {
			log.WithFields(QueryFields(r, startTime)).Error("User does not have any assigned storage quotas.")
			queryErr = append(queryErr, jsonerror{"User does not have any assigned storage quotas."})
		}
		output = queryErr
	} else {
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		output = Out
	}
	jsonout, err := json.Marshal(output)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Fatal(err)
	}
	fmt.Fprintf(w, string(jsonout))
}

func setUserAccessToResource(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	uname := q.Get("username")
	gName := q.Get("groupname")
	rName := q.Get("resourcename")
	shell := q.Get("shell")
	homedir := q.Get("home_dir")
	if uname == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Error("No username specified in http query.")
		fmt.Fprintf(w,"{ \"error\": \"No value for username specified.\" }")
		return
	}
	if rName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Error("No compute resource specified in http query.")
		fmt.Fprintf(w,"{ \"error\": \"No value for resourcename specified.\" }")
		return
	}
	
	if gName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Error("No group name specified in http query.")
		fmt.Fprintf(w,"{ \"error\": \"No value for groupname specified.\" }")
		return
	}
	
	authorized, authout := authorize(r, AuthorizedDNs)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"error\": \""+authout+"not authorized.\" }")
		return
	}
	
	var defShell,defhome sql.NullString
	var grpid,compid,uid int
	
	// see if the user/group/resource combination is already there. If so, then we might just be doing an update.
	
	err := DBptr.QueryRow(`select uid, groupid, compid, shell, home_dir from compute_access as ca join groups on ca.groupid=groups.groupid join users as u on u.uid=ca.uid join compute_resources as cr on cr.compid=ca.compid where cr.name=$1 and users.uname=$2 and groups.name=$3`,rName,uname,gName).Scan(&uid,&grpid,&compid,&defShell,defhome)
	switch {
	case err == sql.ErrNoRows:
		
		// OK, we don't have this combo, so we do an insert now
		
		//grab the default home dir and shell paths for the given compid
		
		checkerr := DBptr.QueryRow(`select default_shell, default_home_dir from compute_resources as cr where cr.name=$1`,rName).Scan(&defShell,&defhome)
		if checkerr == sql.ErrNoRows {
			// the given compid does not exist in this case. Exit accordingly.	
			log.WithFields(QueryFields(r, startTime)).Error("resource " + rName + " does not exist.")
			w.WriteHeader(http.StatusNoContent)
			fmt.Fprintf(w, "{ \"error\": \"Resource does not exist.\" }")
			return	
		}
		//check if the query specified a shell or directory value
		if shell != "" {
			defShell.Valid = true
			defShell.String = shell
		}
		if homedir != "" {
			defhome.Valid = true
			defhome.String = homedir
		}

		// now, do the actual insert

		_, inserr := DBptr.Exec(`insert into compute_access (compid, uid, groupid, last_updated, shell, home_dir) values ( (select compid from crompute_resources where compid.name=$1), (select uid from users where uname=$2), (select groupid from groups where groups.name=$3), NOW(), $4,$5)`, rName, uname, gName, defShell, defhome)
		if inserr != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Error in DB insert: " + inserr.Error())
			// now we also need to do a bunch of other checks here
			if strings.Contains(inserr.Error(),"null value in column \"compid\"") {
				w.WriteHeader(http.StatusNoContent)
				fmt.Fprintf(w, "{ \"error\": \"Resource does not exist.\" }")
				return	
				
			} else if strings.Contains(inserr.Error(),"null value in column \"uid\"") {
				w.WriteHeader(http.StatusNoContent)
				fmt.Fprintf(w, "{ \"error\": \"User does not exist.\" }")
				return	
			} else if strings.Contains(inserr.Error(),"null value in column \"groupid\"") {
				w.WriteHeader(http.StatusNoContent)
				fmt.Fprintf(w, "{ \"error\": \"Group does not exist.\" }")
				return		
			} else {
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprintf(w, "{ \"error\": \"Error in DB insert.\" }")
				return		
			}
		} else {
			log.WithFields(QueryFields(r, startTime)).Info(fmt.Sprintf("Successfully inserted (%s,%s,%s,%s,%s) into compute_access.",rName, uname, gName, defShell, defhome))
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, "{ \"status\": \"success\" }")
			return			
		}
		
	case err != nil:
		log.WithFields(QueryFields(r, startTime)).Error("Error in DB query: " + err.Error()) 
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "{ \"error\": \"Error in DB query.\" }")
		return		
		
	default: // OK, we already have this user/group/resource combo. We just need to check if the shells are the same or whatnot
		
		if defShell.String == shell && defhome.String == homedir {
			// everything in the DB is already the same as the request, so don't do anything
			log.WithFields(QueryFields(r, startTime)).Print("The request already exists in the database. Nothing to do.")
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, "{ \"status\": \"success\" }")
			return	
		} else {
			_, moderr := DBptr.Exec(`update compute_access set shell=$1,home_dir=$2,last_updated=NOW() where groupid=$3 and uid=$4 and compid=$5`,defShell,defhome,grpid,uid,compid)
			if moderr != nil {
				log.WithFields(QueryFields(r, startTime)).Error("Error in DB update: " + err.Error()) 
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprintf(w, "{ \"error\": \"Error in DB update.\" }")
				return		
			} else {
				
				log.WithFields(QueryFields(r, startTime)).Info(fmt.Sprintf("Successfully updated (%s,%s,%s,%s,%s) in compute_access.",rName, uname, gName, defShell, defhome))
				w.WriteHeader(http.StatusOK)
				fmt.Fprintf(w, "{ \"status\": \"success\" }")
				return		
			}	
		}
	}
}
func getAllUsers(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	ao := strings.TrimSpace(q.Get("active"))
	activeonly := false

	if ao != "" {
		if activebool,err := strconv.ParseBool(ao) ; err == nil {
			activeonly = activebool
		} else {
			log.WithFields(QueryFields(r, startTime)).Error("Error in DB query: " + err.Error())
			fmt.Fprintf(w,"{ \"error\": \"Invalid value for active. Must be true or false (or omit it from the query).\" }")
			return
		}
	}
	querystr := `select uname, uid, full_name from users order by uname`
	if ao != "" {
		querystr = "select uname, uid, full_name from users where status='" + strconv.FormatBool(activeonly) + "' order by uname"
	}
	rows, err := DBptr.Query(querystr)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.WithFields(QueryFields(r, startTime)).Error("Error in DB query: " + err.Error())
		fmt.Fprintf(w,"{ \"error\": \"Error in DB query.\" }")
		return
	}
	defer rows.Close()
	
	type jsonout struct {
		Uname string `json:"username"`
		UID int `json:"uid"`
		Fullname string `json:"full_name"`
		
	} 
	var tmpout jsonout
	var Out []jsonout
	
	for rows.Next() {
		rows.Scan(&tmpout.Uname,&tmpout.UID,&tmpout.Fullname)
		Out = append(Out, tmpout)
	}

	var output interface{}	
	if len(Out) == 0 {
		type jsonerror struct {
			Error string `json:"error"`
		}
		var queryErr []jsonerror
		queryErr = append(queryErr, jsonerror{"Query returned no users."})
		log.WithFields(QueryFields(r, startTime)).Error("Query returned no users.")
		output = queryErr
	} else {
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		output = Out
	}
	jsonoutput, err := json.Marshal(output)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err.Error())
	}
	fmt.Fprintf(w, string(jsonoutput))
}
