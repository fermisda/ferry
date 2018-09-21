package main

import (
	"regexp"
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
	expt := q.Get("unitname")
	if uname == "" {
		uname = "%"
	}
	if expt == "" {
		expt = "%"
	}

	rows, err := DBptr.Query(`select uname, dn, user_exists, unit_exists from (
								select distinct 1 as key, uname, dn
								from affiliation_unit_user_certificate as ac
								join affiliation_units as au on ac.unitid = au.unitid
								join user_certificates as uc on ac.dnid = uc.dnid
								join users as u on uc.uid = u.uid 
								where u.uname like $1 and (ac.unitid in (select unitid from grid_fqan where fqan like $3) or '%' = $2)
								order by uname
							) as t right join (
								select 1 as key,
								$1 in (select uname from users) as user_exists,
								($2 in (select name from affiliation_units) or $2 = '%') as unit_exists
							) as c on t.key = c.key;`, uname, expt, "%" + expt + "%")
	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Error(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
		//		http.Error(w,"Error in DB query",404)
		return
	}

	defer rows.Close()

	var userExists, exptExists bool

	type jsonEntry struct {
		Uname string `json:"username"`
		DNs []string `json:"certificates"`
	}
	var Out []jsonEntry

	var tmpUname, tmpDN sql.NullString
	var tmpEntry jsonEntry
	for rows.Next() {
		rows.Scan(&tmpUname, &tmpDN, &userExists, &exptExists)
		if tmpDN.Valid {
			if tmpEntry.Uname == "" {
				tmpEntry = jsonEntry{tmpUname.String, make([]string, 0)}
			}
			if tmpUname.String != tmpEntry.Uname {
				Out = append(Out, tmpEntry)
				tmpEntry = jsonEntry{tmpUname.String, make([]string, 0)}
			}
			tmpEntry.DNs = append(tmpEntry.DNs, tmpDN.String)
		}
	}
	Out = append(Out, tmpEntry)

	var output interface{}	
	if !tmpDN.Valid {
		type jsonerror struct {
			Error []string `json:"ferry_error"`
		}
		var queryErr jsonerror
		if !userExists && uname != "%" {
			queryErr.Error = append(queryErr.Error, "User does not exist.")
			log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
		}
		if !exptExists && expt != "%" {
			queryErr.Error = append(queryErr.Error, "Experiment does not exist.")
			log.WithFields(QueryFields(r, startTime)).Error("Experiment does not exist.")
		}
		if userExists && exptExists {
			queryErr.Error = append(queryErr.Error, "User does not have any certificates registered.")
			log.WithFields(QueryFields(r, startTime)).Error("User does not have any certificates registered.")
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

func getAllUsersCertificateDNs(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	type jsonerror struct {
		Error string `json:"ferry_error"`
	}
	var inputErr []jsonerror

	expt := q.Get("unitname")
	if expt == "" {
		expt = "%"
	}
	ao := strings.TrimSpace(q.Get("active"))
	activeonly := false

	if ao != "" {
		if activebool,err := strconv.ParseBool(ao) ; err == nil {
			activeonly = activebool
		} else {
			log.WithFields(QueryFields(r, startTime)).Error("Error in DB query: " + err.Error())
			inputErr = append(inputErr, jsonerror{"Invalid value for active. Must be true or false (or omit it from the query)."})
		}
	}

	lastupdate, parserr := stringToParsedTime(strings.TrimSpace(q.Get("last_updated")))
	if parserr != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error parsing provided update time: " + parserr.Error())
		inputErr = append(inputErr, jsonerror{"Error parsing last_updated time. Check ferry logs. If provided, it should be an integer representing an epoch time."})
	}
	
	if len(inputErr) > 0 {
		jsonout, err := json.Marshal(inputErr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error(err)
		}
		fmt.Fprintf(w, string(jsonout))
		return
	}
	
	rows, err := DBptr.Query(`select uname, name, dn, unit_exists from (
								select 1 as key, uname, name, uc.dn from affiliation_unit_user_certificate as ac
								left join user_certificates as uc on ac.dnid = uc.dnid
								left join users as u on uc.uid = u.uid
								left join affiliation_units as au on ac.unitid = au.unitid
								where name like $1 and (status = $2 or not $2) and (ac.last_updated>=$3 or $3 is null) order by uname
							) as t right join (
								select 1 as key,
								$1 in (select name from affiliation_units) as unit_exists
							) as c on t.key = c.key;`, expt, activeonly, lastupdate)
	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Error(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
		return
	}
	defer rows.Close()

	var exptExists bool
	type jsoncert struct {
		UnitName string `json:"unit_name"`
		DN       string `json:"dn"`
	}
	type jsonuser struct {
		Uname string `json:"username"`
		Certs []jsoncert `json:"certificates"`
	}
	var Out []jsonuser

	prevUname := ""
	for rows.Next() {
		var tmpUname, tmpUnitName, tmpDN sql.NullString
		rows.Scan(&tmpUname, &tmpUnitName, &tmpDN, &exptExists)
		if tmpUname.Valid {
			if prevUname != tmpUname.String {
				Out = append(Out, jsonuser{tmpUname.String, make([]jsoncert, 0)})
				prevUname = tmpUname.String
			}
			Out[len(Out)-1].Certs = append(Out[len(Out)-1].Certs, jsoncert{tmpUnitName.String, tmpDN.String})
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
	expt := q.Get("unitname")
	if uname == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No username specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No username specified.\" }")
		return
	}
	if expt == "" {
		expt = "%"
	}

	lastupdate, parserr := stringToParsedTime(strings.TrimSpace(q.Get("last_updated")))
	if parserr != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error parsing provided update time: " + parserr.Error())
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error parsing last_updated time. Check ferry logs. If provided, it should be an integer representing an epoch time.\"}")
		return
	}

	rows, err := DBptr.Query(`select name, fqan, user_exists, unit_exists from (
								select 1 as key, name, fqan, ga.last_updated from
								grid_access as ga right join
								(select * from users where uname = $1) as us on ga.uid = us.uid	left join
								grid_fqan as gf on ga.fqanid = gf.fqanid join
								(select * from affiliation_units where name like $2) as au on gf.unitid = au.unitid
							) as T
							right join (
								select 1 as key,
								$1 in (select uname from users) as user_exists,
								$2 in (select name from affiliation_units) as unit_exists
							) as C on T.key = C.key where T.last_updated >= $3 or $3 is null order by T.name;`, uname, expt, lastupdate)
	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Error(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
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
				log.WithFields(QueryFields(r, startTime)).Error(jsonerr)
			}
			output += string(outline)
			idx++
		}
	}
	if idx == 0 {
		if !userExists {
			output += `"ferry_error": "User does not exist.",`
			log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
		}
		if !exptExists {
			output += `"ferry_error": "Experiment does not exist.",`
			log.WithFields(QueryFields(r, startTime)).Error("Experiment does not exist.")
		}
		output += `"ferry_error": "User do not have any assigned FQANs."`
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
	expt := q.Get("unitname")
	if expt == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No unitname specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No unitname specified.\" }")
		return
	}

	rows, err := DBptr.Query(`select t1.uname, c.unit_exists from 
							 (select distinct 1 as key, us.uname from users as us right join grid_access as ga on us.uid=ga.uid
							  left join grid_fqan as gf on ga.fqanid = gf.fqanid
							  left join affiliation_units as au on gf.unitid = au.unitid
							  where ga.is_superuser=true and au.name=$1) as t1
							  right join (select 1 as key, $1 in (select name from affiliation_units) as unit_exists) as c on c.key = t1.key`, expt)
	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Error(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
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
				log.WithFields(QueryFields(r, startTime)).Error(jsonerr)
			}
			output += string(outline)
			idx++
		}
	}
	if idx == 0 {
		if !exptExists {
			output += `"ferry_error": "Experiment does not exist.",`
			log.WithFields(QueryFields(r, startTime)).Error("Experiment does not exist.")
		}
		output += `"ferry_error": "No super users found."`
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
	authorized, authout := authorize(r)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"ferry_error\": \""+authout+"not authorized.\" }")
		return
	}

	q := r.URL.Query()
	uName := strings.TrimSpace(q.Get("username"))
	unitName := strings.TrimSpace(q.Get("unitname"))
	if uName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No user name specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No username specified.\" }")
		return
	}
	if unitName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No unit name specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No unitname specified.\" }")
		return
	}

	var uid,unitid sql.NullInt64
 
	DBtx, cKey, err := LoadTransaction(r, DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"ferry_error\": \"Unable to start database transaction.\" }")
		return
	}
	defer DBtx.Rollback(cKey)

	queryerr := DBtx.tx.QueryRow(`select uid, unitid from (select 1 as key, uid from users where uname=$1) as us full outer join (select 1 as key, unitid from affiliation_units au where au.name=$2) as aut on us.key=aut.key`, uName, unitName).Scan(&uid,&unitid)
	
	if queryerr == sql.ErrNoRows {
		log.WithFields(QueryFields(r, startTime)).Error("User and unit names do not exist.")
		if cKey != 0 {
			fmt.Fprintf(w, "{ \"ferry_error\": \"User and unit names do not exist.\" }")
		} else {
			DBtx.Report("User and unit names do not exist.")
		}
		return
	} else if queryerr != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error: " + queryerr.Error())
		if cKey !=0 {
			fmt.Fprintf(w, "{ \"ferry_error\": \"Something went wrong.\" }")
		} 
		return
	}
	if ! uid.Valid {
		log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
		if cKey != 0 {
			fmt.Fprintf(w, "{ \"ferry_error\": \"User does not exist.\" }")
		} else {
			DBtx.Report("User does not exist.")	
		}
		return
	} 
	if ! unitid.Valid {
		log.WithFields(QueryFields(r, startTime)).Error("Unit does not exist.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"Unit does not exist.\" }")
		return
	}

	_, err = DBtx.Exec(`update grid_access set is_superuser = true, last_updated = NOW() 
                            where uid = $1 and fqanid in (select fqanid from grid_fqan  
                            where unitid = $2)`, uid.Int64, unitid.Int64)

	if err == nil {
		if cKey != 0 { DBtx.Commit(cKey) }
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		fmt.Fprintf(w, "{ \"ferry_status\": \"success\" }")
	} else {
		log.WithFields(QueryFields(r, startTime)).Error("Error: " + err.Error())
		fmt.Fprintf(w, "{ \"ferry_error\": \"Something went wrong.\" }")
	}
}

func getUserGroups(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	uname := q.Get("username")
	if uname == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No username specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No username specified.\" }")
		return
	}
	pingerr := DBptr.Ping()
	if pingerr != nil {
		log.WithFields(QueryFields(r, startTime)).Error(pingerr)
	}
	
	lastupdate, parserr :=  stringToParsedTime(strings.TrimSpace(q.Get("last_updated")))
	if parserr != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error parsing provided update time: " + parserr.Error())
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error parsing last_updated time. Check ferry logs. If provided, it should be an integer representing an epoch time.\"}")
		return
	}

	rows, err := DBptr.Query(`select groups.gid, groups.name, groups.type from groups INNER JOIN user_group on (groups.groupid = user_group.groupid) INNER JOIN users on (user_group.uid = users.uid) where users.uname=$1 and (user_group.last_updated>=$2 or $2 is null)`, uname, lastupdate)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "Error in DB query\n")
	} else {
		defer rows.Close()

		idx := 0

		type jsonout struct {
			Gid       int    `json:"gid"`
			Groupname string `json:"groupname"`
			Grouptype string `json:"grouptype"`
		}

		var Out jsonout

		for rows.Next() {
			if idx == 0 {
				fmt.Fprintf(w, "[ ")
			} else {
				fmt.Fprintf(w, ",")
			}
			rows.Scan(&Out.Gid, &Out.Groupname, &Out.Grouptype)
			outline, jsonerr := json.Marshal(Out)
			if jsonerr != nil {
				log.WithFields(QueryFields(r, startTime)).Error(jsonerr)
			}
			fmt.Fprintf(w, string(outline))
			idx += 1
		}
		if idx == 0 {
			log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
			fmt.Fprintf(w, `{ "ferry_error": "User does not exist." }`)
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
		log.WithFields(QueryFields(r, startTime)).Error("No username specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No username specified.\" }")
		return
	}
	pingerr := DBptr.Ping()
	if pingerr != nil {
		log.WithFields(QueryFields(r, startTime)).Error(pingerr)
	}
	rows, err := DBptr.Query(`select full_name, uid, status, expiration_date from users where uname=$1`, uname)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
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
				log.WithFields(QueryFields(r, startTime)).Error(jsonerr)
			}
			fmt.Fprintf(w, string(outline))
			idx += 1
		}
		if idx == 0 {
			log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
			fmt.Fprintf(w, `{ "ferry_error": "User does not exist." }`)
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

	uName := strings.TrimSpace(q.Get("username"))
	gName := strings.TrimSpace(q.Get("groupname"))
	gType := strings.TrimSpace(q.Get("grouptype"))
	isLeader := strings.TrimSpace(q.Get("is_leader"))

	if uName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No username specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No username specified.\" }")
		return
	}
	if gName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No groupname specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No groupname specified.\" }")
		return
	}
	if gType == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No grouptype specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No grouptype specified.\" }")
		return
	}
	if isLeader == "" {
		isLeader = "false"
	} else {
		_, err := strconv.ParseBool(q.Get("is_leader"))
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Invalid is_leader specified in http query.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"Invalid is_leader specified.\" }")
			return
		}
	}

	authorized, authout := authorize(r)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"ferry_error\": \""+authout+"not authorized.\" }")
		return
	}

	DBtx, cKey, err := LoadTransaction(r, DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
	}
	defer DBtx.Rollback(cKey)

//	_, err = DBtx.Exec(fmt.Sprintf(`do $$
//										declare uid int;
//										declare groupid int;
//									begin
//										select u.uid into uid from users as u where uname = '%s';
//										select g.groupid into groupid from groups as g where name = '%s' and type = '%s';
//										
//										insert into user_group (uid, groupid, is_leader, last_updated)
//														values (uid, groupid, %s, NOW());
//									end $$;`, uName, gName, gType, isLeader))
//

	_, err = DBtx.Exec(`insert into user_group (uid, groupid, is_leader, last_updated) values
                            ((select uid from users where uname=$1),
                             (select groupid from groups where name=$2 and type=$3),
                             $4, NOW())`,uName, gName, gType, isLeader)
	if err == nil {
		if cKey != 0 {
			log.WithFields(QueryFields(r, startTime)).Info("Success!")
			fmt.Fprintf(w, "{ \"ferry_status\": \"success\" }")
		}
	} else {
		if strings.Contains(err.Error(), `duplicate key value violates unique constraint`) {
			log.WithFields(QueryFields(r, startTime)).Error("User already belongs to this group.")
			if cKey != 0 {
				fmt.Fprintf(w, "{ \"ferry_error\": \"User already belongs to this group.\" }")
			}
		} else if strings.Contains(err.Error(), `null value in column "uid" violates not-null constraint`) {
			log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
			if cKey != 0 {
				fmt.Fprintf(w, "{ \"ferry_error\": \"User does not exist.\" }")
			}
		} else if strings.Contains(err.Error(), `null value in column "groupid" violates not-null constraint`) {
			log.WithFields(QueryFields(r, startTime)).Error("Group does not exist.")
			if cKey != 0 {
				fmt.Fprintf(w, "{ \"ferry_error\": \"Group does not exist.\" }")
			}
		} else if strings.Contains(err.Error(), `invalid input value for enum`) {
			log.WithFields(QueryFields(r, startTime)).Error("Invalid group type.")
			if cKey != 0 {
				fmt.Fprintf(w, "{ \"ferry_error\": \"Invalid group type.\" }")
			}
		} else {
			log.WithFields(QueryFields(r, startTime)).Error(err.Error())
			if cKey != 0 {
				fmt.Fprintf(w, "{ \"ferry_error\": \"Something went wrong.\" }")
			}
		}
	}
	if cKey != 0 {
		DBtx.Commit(cKey)
	}
}

func removeUserFromGroup(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	type jsonerror struct {
		Error []string `json:"ferry_error"`
	}
	var inputErr jsonerror

	user := strings.TrimSpace(q.Get("username"))
	group := strings.TrimSpace(q.Get("groupname"))
	gtype := strings.TrimSpace(q.Get("grouptype"))

	if user == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No username specified in http query.")
		inputErr.Error = append(inputErr.Error, "No username specified.")
	}
	if group == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No groupname specified in http query.")
		inputErr.Error = append(inputErr.Error, "No groupname specified.")
	}
	if gtype == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No grouptype specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No grouptype specified.\" }")
		return
	}

	if len(inputErr.Error) > 0 {
		jsonout, err := json.Marshal(inputErr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error(err)
		}
		fmt.Fprintf(w, string(jsonout))
		return
	}

	//require auth	
	authorized,authout := authorize(r)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w,"{ \"ferry_error\": \"" + authout + "not authorized.\" }")
		return
	}

	DBtx, cKey, err := LoadTransaction(r, DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
	}
	defer DBtx.Rollback(cKey)

	var uid, groupid sql.NullInt64

	queryerr := DBtx.tx.QueryRow(`select uid, groupid from (select 1 as key, uid from users where uname=$1) as us full outer join (select 1 as key, groupid, type from groups where name = $2 and type = $3) as g on us.key=g.key`,user, group, gtype).Scan(&uid,&groupid)
	if queryerr == sql.ErrNoRows {
		log.WithFields(QueryFields(r, startTime)).Error("User and group names do not exist.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"User and group names do not exist.\" }")
		return	
	} else if queryerr != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error: " + queryerr.Error())
		fmt.Fprintf(w, "{ \"ferry_error\": \"Something went wrong.\" }")
		return
	}
	if ! uid.Valid {
		log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"User does not exist.\" }")
		return
	} 
	if ! groupid.Valid {
		log.WithFields(QueryFields(r, startTime)).Error("Group does not exist.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"Group does not exist.\" }")
		return
	}

	query := fmt.Sprintf(`do $$
						  declare

							vUid constant int := %d;
							vGroupid constant int := %d;
							vError text;
						  begin

							if vUid is null then vError = concat(vError, 'noUser,'); end if;
							if vGroupid is null then vError = concat(vError, 'noGroup,'); end if;
							if (vUid, vGroupid) not in (select uid, groupid from user_group) then vError = concat(vError, 'user_group,'); end if;
							vError = trim(both ',' from vError);

							if vError is not null then raise '%%', vError; end if;
							
							delete from user_group where uid = vUid and groupid = vGroupid;
						  end $$;`, uid.Int64, groupid.Int64)
	_, err = DBtx.Exec(query)

	re := regexp.MustCompile(`[\s\t\n]+`)
	log.Debug(re.ReplaceAllString(query, " "))

	var output interface{}
	if err != nil {
		var queryErr jsonerror
		if strings.Contains(err.Error(), `noUser`) {
			log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
			queryErr.Error = append(queryErr.Error, "User does not exist.")
		}
		if strings.Contains(err.Error(), `noGroup`) {
			log.WithFields(QueryFields(r, startTime)).Error("Group does not exist.")
			queryErr.Error = append(queryErr.Error, "Group does not exist.")
		}
		if strings.Contains(err.Error(), `user_group`) {
			log.WithFields(QueryFields(r, startTime)).Error("User does not belong to this group.")
			queryErr.Error = append(queryErr.Error, "User does not belong to this group.")
		}
		if strings.Contains(err.Error(), `invalid input value for enum`) {
			log.WithFields(QueryFields(r, startTime)).Error("Invalid group type.")
			queryErr.Error = append(queryErr.Error, "Invalid group type.")
		}
		if len(queryErr.Error) == 0 {
			log.WithFields(QueryFields(r, startTime)).Error(err.Error())
			queryErr.Error = append(queryErr.Error, "Something went wrong.")
		}
		output = queryErr
	} else {
		type jsonstatus struct {
			Error string `json:"ferry_status"`
		}
		output = jsonstatus{"success"}
		log.WithFields(QueryFields(r, startTime)).Info("Success!")

		DBtx.Commit(cKey)
		if cKey == 0 {
			return
		}
	}
	jsonout, err := json.Marshal(output)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
	}
	fmt.Fprintf(w, string(jsonout))
}

func setUserExperimentFQAN(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	uName := strings.TrimSpace(q.Get("username"))
	fqan := strings.TrimSpace(q.Get("fqan"))
	unitName := strings.TrimSpace(q.Get("unitname"))

	if uName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No username specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No username specified.\" }")
		return
	}
	if fqan == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No fqan specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No fqan specified.\" }")
		return
	}
	if unitName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No unitname specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No unitname specified.\" }")
		return
	}

	authorized, authout := authorize(r)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"ferry_error\": \""+authout+"not authorized.\" }")
		return
	}

	DBtx, cKey, err := LoadTransaction(r, DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
	}
	defer DBtx.Rollback(cKey)

	var uid, unitid, fqanid int
	queryerr := DBtx.QueryRow(`select uid from users where uname=$1 for update`, uName).Scan(&uid)
	switch {
	case queryerr == sql.ErrNoRows:
		log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
		if cKey != 0 {
			fmt.Fprintf(w,"{ \"ferry_error\": \"User does not exist.\" }")
		}
		return
	case queryerr != nil:
		log.WithFields(QueryFields(r, startTime)).Error("Error during query:" + queryerr.Error())
		if cKey != 0 {
			fmt.Fprintf(w,"{ \"ferry_error\": \"Error during DB query; check logs.\" }")
		}
		return	
	}

	queryerr = DBtx.QueryRow(`select unitid from affiliation_units where name=$1 for update`, unitName).Scan(&unitid)
	switch {
	case queryerr == sql.ErrNoRows:
		log.WithFields(QueryFields(r, startTime)).Error("Affiliation unit does not exist.")
		if cKey != 0 {
			fmt.Fprintf(w,"{ \"ferry_error\": \"Affiliation unit does not exist.\" }")
		}
		return
	case queryerr != nil:
		log.WithFields(QueryFields(r, startTime)).Error("Error during query:" + queryerr.Error())
		if cKey != 0 {
			fmt.Fprintf(w,"{ \"ferry_error\": \"Error during DB query; check logs.\" }")
		}
		return	
	}
	
	queryerr = DBtx.QueryRow(`select fqanid from grid_fqan as gf join affiliation_units as au on gf.unitid=au.unitid where au.name=$1 and gf.fqan=$2`,unitName, fqan).Scan(&fqanid)
	switch {
	case queryerr == sql.ErrNoRows:
		log.WithFields(QueryFields(r, startTime)).Error("FQAN " + fqan + " not assigned to affiliation unit " + unitName + ".")
		if cKey != 0 {
			fmt.Fprintf(w,"{ \"ferry_error\": \"FQAN not assigned to specified unit.\" }")
		} else {
			DBtx.Report("FQAN not assigned to specified unit")	
		}
		return
	case queryerr != nil:
		log.WithFields(QueryFields(r, startTime)).Error("Error during query:" + queryerr.Error())
		if cKey != 0 {
			fmt.Fprintf(w,"{ \"ferry_error\": \"Error during DB query; check logs.\" }")
		}
		return
	}

	var hasCert bool
	queryerr = DBtx.QueryRow(`select count(*) > 0 from affiliation_unit_user_certificate as ac
							   join user_certificates as uc on ac.dnid = uc.dnid
							   where uid = $1 and unitid = $2`, uid, unitid).Scan(&hasCert)
	switch {
	case queryerr == nil:
		if !hasCert {
			log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
			if cKey != 0 {
				fmt.Fprintf(w,"{ \"ferry_error\": \"User does not exist.\" }")
			}
			return
		}
	default:
		log.WithFields(QueryFields(r, startTime)).Error("Error during query:" + queryerr.Error())
		if cKey != 0 {
			fmt.Fprintf(w,"{ \"ferry_error\": \"Error during DB query; check logs.\" }")
		}
		return	
	}

	_, err = DBtx.Exec(`insert into grid_access (uid, fqanid, is_superuser, is_banned, last_updated) values ($1, $2, false, false, NOW())`, uid, fqanid)
	if err == nil {
		if cKey != 0 {
			log.WithFields(QueryFields(r, startTime)).Info("Success!")
			fmt.Fprintf(w, "{ \"ferry_status\": \"success\" }")
		}
	} else {
		if strings.Contains(err.Error(), `null value in column "uid" violates not-null constraint`) {
			log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
			if cKey != 0 {
				fmt.Fprintf(w, "{ \"ferry_error\": \"User does not exist.\" }")
			}
		} else if strings.Contains(err.Error(), `null value in column "fqanid" violates not-null constraint`) {
			log.WithFields(QueryFields(r, startTime)).Error("FQAN does not exist.")
			if cKey != 0 {
				fmt.Fprintf(w, "{ \"ferry_error\": \"FQAN does not exist.\" }")
			} else {
				DBtx.Report("FQAN does not exist.")
			}
		} else if strings.Contains(err.Error(), `duplicate key value violates unique constraint`) {
			if cKey != 0 {
				log.WithFields(QueryFields(r, startTime)).Error("This association already exists.")
				fmt.Fprintf(w, "{ \"ferry_error\": \"This association already exists.\" }")
			}
		} else {
			log.WithFields(QueryFields(r, startTime)).Error(err.Error())
			if cKey != 0 {
				fmt.Fprintf(w, "{ \"ferry_error\": \"Something went wrong.\" }")
			}
		}
		return
	}
	
	DBtx.Commit(cKey)
}

func setUserShellAndHomeDir(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	rName := strings.TrimSpace(q.Get("resourcename"))
	uName := strings.TrimSpace(q.Get("username"))
	shell := strings.TrimSpace(q.Get("shell"))
	hDir  := strings.TrimSpace(q.Get("homedir"))

	if rName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No resourcename specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No resourcename specified.\" }")
		return
	}
	if uName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No username specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No username specified.\" }")
		return
	}
	if shell == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No shell specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No shell specified.\" }")
		return
	}
	if hDir == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No homedir specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No homedir specified.\" }")
		return
	}

	authorized, authout := authorize(r)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"ferry_error\": \""+authout+"not authorized.\" }")
		return
	}

	DBtx, cKey, err := LoadTransaction(r, DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
	}
	defer DBtx.Rollback(cKey)

	// check whether the user and resource actually exist before doing anything
	var cauid,cacompid sql.NullInt64
	queryerr := DBtx.QueryRow(`select (select uid from users where uname=$1), (select compid from compute_resources where name=$2)`, uName, rName).Scan(&cauid, &cacompid)
	if queryerr != nil && queryerr != sql.ErrNoRows {
		log.WithFields(QueryFields(r, startTime)).Error("Error verifying user and resource status: " + queryerr.Error() + ". Will not proceed.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query. Check log.\" }")
		return
	}
	if !cauid.Valid {
		log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"User does not exist.\" }")	
		return
	}
	if !cacompid.Valid {
		log.WithFields(QueryFields(r, startTime)).Error("Resource does not exist.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"Resource does not exist.\" }")	
		return
	}

	res, err := DBtx.Exec(`update compute_access set shell = $1, home_dir = $2, last_updated = NOW() where compid = $3 and uid = $4`, shell, hDir, cacompid, cauid)

	if err == nil {
		//check whether any rows were modified. If no rows were modified, the user did not have access to this compute resource. Print such a message.
		aRows, _ := res.RowsAffected()
		if aRows == 0 {
			log.WithFields(QueryFields(r, startTime)).Info("User " + uName + " does not have access to resource " + rName + ".")
                        fmt.Fprintf(w, "{ \"ferry_error\": \"User does not have access to this resource.\" }")
		} else {
			log.WithFields(QueryFields(r, startTime)).Info("Success!")
			fmt.Fprintf(w, "{ \"ferry_status\": \"success\" }")
		}
	} else {
		if strings.Contains(err.Error(), `User does not exist.`) {
			log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"User does not exist.\" }")
		} else if strings.Contains(err.Error(), `Resource does not exist.`) {
			log.WithFields(QueryFields(r, startTime)).Error("Resource does not exist.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"Resource does not exist.\" }")
		} else {
			log.WithFields(QueryFields(r, startTime)).Error(err.Error())
			fmt.Fprintf(w, "{ \"ferry_error\": \"Something went wrong.\" }")
		}
	}

	if cKey != 0 {	DBtx.Commit(cKey) }
}

func setUserShell(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	aName := strings.TrimSpace(q.Get("unitname"))
	uName := strings.TrimSpace(q.Get("username"))
	shell := strings.TrimSpace(q.Get("shell"))

	if aName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No unitname specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No unitname specified.\" }")
		return
	}
	if uName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No username specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No username specified.\" }")
		return
	}
	if shell == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No shell specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No shell specified.\" }")
		return
	}

	authorized, authout := authorize(r)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"ferry_error\": \""+authout+"not authorized.\" }")
		return
	}

	
	var uid, unitid sql.NullInt64

	queryerr := DBptr.QueryRow(`select uid, unitid from (select 1 as key, uid from users where uname = $1) as u full outer join (select 1 as key, unitid from affiliation_units au where au.name = $2 ) as aut on u.key=aut.key`, uName, aName).Scan(&uid,&unitid)
	if queryerr == sql.ErrNoRows {
		log.WithFields(QueryFields(r, startTime)).Error("User and unit do not exist.")	
		fmt.Fprintf(w, "{ \"ferry_error\": \"User and unit do not exist.\" }")
		return
	}
	if !uid.Valid {
		log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"User does not exist.\" }")
	}
	if !unitid.Valid {
		log.WithFields(QueryFields(r, startTime)).Error("Unit does not exist.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"Unit does not exist.\" }")
	}

	DBtx, cKey, err := LoadTransaction(r, DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
	}
	defer DBtx.Rollback(cKey)

	res, err := DBtx.Exec(`update compute_access set shell = $1, last_updated = NOW()
			    where uid = $2 and compid in (select compid from compute_resources where unitid = $3)`, shell, uid, unitid)
	if err == nil {
		aRows, _ := res.RowsAffected()
		if aRows == 0 {
			log.WithFields(QueryFields(r, startTime)).Info("User " + uName + " does not have access to resources owned by " + aName + ".")
                        fmt.Fprintf(w, "{ \"ferry_error\": \"User does not have access to this resource.\" }")
		} else {
			log.WithFields(QueryFields(r, startTime)).Info("Success!")
			fmt.Fprintf(w, "{ \"ferry_status\": \"success\" }")
		}
	} else {	
		log.WithFields(QueryFields(r, startTime)).Error(err.Error())
		fmt.Fprintf(w, "{ \"ferry_error\": \"Something went wrong.\" }")
	}
	
	if cKey != 0 { DBtx.Commit(cKey) }
}

func getUserShellAndHomeDir(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	comp := q.Get("resourcename")
	user := q.Get("username")

	if comp == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No resourcename specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No resourcename specified.\" }")
		return
	}
	if user == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No username specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No username specified.\" }")
		return
	}

	lastupdate, parserr := stringToParsedTime(strings.TrimSpace(q.Get("last_updated")))
	if parserr != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error parsing provided update time: " + parserr.Error())
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error parsing last_updated time. Check ferry logs. If provided, it should be an integer representing an epoch time.\"}")
		return
	}

	rows, err := DBptr.Query(`select t1.shell, t1.home_dir, c.resource_exists, c.user_exists from
							 (select 1 as key, ca.shell, ca.home_dir from compute_access as ca
							  left join compute_resources as cr on ca.compid = cr.compid
							  left join users as us on ca.uid = us.uid 
							  where cr.name=$1 and us.uname=$2 and (ca.last_updated>=$3 or $3 is null)) as t1
							  right join (select 1 as key, $1 in (select name from compute_resources) as resource_exists,
														   $2 in (select uname from users) as user_exists)
							  as c on c.key = t1.key`, comp, user, lastupdate)
	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Error(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
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
				log.WithFields(QueryFields(r, startTime)).Error(jsonerr)
			}
			output += string(outline)
			idx++
		}
	}
	if idx == 0 {
		if !compExists {
			output += `{"ferry_error": "Resource does not exist."},`
			log.WithFields(QueryFields(r, startTime)).Error("Resource does not exist.")
		}
		if !userExists {
			output += `{"ferry_error": "User does not exist."},`
			log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
		}
		output += `{"ferry_error": "User doesn't have access to resource."}`
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
	rName := strings.TrimSpace(strings.ToLower(q.Get("resourcename")))
	uName := strings.TrimSpace(q.Get("username"))
	unitName := strings.TrimSpace(q.Get("unitname"))

	if rName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No resource name specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No resourcename specified.\" }")
		return

	}
	if uName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No user name specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No username specified.\" }")
		return
	}
	if unitName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No unit name specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No unitname specified.\" }")
		return
	}

	rows, err := DBptr.Query(`select sq.path,sq.value, sq.unit, sq.valid_until from storage_quota sq INNER JOIN affiliation_units on affiliation_units.unitid = sq.unitid INNER JOIN storage_resources on storage_resources.storageid = sq.storageid INNER JOIN users on users.uid = sq.uid where affiliation_units.name=$1 AND storage_resources.type=$2 and users.uname=$3`, unitName, rName, uName)
	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Error(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")

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
		var tmpPath, tmpUnit, tmpValue, tmpValid sql.NullString
		rows.Scan(&tmpPath, &tmpValue, &tmpUnit, &tmpValid)
		if tmpValue.Valid {
			Out.Path, Out.Value, Out.Unit, Out.ValidUntil = tmpPath.String, tmpValue.String, tmpUnit.String, tmpValid.String
			outline, jsonerr := json.Marshal(Out)
			if jsonerr != nil {
				log.WithFields(QueryFields(r, startTime)).Error(jsonerr)
			}
			output += string(outline)
			idx++
		}
	}
	if idx == 0 {
		output += `{"ferry_error": "User has no quotas registered."}`
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
	authorized, authout := authorize(r)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"ferry_error\": \""+authout+"not authorized.\" }")
		return
	}

	q := r.URL.Query()
	quota := strings.TrimSpace(q.Get("quota"))
	uName := strings.TrimSpace(q.Get("username"))
	unitName := strings.TrimSpace(q.Get("unitname"))
	unit := strings.TrimSpace(q.Get("quota_unit"))
	rName := strings.TrimSpace(strings.ToUpper(q.Get("resourcename")))
	isgrp := strings.TrimSpace( strings.ToLower(q.Get("isGroup")))
	validtime := strings.TrimSpace(q.Get("valid_until"))
	path := strings.TrimSpace(q.Get("path"))

	var isGroup bool
	var spath sql.NullString

	if isgrp == "" {
		isGroup = false
	} else {
		ig, parserr := strconv.ParseBool(isgrp)
		if parserr != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Invalid isGroup specified in call.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"Invalid isGroup value specified.\" }")
			return
		}
		isGroup = ig
	}
	if quota == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No quota value specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No quota specified.\" }")
		return
	}

	var vUntil sql.NullString
	if validtime != "" && strings.ToUpper(validtime) != "NULL" {
		vUntil.Valid = true
		vUntil.String = validtime
	}
	
	if uName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No user name given.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No username provided.\" }")
		return
	}
	if rName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No resource name given.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No resourcename provided.\" }")
		return
	}
	if unitName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No affiliation unit given.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No unitname provided.\" }")
		return
	}
	if unit == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No unit given.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No unit provided.\" }")
		return
	}

	// We want to store the value in the DB in bytes, no matter what the input unit is. Convert the value here and then set the unit of "B" for bytes	
	newquota, converr := convertValue(quota, unit, "B")
	if converr != nil {
		log.WithFields(QueryFields(r, startTime)).Error(converr.Error())
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error converting unit value. It must be a number.\" }")
		return	
	}
	// set the quota value to be stored to newquota, which is now in bytes
	quota = strconv.FormatFloat(newquota, 'f', 0, 64)
	unit = "B"
	
	if path == "" || strings.ToUpper(path) == "NULL" {
		spath.Valid = false
		spath.String = ""
	} else {
		spath.Valid = true
		spath.String = path
	}
	
	DBtx, cKey, err := LoadTransaction(r, DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
	}
	defer DBtx.Rollback(cKey)
	
	
	
	var vSid,vId,vUnitid sql.NullInt64

	//
	//querystr := 
	//queryerr := DBtx.QueryRow(querystr,
	//
	
	// get storageID, unitid, uid,
	querystr := ""
	if isGroup {
		querystr = `select (select storageid from storage_resources where name=$1), (select groupid as id from groups where name=$2), (select unitid from affiliation_units where name=$3)`
	} else {
		querystr = `select (select storageid from storage_resources where name=$1), (select uid as id from users where uname=$2), (select unitid from affiliation_units where name=$3)`
	}
	queryerr := DBtx.QueryRow(querystr,rName, uName, unitName).Scan(&vSid, &vId, &vUnitid)
	if queryerr == sql.ErrNoRows {
		log.WithFields(QueryFields(r, startTime)).Error("Unit does not exist.")
		if cKey != 0 { 
			fmt.Fprintf(w, "{ \"ferry_error\": \"Unit does not exist.\" }")	
		}
		DBtx.Report("Unit does not exist.")
		return
	} else if queryerr != nil {
		log.WithFields(QueryFields(r, startTime)).Error("DB error: " + queryerr.Error())
		if cKey != 0 { 
			fmt.Fprintf(w, "{ \"ferry_error\": \"DB error; check log.\" }")	
		}
		DBtx.Report("DB error; check log.")
		return
	}
	if ! vId.Valid {
		if isGroup {
			log.WithFields(QueryFields(r, startTime)).Error("Group does not exist.")
			if cKey !=0 {
				fmt.Fprintf(w, "{ \"ferry_error\": \"Group does not exist.\" }")
			} else{
				DBtx.Report("Group does not exist.")	
			}
		} else {
			log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
			if cKey != 0 {
				fmt.Fprintf(w, "{ \"ferry_error\": \"User does not exist.\" }")	
			} else {
				DBtx.Report("User does not exist.")	
			}
		}
		return
	} 
	if ! vSid.Valid {
		log.WithFields(QueryFields(r, startTime)).Error("Resource does not exist.")
		if cKey != 0 {
			fmt.Fprintf(w, "{ \"ferry_error\": \"Resource does not exist.\" }")
		} else {
			DBtx.Report("Resource does not exist.")	
		}
		return
	} 
	
	var(
		column string
		quotaid sql.NullInt64
		vPath sql.NullString
	)
	
	if isGroup {
		column = `groupid`
	} else { 
		column = `uid` 
	}
	
	//	queryerr = DBtx.tx.QueryRow(`select quotaid,path from storage_quota where storageid = $1 and ` + column + ` = $2 and unitid = $3 and ((valid_until is NULL and not $4) or (valid_until is not null and (valid_until = $5 and $4)))`, vSid, vId, vUnitid, vUntil.Valid, vUntil).Scan(&quotaid,&vPath)
	queryerr = DBtx.tx.QueryRow(`select quotaid,path from storage_quota where storageid = $1 and ` + column + ` = $2 and unitid = $3 and ((valid_until is NULL and not $4) or (valid_until is not null and $4))`, vSid, vId, vUnitid, vUntil.Valid).Scan(&quotaid,&vPath)
	// if we did not specify a path in the API call, stick with the existing one
	if vPath.Valid && ! spath.Valid && strings.ToUpper(path) != "NULL" {
		spath.Valid = true
		spath.String = vPath.String
	}
	if queryerr == sql.ErrNoRows {
		if vUntil.Valid {
			_, err = DBtx.Exec(`insert into storage_quota (storageid, ` + column + `, unitid, value, unit, valid_until, path, last_updated) values ($1, $2, $3, $4, $5, $6, $7, NOW())`,vSid, vId, vUnitid, quota, unit, vUntil.String, spath.String)
		} else {
			_, err = DBtx.Exec(`insert into storage_quota (storageid, ` + column + `, unitid, value, unit, path, last_updated) values ($1, $2, $3, $4, $5, $6, NOW())`,vSid, vId, vUnitid, quota, unit, spath.String)	
			
		}
	} else if queryerr != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error checking for existing quota: " + queryerr.Error())
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error checking for existing quota.\" }")
		return
	} else {
		if vUntil.Valid {
			_, err = DBtx.Exec(`update storage_quota set value = $1, unit = $2, valid_until = $3, last_updated = NOW(), path = $4 where quotaid = $5`, quota, unit, vUntil.String, spath.String, quotaid)
		} else {
			_, err = DBtx.Exec(`update storage_quota set value = $1, unit = $2, last_updated = NOW(), path = $3 where quotaid = $4`, quota, unit, spath.String, quotaid)	
		}
	}
	
	if err == nil {
		DBtx.Commit(cKey)
		
		if cKey != 0 {
			log.WithFields(QueryFields(r, startTime)).Info("Success!")
			fmt.Fprintf(w, "{ \"ferry_status\": \"success\" }")
		}
	} else {
		if strings.Contains(err.Error(), `User does not exist.`) {
			log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"User does not exist.\" }")
		} else if strings.Contains(err.Error(), `Resource does not exist.`) {
			log.WithFields(QueryFields(r, startTime)).Error("Resource does not exist.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"Resource does not exist.\" }")
		} else if strings.Contains(err.Error(), `Group does not exist.`) {
			log.WithFields(QueryFields(r, startTime)).Error("Group does not exist.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"Group does not exist.\" }")
		} else {
			log.WithFields(QueryFields(r, startTime)).Error(err.Error())
			fmt.Fprintf(w, "{ \"ferry_error\": \"Something went wrong.\" }")
		}
	}
}

func setUserExternalAffiliationAttribute(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	uName := strings.TrimSpace(q.Get("username"))
	attribute := strings.TrimSpace(q.Get("attribute"))
	value := strings.TrimSpace(q.Get("value"))

	if uName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No username specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No username specified.\" }")
		return
	}
	if attribute == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No attribute specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No attribute specified.\" }")
		return
	}
	if value == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No value specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No value specified.\" }")
		return
	}

	authorized, authout := authorize(r)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"ferry_error\": \""+authout+"not authorized.\" }")
		return
	}

	DBtx, cKey, err := LoadTransaction(r, DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
	}
	defer DBtx.Rollback(cKey)

//	_, err = DBtx.Exec(fmt.Sprintf(`do $$
//									declare v_uid int;
//									
//									declare c_uname text = '%s';
//									declare c_attribute text = '%s';
//									declare c_value text = '%s';
//
//									begin
//										select uid into v_uid from users where uname = c_uname;
//										if v_uid is null then
//											raise 'uname does not exist';
//										end if;
//
//										if (v_uid, c_attribute) not in (select uid, attribute from external_affiliation_attribute) then
//											insert into external_affiliation_attribute (uid, attribute, value)
//											values (v_uid, c_attribute, c_value);
//										else
//											update external_affiliation_attribute set
//												value = c_value,
//												last_updated = NOW()
//											where uid = v_uid and attribute = c_attribute;
//										end if;
//									end $$;`, uName, attribute, value))
	execstr := ""
	var uid int
	var att sql.NullString
	queryerr := DBtx.tx.QueryRow(`select us.uid,eaa.attribute from (select uid from users where uname = $1) as us left join (select uid, attribute from external_affiliation_attribute where attribute = $2) as eaa on us.uid=eaa.uid`, uName, attribute).Scan(&uid,&att)
	if queryerr == sql.ErrNoRows {
		log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"User does not exist.\" }")
		return
	} else if queryerr != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error in DB query: " + queryerr.Error())
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query. Check logs.\" }")
		return
	}
	// if att is valid that means the user/attriute combo is in the table already, so this is an update.
	// if it is not valid, then we are doing an insert.
	if att.Valid {
		execstr = `update external_affiliation_attribute set value = $3, last_updated = NOW() where uid = $1  and attribute = $2`
	} else {
		execstr = `insert into external_affiliation_attribute (uid, attribute, value) values ($1, $2, $3)`
		att.String = attribute
		att.Valid = true
	}
	_, err = DBtx.Exec(execstr, uid, att.String, value)
	
	if err == nil {
		DBtx.Commit(cKey)

		if cKey != 0 {
			log.WithFields(QueryFields(r, startTime)).Info("Success!")
			fmt.Fprintf(w, "{ \"ferry_status\": \"success\" }")
		}
	} else {
		if strings.Contains(err.Error(), `uname does not exist`) {
			log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"User does not exist.\" }")
		} else {
			log.WithFields(QueryFields(r, startTime)).Error(err.Error())
			fmt.Fprintf(w, "{ \"ferry_error\": \"Something went wrong.\" }")
		}
	}
}
func removeUserExternalAffiliationAttribute(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	uName := strings.TrimSpace(q.Get("username"))
	attribute := strings.TrimSpace(q.Get("attribute"))

	if uName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No username specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No username specified.\" }")
		return
	}
	if attribute == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No attribute specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No attribute specified.\" }")
		return
	}

	authorized, authout := authorize(r)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"ferry_error\": \""+authout+"not authorized.\" }")
		return
	}

	DBtx, cKey, err := LoadTransaction(r, DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
	}
	defer DBtx.Rollback(cKey)

	var uid int
	var att sql.NullString

	queryerr := DBtx.tx.QueryRow(`select us.uid,eaa.attribute from (select uid from users where uname = $1) as us left join (select uid, attribute from external_affiliation_attribute where attribute = $2) as eaa on us.uid=eaa.uid`,uName, attribute).Scan(&uid,&att)
	if queryerr == sql.ErrNoRows {
		log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"User does not exist.\" }")
		return
	} else if queryerr != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error in DB query: " + queryerr.Error())
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query. Check logs.\" }")
		return
	}
	if !att.Valid {
		log.WithFields(QueryFields(r, startTime)).Error("Attribute does not exist.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"Attribute does not exist.\" }")
		return	
	}
	
	//	_, err = DBtx.Exec(fmt.Sprintf(`do $$
	//									declare v_uid int;
//									
//									declare c_uname text = '%s';
//									declare c_attribute text = '%s';
//
//									begin
//										select uid into v_uid from users where uname = c_uname;
//										if v_uid is null then
//											raise 'uname does not exist';
//										end if;
//
//										if (v_uid, c_attribute) not in (select uid, attribute from external_affiliation_attribute) then
//											raise 'attribute does not exist';
//										end if;
//
//										delete from external_affiliation_attribute where uid = v_uid and attribute = c_attribute;
//									end $$;`, uName, attribute))
//
	_, err = DBtx.Exec(`delete from external_affiliation_attribute where uid = $1 and attribute = $2`, uid, att.String)

	if err == nil {
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		fmt.Fprintf(w, "{ \"ferry_status\": \"success\" }")
	} else {
		if strings.Contains(err.Error(), `uname does not exist`) {
			log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"User does not exist.\" }")
		} else if strings.Contains(err.Error(), `attribute does not exist`) {
			log.WithFields(QueryFields(r, startTime)).Error("External affiliation attribute does not exist.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"External affiliation attribute does not exist.\" }")
		} else {
			log.WithFields(QueryFields(r, startTime)).Error(err.Error())
			fmt.Fprintf(w, "{ \"ferry_error\": \"Something went wrong.\" }")
		}
	}

	DBtx.Commit(cKey)
}
func getUserExternalAffiliationAttributes(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	user := strings.TrimSpace(q.Get("username"))

	if user == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No username specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No username specified.\" }")
		return
	}
	lastupdate, parserr := stringToParsedTime(strings.TrimSpace(q.Get("last_updated")))
	if parserr != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error parsing provided update time: " + parserr.Error())
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error parsing last_updated time. Check ferry logs. If provided, it should be an integer representing an epoch time.\"}")
		return
	}

	rows, err := DBptr.Query(`select attribute, value, user_exists from
							 (select 1 as key, a.attribute, a.value, u.uname, a.last_updated from external_affiliation_attribute as a 
							  left join users as u on a.uid = u.uid where uname = $1) as t right join
							 (select 1 as key, $1 in (select uname from users) as user_exists) as c on t.key = c.key where a.last_updated>=$3 or $3 is null;`, user, lastupdate)

	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Error(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
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
		type jsonerror struct {
			Error string `json:"ferry_error"`
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
		log.WithFields(QueryFields(r, startTime)).Error(err)
	}
	fmt.Fprintf(w, string(jsonout))

}

func addCertificateDNToUser(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

	authorized, authout := authorize(r)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"ferry_error\": \""+authout+"not authorized.\" }")
		return
	}

	q := r.URL.Query()
	uName := strings.TrimSpace(q.Get("username"))
	unitName := strings.TrimSpace(q.Get("unitname"))
	subjDN := strings.TrimSpace(q.Get("dn"))
	if uName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No username specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No username specified.\" }")
		return
	}
	if subjDN == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No DN specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No dn specified.\" }")
		return
	}

	DBtx, cKey, err := LoadTransaction(r, DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
	}
	defer DBtx.Rollback(cKey)

	var uid, dnid sql.NullInt64
	queryerr := DBtx.tx.QueryRow(`select us.uid, uc.dnid from (select 1 as key, uid from users where uname=$1 for update) as us full outer join (select 1 as key, dnid from user_certificates where dn=$2 for update) as uc on uc.key=us.key`,uName, subjDN).Scan(&uid,&dnid)
	if queryerr == sql.ErrNoRows {
		log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
		if cKey != 0 {
			fmt.Fprintf(w, "{ \"ferry_error\": \"User does not exist.\" }")
		}
		return
	} else if queryerr != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error in DB query: " + queryerr.Error())
		if cKey != 0 {
			fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query. Check logs.\" }")
		}
		return
	}
	if ! uid.Valid {
		log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
		if cKey != 0 {
			fmt.Fprintf(w, "{ \"ferry_error\": \"User does not exist.\" }")
		}
		return		
	}
	if ! dnid.Valid {
		_, err := DBtx.Exec(`insert into user_certificates (dn, uid, last_updated) values ($1, $2, NOW()) returning dnid`, subjDN, uid.Int64)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Error in DB insert: " + err.Error())
			if cKey != 0 {
				fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB insert. Check logs.\" }")
			}
			DBtx.Rollback(cKey)
			return
		}
	} else {
		if unitName == "" {
			// error about DN already existing
			log.WithFields(QueryFields(r, startTime)).Error("DN already exists and is assigned to this affiliation unit.")
			if cKey != 0 {
				fmt.Fprintf(w, "{ \"ferry_error\": \"DN already exists and is assigned to this affiliation unit.\" }")
			}
			return	
		}	
	}
	_, err = DBtx.Exec(`insert into affiliation_unit_user_certificate (unitid, dnid, last_updated) values ((select unitid from affiliation_units where name=$1), (select dnid from user_certificates where dn=$2), NOW())`,unitName, subjDN)
	if err != nil {
		if strings.Contains(err.Error(), `pk_affiliation_unit_user_certificate`) {
			if cKey != 0 {
				log.WithFields(QueryFields(r, startTime)).Error("DN already exists and is assigned to this affiliation unit.")
				fmt.Fprintf(w, "{ \"ferry_error\": \"DN already exists and is assigned to this affiliation unit.\" }")
			}
		} else if strings.Contains(err.Error(), `null value in column "unitid"`) {
			log.WithFields(QueryFields(r, startTime)).Error("Affiliation unit does not exist.")
			if cKey != 0 {
				fmt.Fprintf(w, "{ \"ferry_error\": \"Affiliation unit does not exist.\" }")
			}
		} else if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Error in DB insert: " + err.Error())
			if cKey != 0 {
				fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB insert. Check logs.\" }")
			}
			return
		}
	} else {
		if cKey != 0 {
			log.WithFields(QueryFields(r, startTime)).Info("Success!")
			fmt.Fprintf(w, "{ \"ferry_status\": \"success\" }")
		}
		DBtx.Commit(cKey)
	}
}

func removeUserCertificateDN(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

	authorized, authout := authorize(r)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"ferry_error\": \""+authout+"not authorized.\" }")
		return
	}

	q := r.URL.Query()
	uName := strings.TrimSpace(q.Get("username"))
	subjDN := strings.TrimSpace(q.Get("dn"))
	if uName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No username specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No username specified.\" }")
		return
	}
	if subjDN == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No DN specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No dn specified.\" }")
		return
	}

	DBtx, cKey, err := LoadTransaction(r, DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
	}
	defer DBtx.Rollback(cKey)

	var uid, dnid sql.NullInt64
	queryerr := DBtx.tx.QueryRow(`select us.uid, uc.dnid from (select 1 as key, uid from users where uname=$1 for update) as us full outer join (select 1 as key, dnid from user_certificates where dn=$2 for update) as uc on uc.key=us.key`,uName, subjDN).Scan(&uid,&dnid)
	if queryerr == sql.ErrNoRows {
		log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"User does not exist.\" }")
		return
	} else if queryerr != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error in DB query: " + queryerr.Error())
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query. Check logs.\" }")
		return
	}
	if ! uid.Valid {
		log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"User does not exist.\" }")
		return		
	}
	if ! dnid.Valid {
		log.WithFields(QueryFields(r, startTime)).Error("DN does not exist.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"DN does not exist.\" }")
		return		
	}
	_, err = DBtx.Exec(fmt.Sprintf(`do $$ 	
										declare  u_uid constant int := %d;
										declare  u_dnid constant int := %d;
										declare  v_count int;
									
									begin

										if (u_dnid, u_uid) not in (select dnid, uid from user_certificates) then
											raise 'dnid uid association does not exist';
										end if;

										select count(*) into v_count from
											 (select uid, unitid, count(unitid)
											  from affiliation_unit_user_certificate as ac
											  join user_certificates as uc on ac.dnid = uc.dnid
											  where uid = u_uid and unitid in (select unitid
																			   from affiliation_unit_user_certificate
																			   where dnid = u_dnid)
											  group by unitid, uid order by uid, unitid, count) as c
										where c.count = 1;

										if v_count > 0 then
											raise 'unique dnid unitid association';
										end if;

										delete from affiliation_unit_user_certificate where dnid=u_dnid;
										delete from user_certificates where dnid=u_dnid and uid=u_uid;
									end $$;`, uid.Int64, dnid.Int64))
	if err == nil {
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		fmt.Fprintf(w, "{ \"ferry_status\": \"success\" }")
	} else {
		if strings.Contains(err.Error(), `dnid uid association does not exist`) {
			log.WithFields(QueryFields(r, startTime)).Error("USER DN association does not exist.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"USER DN association does not exist.\" }")
		} else if strings.Contains(err.Error(), `unique dnid unitid association`) {
			log.WithFields(QueryFields(r, startTime)).Error("This certificate is unique for the user in one or more affiliation units.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"This certificate is unique for the user in one or more affiliation units.\" }")
		} else {
			log.WithFields(QueryFields(r, startTime)).Error(err.Error())
			fmt.Fprintf(w, "{ \"ferry_error\": \"Something went wrong.\" }")
			return
		}
	}

	DBtx.Commit(cKey)
}

func setUserInfo(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	var fName, status, eDate sql.NullString

	uName := strings.TrimSpace(q.Get("username"))
	fName.String = strings.TrimSpace(q.Get("fullname"))
	status.String = strings.TrimSpace(q.Get("status"))
	eDate.String =strings.TrimSpace( q.Get("expiration_date"))

	if uName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No username specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No username specified.\" }")
		return
	}
	if fName.String == "" {
		fName.Valid = false
	} else {
		fName.Valid = true
	}
	if status.String == "" {
		status.Valid = false
	} else {
		_, err := strconv.ParseBool(status.String)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Invalid status specified in http query.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"Invalid status specified. Should be true or false.\" }")
			return
		}
		status.Valid = true
	}

	if eDate.String == "" {
		eDate.Valid = false
	} else if strings.ToLower(eDate.String) == "null" {
		eDate.Valid = false
	} else {
		eDate.String = fmt.Sprintf("'%s'", eDate.String)
		eDate.Valid = true
	}

	authorized, authout := authorize(r)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"ferry_error\": \""+authout+"not authorized.\" }")
		return
	}

	DBtx, cKey, err := LoadTransaction(r, DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
	}
	defer DBtx.Rollback(cKey)


	var uidint int

	queryerr := DBtx.tx.QueryRow(`select uid from users where uname=$1`,uName).Scan(&uidint)
	if queryerr == sql.ErrNoRows {
		log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"User does not exist.\" }")
		return
	} else if queryerr != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error determining uid.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"DB error determining uid.\" }")
		return
	}
	query := `update users set full_name = coalesce($2, full_name), status = coalesce($3, status), expiration_date = coalesce($4, expiration_date), last_updated = NOW() where uid = $1`
	if strings.ToLower(eDate.String) == "null" {
		query = strings.Replace(query, "coalesce($4, expiration_date)", "$4", 1)
	}
	print(query)
	_, err = DBtx.Exec(query, uidint, fName, status, eDate)

	if err == nil {
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		fmt.Fprintf(w, "{ \"ferry_status\": \"success\" }")
		DBtx.Commit(cKey)
	} else {
		if strings.Contains(err.Error(), `invalid input syntax for type date`) ||
			strings.Contains(err.Error(), `date/time field value out of range`) {
			log.WithFields(QueryFields(r, startTime)).Error("Invalid expiration date.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"Invalid expiration date.\" }")
		} else {
			log.WithFields(QueryFields(r, startTime)).Error(err.Error())
			fmt.Fprintf(w, "{ \"ferry_error\": \"Something went wrong.\" }")
		}
	}
}

func createUser(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

	authorized, authout := authorize(r)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"ferry_error\": \""+authout+"not authorized.\" }")
		return
	}

	q := r.URL.Query()
	uid := strings.TrimSpace(q.Get("uid"))
	uName := strings.TrimSpace(q.Get("username"))
	fullname :=strings.TrimSpace(q.Get("fullname"))
	status, err := strconv.ParseBool(strings.TrimSpace(q.Get("status")))
	expdate := strings.TrimSpace(q.Get("expirationdate"))
	groupname := strings.TrimSpace(q.Get("groupname"))

	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Invalid status specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"Invalid status value. Must be true or false.\" }")
		return
	}
	if uName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No username specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No username specified.\" }")
		return
	}
	if uid == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No UID specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No uid specified.\" }")
		return
	}
	if fullname == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No fullname specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No fullname specified.\" }")
		return
	}
	if expdate == "" {
		expdate = "2038-01-01"
	}
	if groupname == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No groupname specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No groupname specified.\" }")
		return
	}

	DBtx, cKey, err := LoadTransaction(r, DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
	}
	defer DBtx.Rollback(cKey)

	_, err = DBtx.Exec(`insert into users (uname, uid, full_name, status, expiration_date, last_updated)
						values ($1, $2, $3, $4, $5, NOW())`,
						uName, uid, fullname, status, expdate)
	if err != nil {
		if strings.Contains(err.Error(), "invalid input syntax for type date") {
			log.WithFields(QueryFields(r, startTime)).Error("Invalid expiration date.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"Invalid expiration date.\" }")
		} else if strings.Contains(err.Error(), "duplicate key value violates unique constraint \"pk_users\"") {
			log.WithFields(QueryFields(r, startTime)).Error("UID already exists.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"UID already exists\" }")
		} else if strings.Contains(err.Error(), "duplicate key value violates unique constraint \"unq_users_uname\"") {
			log.WithFields(QueryFields(r, startTime)).Error("Username already exists.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"Username already exists.\" }")
		} else {
			log.WithFields(QueryFields(r, startTime)).Error(err.Error())
			fmt.Fprintf(w, "{ \"ferry_error\": \"" + strings.Replace(err.Error(), "\"", "'", -1) + "\" }")
		}
		return
	}

	_, err = DBtx.Exec(`insert into user_group (uid, groupid, is_leader, last_updated)
						values ($1, (select groupid from groups where name = $2 and type = 'UnixGroup'), false, NOW())`,
						uid, groupname)
	if err != nil {
		if strings.Contains(err.Error(), "null value in column \"groupid\" violates not-null constraint") {
			log.WithFields(QueryFields(r, startTime)).Error("Group does not exist.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"Group does not exist.\" }")
		} else {
			log.WithFields(QueryFields(r, startTime)).Error(err.Error())
			fmt.Fprintf(w, "{ \"ferry_error\": \"" + strings.Replace(err.Error(), "\"", "'", -1) + "\" }")
		}
		return
	}

	if cKey != 0 {
		fmt.Fprintf(w, "{ \"ferry_status\": \"success\" }")
	}
	log.WithFields(QueryFields(r, startTime)).Info("Success!")
	DBtx.Commit(cKey)
}

func getMemberAffiliations(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	type jsonerror struct {
		Error string `json:"ferry_error"`
	}
	var inputErr []jsonerror

	user := q.Get("username")
	expOnly := false

	if user == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No username specified in http query.")
		inputErr = append(inputErr, jsonerror{"No username specified."})
	}
	if q.Get("experimentsonly") != "" {
		var err error
		if expOnly, err = strconv.ParseBool(q.Get("experimentsonly")); err != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Invalid experimentsonly specified in http query.")
			inputErr = append(inputErr, jsonerror{"Invalid experimentsonly specified."})
		}
	}
	lastupdate, parserr :=  stringToParsedTime(strings.TrimSpace(q.Get("last_updated")))
	if parserr != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error parsing provided update time: " + parserr.Error())
		inputErr = append(inputErr, jsonerror{"Error parsing last_updated time. Check ferry logs. If provided, it should be an integer representing an epoch time."})
	}
	if len(inputErr) > 0 {
		jsonout, err := json.Marshal(inputErr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error(err)
		}
		fmt.Fprintf(w, string(jsonout))
		return
	}

	rows, err := DBptr.Query(`select name, alternative_name, user_exists from (
									select distinct 1 as key, * from 
										(select au.name, au.alternative_name from affiliation_units as au
										 join affiliation_unit_user_certificate as ac on au.unitid = ac.unitid
										 join user_certificates as uc on ac.dnid = uc.dnid
										 join users as u on uc.uid = u.uid
										 where u.uname = $1 and (((au.unitid in (select unitid from voms_url)) = $2) or not $2) and (ac.last_updated>=$3 or $3 is null)
									) as t
									right join (select 1 as key, $1 in (select uname from users) as user_exists) as c on key = c.key
							 ) as r;`, user, expOnly, lastupdate)

	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Error(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
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
		log.WithFields(QueryFields(r, startTime)).Error(err)
	}
	fmt.Fprintf(w, string(jsonout))
}

func getUserUID(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	uName := q.Get("username")
	if uName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No username specified in http query.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No username specified (use username=foo in the API query).\" }")
		return
	}
	var uid int
	checkerr := DBptr.QueryRow(`select uid from users where uname=$1`, uName).Scan(&uid)
	
	switch {
	case checkerr == sql.ErrNoRows:
		fmt.Fprintf(w, "{ \"ferry_error\": \"User does not exist.\" }")
		log.WithFields(QueryFields(r, startTime)).Error("user " + uName + " not found in DB.")
		return
		
	case checkerr != nil: 
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
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
		log.WithFields(QueryFields(r, startTime)).Error("No uid specified in http query.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No uid specified (use uid=<number> in API query).\" }")
		return
	}
	uid,err := strconv.Atoi(uidstr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Invalid uid specified (either missing or not an integer).")
		fmt.Fprintf(w,"{ \"ferry_error\": \"Invalid uid format.\" }")
		return	
	}
	
	var uname string
	checkerr := DBptr.QueryRow(`select uname from users where uid=$1`, uid).Scan(&uname)
	
	switch {
	case checkerr == sql.ErrNoRows:
		fmt.Fprintf(w, "{ \"ferry_error\": \"User does not exist.\" }")
		log.WithFields(QueryFields(r, startTime)).Error("user ID " + uidstr + " not found in DB.")
		return
		
	case checkerr != nil:
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
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
		log.WithFields(QueryFields(r, startTime)).Error("No username specified in http query.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No username specified.\" }")
		return		
	}

	authorized, authout := authorize(r)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"ferry_error\": \""+authout+"not authorized.\" }")
		return
	}

	// check if the username is already in the DB. If it is not, say so and exit since there is nothing to delete.
	var uname string
	checkerr := DBptr.QueryRow(`select uid from users where uname=$1`, uName).Scan(&uname)
	
	switch {
	case checkerr == sql.ErrNoRows: 
		// set the header for success since we are already at the desired result
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "{ \"ferry_error\": \"Nothing to delete; user does not exist.\" }")
		log.WithFields(QueryFields(r, startTime)).Info("user ID " + uName + " not found in DB.")
		return	
	case checkerr != nil:
		fmt.Fprintf(w, "{ \"ferry_error\": \"Nothing to delete; user does not exist.\" }")
		log.WithFields(QueryFields(r, startTime)).Error("deleteUser: Error querying DB for user " + uName + ".")
		return	
	default:
		// actually do the deletion now
		DBtx, cKey, err := LoadTransaction(r, DBptr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error(err)
		}
		defer DBtx.Rollback(cKey)
	
		_, err = DBtx.Exec(`delete from users where uname=$1`,uName) 
		if err == nil {	
			fmt.Fprintf(w, "{ \"ferry_status\": \"success\" }")
			log.WithFields(QueryFields(r, startTime)).Info("Success!")
			DBtx.Commit(cKey)
			return
		} else {
			fmt.Fprintf(w, "{ \"ferry_error\": \"%s\" }",err.Error())
			log.WithFields(QueryFields(r, startTime)).Error("deleteUser: Error during delete action for user " + uName + ": " + err.Error())
			return			
		}	
	}
}

func getUserAccessToComputeResources(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	type jsonerror struct {
		Error string `json:"ferry_error"`
	}
	var inputErr []jsonerror

	user := q.Get("username")

	if user == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No username specified in http query.")
		inputErr = append(inputErr, jsonerror{"No username specified."})
	}
	lastupdate, parserr :=  stringToParsedTime(strings.TrimSpace(q.Get("last_updated")))
	if parserr != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error parsing provided update time: " + parserr.Error())
		inputErr = append(inputErr, jsonerror{"Error parsing last_updated time. Check ferry logs. If provided, it should be an integer representing an epoch time."})
	}
	
	if len(inputErr) > 0 {
		jsonout, err := json.Marshal(inputErr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error(err)
		}
		fmt.Fprintf(w, string(jsonout))
		return
	}

	rows, err := DBptr.Query(`select  name, type, shell, home_dir, user_exists from
							(select 1 as key, u.uname, cr.name, cr.type, ca.* from
								compute_access as ca left join
								users as u on ca.uid = u.uid left join
								compute_resources as cr on ca.compid = cr.compid
								where u.uname = $1 and (ca.last_updated>=$2 or $2 is null)
							) as t 
							right join (
								select 1 as key, $1 in (select uname from users) as user_exists
							) as c on t.key = c.key;`, user, lastupdate)

	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Error(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
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
		log.WithFields(QueryFields(r, startTime)).Error(err)
	}
	fmt.Fprintf(w, string(jsonout))
}

func getUserAllStorageQuotas(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	type jsonerror struct {
		Error string `json:"ferry_error"`
	}
	var inputErr []jsonerror

	user := q.Get("username")

	if user == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No username specified in http query.")
		inputErr = append(inputErr, jsonerror{"No username specified."})
	}
	
	lastupdate, parserr := stringToParsedTime(strings.TrimSpace(q.Get("last_updated")))
	if parserr != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error parsing provided update time: " + parserr.Error())
		inputErr = append(inputErr, jsonerror{"Error parsing last_updated time. Check ferry logs. If provided, it should be an integer representing an epoch time."})
	}
	
	if len(inputErr) > 0 {
		jsonout, err := json.Marshal(inputErr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error(err)
		}
		fmt.Fprintf(w, string(jsonout))
		return
	}

	rows, err := DBptr.Query(`select  name, path, value, unit, valid_until, user_exists from
							(select 1 as key, u.uname, sr.name, sr.type, sq.* from
								storage_quota as sq left join
								users as u on sq.uid = u.uid left join
								storage_resources as sr on sq.storageid = sr.storageid
								where u.uname = $1 and (sq.last_updated >= $2 or $2 is null)
							) as t 
							right join (
								select 1 as key, $1 in (select uname from users) as user_exists
							) as c on t.key = c.key;`, user, lastupdate)

	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Error(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
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
		log.WithFields(QueryFields(r, startTime)).Error(err)
	}
	fmt.Fprintf(w, string(jsonout))
}

func setUserAccessToComputeResource(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	uname := strings.TrimSpace(q.Get("username"))
	gName := strings.TrimSpace(q.Get("groupname"))
	rName := strings.TrimSpace(q.Get("resourcename"))
	shell := strings.TrimSpace(q.Get("shell"))
	homedir := strings.TrimSpace(q.Get("home_dir"))
	is_primary := strings.TrimSpace(q.Get("is_primary"))

	type jsonerror struct {
		Error string `json:"ferry_error"`
	}
	var inputErr []jsonerror

	if uname == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No username specified in http query.")
		inputErr = append(inputErr, jsonerror{"No value for username specified."})
	}
	if rName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No compute resource specified in http query.")
		inputErr = append(inputErr, jsonerror{"No value for resourcename specified."})
	}
	if gName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No group name specified in http query.")
		inputErr = append(inputErr, jsonerror{"No value for groupname specified."})
	}

	var cagPrimary sql.NullBool
	ispri := false
	if is_primary != "" { 
		tmppri,prierr := strconv.ParseBool(is_primary)
		if prierr != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Invalid value of is_primary. If specified it must be true or false.")
			inputErr = append(inputErr, jsonerror{"Invalid value of is_primary. If specified it must be true or false."})	
		} else {
			ispri = tmppri
		}
	}
	
	if len(inputErr) > 0 {
		jsonout, err := json.Marshal(inputErr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error(err)
		}
		fmt.Fprintf(w, string(jsonout))
		return
	}
	
	authorized, authout := authorize(r)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"ferry_error\": \""+authout+"not authorized.\" }")
		return
	}

	DBtx, cKey, err := LoadTransaction(r, DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
	}
	defer DBtx.Rollback(cKey)
	
	var (
		defShell,defhome sql.NullString
//		grpid,compid,uid sql.NullInt64
		grpid,compid,uid int
	)
	
	// We need to act on two, possibly three, tables: compute_access, compute_access_group and possibly user_group. Let's just work on them independently, but not commit until 
	// both are done.
// This is for the future, but not right now due to time constraints.
//	err = DBtx.tx.QueryRow(`select uid,groupid,compid from ((select 1 as key, uid from users where uname=$1) as myuid full outer join (select 1 as key,groupid from groups where name=$2) as mygroup using(key)) as ugroup right join (select 1 as key, compid from compute_resources where name=$3) as myresource using (key)`,uname,gName,rName).Scan(&uid,&grpid,&compid)


	//We need to check whether the user is in the requested group. If not, add now, or the subsequent steps will fail.
	err = DBtx.tx.QueryRow(`select uid, groupid from user_group join users using(uid) join groups using (groupid) where users.uname=$1 and groups.name=$2`,uname,gName).Scan(&uid,&grpid)
	if err == sql.ErrNoRows {
		// do the insertion now
		_, ugerr := DBtx.Exec(`insert into user_group (uid, groupid) values ((select uid from users where uname=$1),(select groupid from groups where name=$2))`,uname,gName)
		if ugerr != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Error inserting into user_group: " + ugerr.Error())
			if cKey != 0 {
				fmt.Fprintf(w, "{ \"ferry_error\": \"Error checking user_group table. Aborting.\" }")	
			}
			return	
		}
	} else if err != nil {
		
		log.WithFields(QueryFields(r, startTime)).Error("Error checking user_group: " + err.Error())
		if cKey != 0 {
			fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
		}
		return
	}
	
	// OK, now we deal with compute_access in much the same way.
	// In this case we have shell and home directory to deal with though instead of is_primary
	
	err = DBtx.tx.QueryRow(`select ca.uid, ca.compid, ca.shell, ca.home_dir from compute_access as ca
						   join users as u on u.uid=ca.uid
						   join compute_resources as cr on cr.compid=ca.compid
						   where cr.name=$1 and u.uname=$2`,rName,uname).Scan(&uid,&compid,&defShell,&defhome)
	
	switch {
	case err == sql.ErrNoRows:
		
		//grab the default home dir and shell paths for the given compid
		
		checkerr := DBtx.tx.QueryRow(`select default_shell, default_home_dir from compute_resources as cr where cr.name=$1`,rName).Scan(&defShell,&defhome)
		if checkerr == sql.ErrNoRows {
			// the given compid does not exist in this case. Exit accordingly.	
			log.WithFields(QueryFields(r, startTime)).Error("resource " + rName + " does not exist.")
			if cKey != 0 {
				fmt.Fprintf(w, "{ \"ferry_error\": \"Resource does not exist.\" }")
			}
			return	
		}
		//check if the query specified a shell or directory value
		if shell != "" {
			defShell.Valid = true
			defShell.String = strings.TrimSpace(shell)
		}
		//if homedir was provided, use it exactly
		if homedir != "" {
			defhome.Valid = true
			defhome.String = strings.TrimSpace(homedir)
		} else {
			// it was not provided, so we are going to assume the home dir is default_home_dir/username.
			// If default_home_dir is /nashome, we will do /nashome/first letter of username/username
			if defhome.String == "/nashome" || defhome.String == "/nashome/" {
				defhome. Valid = true
				defhome.String = "/nashome/" + uname[0:1]
			} 
			defhome.String = defhome.String + "/" + uname
		}
		// now, do the actual insert
		
		_, inserr := DBtx.Exec(`insert into compute_access (compid, uid, shell, home_dir)
								values ((select compid from compute_resources where name = $1),
										(select uid from users where uname = $2), $3, $4)`,
			rName, uname, defShell, defhome)
		if inserr != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Error in DB insert: " + inserr.Error())
			// now we also need to do a bunch of other checks here
			if strings.Contains(inserr.Error(),"null value in column \"compid\"") {
				if cKey != 0 {
					fmt.Fprintf(w, "{ \"ferry_error\": \"Resource does not exist.\" }")
				}
				return	
				
			} else if strings.Contains(inserr.Error(),"null value in column \"uid\"") {
				if cKey != 0 {
					fmt.Fprintf(w, "{ \"ferry_error\": \"User does not exist.\" }")
				}
				return	
			} else {
				w.WriteHeader(http.StatusNotFound)
				if cKey != 0 {
					fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB insert.\" }")
				}
				return		
			}
		} else {
			log.WithFields(QueryFields(r, startTime)).Info(fmt.Sprintf("Successfully inserted (%s,%s,%s,%s) into compute_access.",rName, uname, defShell, defhome))		
		}
		
	case err != nil:
		log.WithFields(QueryFields(r, startTime)).Error("Error in DB query: " + err.Error()) 
		if cKey != 0 {
			fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
		}
		return		
		
	default: // OK, we already have this resource. We now need to check if the call is trying to change the shell or home dir.
		
		if "" != shell || "" != homedir {
			_, moderr := DBtx.Exec(`update compute_access set shell=$1,home_dir=$2,last_updated=NOW() where uid=$3 and compid=$4`,defShell,defhome,uid,compid)
			if moderr != nil {
				log.WithFields(QueryFields(r, startTime)).Error("Error in DB update: " + err.Error()) 
				if cKey != 0 {
					fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB update.\" }")
				}
				return		
			} else {
				log.WithFields(QueryFields(r, startTime)).Info(fmt.Sprintf("Successfully updated (%s,%s,%s,%s) in compute_access.",rName, uname, defShell, defhome))			
			}
		}
		
	}

	// Begin with compute_access_group
	// see if the user/group/resource combination is already there. If so, then we might just be doing an update.
	
	err = DBtx.tx.QueryRow(`select cag.uid, cag.groupid, cag.compid, cag.is_primary from compute_access_group as cag
						   join groups as g on cag.groupid=g.groupid
						   join users as u on u.uid=cag.uid
						   join compute_resources as cr on cr.compid=cag.compid
						   where cr.name=$1 and u.uname=$2 and g.name=$3`,rName,uname,gName).Scan(&uid,&grpid,&compid,&cagPrimary)
	switch {
	case err == sql.ErrNoRows:

		// OK, we don't have this combo, so we do an insert now
		cagPrimary.Valid = true
		if is_primary != "" {

			cagPrimary.Bool = ispri
		}

		// Now, if the API call said is_primary = true, we need to check for other, existing entries for the same compid and uid, and set their is_primary flag to false. Onyl do this is is_primary was set to true though.
		if is_primary != "" && cagPrimary.Bool == true {
			_, uperr := DBtx.Exec(`update compute_access_group set is_primary=false, last_updated=NOW() where compid=(select compid from compute_resources where name=$1) and uid=(select uid from users where uname=$2) and groupid not in (select groupid from groups where groups.name=$3 and groups.type = 'UnixGroup')`,rName, uname, gName)
			if uperr != nil {	
				
				log.WithFields(QueryFields(r, startTime)).Error("Error update is_primary field in existing DB entries: " + uperr.Error())	
				if cKey != 0 {
					fmt.Fprintf(w, "{ \"ferry_error\": \"Error updating is_primary value for pre-existing compute_access_group entries. See ferry log.\" }")
				}
				return
			}
		}
		
		_, inserr := DBtx.Exec(`insert into compute_access_group (compid, uid, groupid, last_updated, is_primary) values ( (select compid from compute_resources where name=$1), (select uid from users where uname=$2), (select groupid from groups where groups.name=$3 and groups.type = 'UnixGroup'), NOW(), $4)`, rName, uname, gName, cagPrimary)
		if inserr != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Error in DB insert: " + inserr.Error())
			// now we also need to do a bunch of other checks here
			if strings.Contains(inserr.Error(),"null value in column \"compid\"") {
				if cKey != 0 {
					fmt.Fprintf(w, "{ \"ferry_error\": \"Resource does not exist.\" }")
				}
				return	
				
			} else if strings.Contains(inserr.Error(),"null value in column \"uid\"") {
				if cKey != 0 {
					fmt.Fprintf(w, "{ \"ferry_error\": \"User does not exist.\" }")
				}
				return	
			} else if strings.Contains(inserr.Error(),"null value in column \"groupid\"") {
				if cKey != 0 {
					fmt.Fprintf(w, "{ \"ferry_error\": \"Group does not exist.\" }")
				}
				return		
			} else {
				if cKey != 0 {
					fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB insert.\" }")
				}
				return		
			}
			
		} else {
			log.WithFields(QueryFields(r, startTime)).Info(fmt.Sprintf("Successfully inserted (%s,%s,%s) into compute_access_group.",rName, uname, gName))
		}
		
	case err != nil:
		log.WithFields(QueryFields(r, startTime)).Error("Error in DB query: " + err.Error()) 
		if cKey != 0 {
			fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
			}
		return		
		
	default: // OK, we already have this user/group/resource combo. We just need to check if the call is trying to change is_primary from what it is. If is_primary was not provided, that implies we're just keeping what is already there, so just log that nothing is changing and return success.
		
		if ((cagPrimary.Valid && cagPrimary.Bool == ispri) || is_primary == "") && "" == shell && "" == homedir {
			// everything in the DB is already the same as the request, so don't do anything
			log.WithFields(QueryFields(r, startTime)).Print("The request already exists in the database. Nothing to do.")
			if cKey != 0 {
				fmt.Fprintf(w, "{ \"ferry_error\": \"The request already exists in the database.\" }")
			}
			DBtx.Report("The request already exists in the database.")
			return
		} else {
			if is_primary != "" {
				//change the value stored in cagPrimary.Bool to be that of ispri, which is the new value
				cagPrimary.Valid = true
				cagPrimary.Bool = ispri
					// Now, as before, we should set is_primary for any other entries to false, if we just set this entry to true
				if cagPrimary.Bool == true {
					
					_, moderr := DBtx.Exec(`update compute_access_group set is_primary=false,last_updated=NOW() where groupid != $1 and uid=$2 and compid=$3`,grpid,uid,compid)
					if moderr != nil {
						log.WithFields(QueryFields(r, startTime)).Error("Error in DB update: " + err.Error()) 
						if cKey != 0 {
							fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB update.\" }")
							}
						return		
					} else {
						
						log.WithFields(QueryFields(r, startTime)).Info(fmt.Sprintf("Successfully updated (%s,%s) entries in compute_access_group.",rName, uname))					
					}
					_, moderr = DBtx.Exec(`update compute_access_group set is_primary=$1,last_updated=NOW() where groupid=$2 and uid=$3 and compid=$4`,cagPrimary,grpid,uid,compid)
					if moderr != nil {
						log.WithFields(QueryFields(r, startTime)).Error("Error in DB update: " + err.Error()) 
						if cKey != 0 {
							fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB update.\" }")
						}
						return		
					} else {
						
						log.WithFields(QueryFields(r, startTime)).Info(fmt.Sprintf("Successfully updated (%s,%s,%s,%s) in compute_access_group.",rName, uname, gName,is_primary))					
					}
					
				}
			}	
		}
	}
		
	// Finally commit the transaction if both parts succeeded and we don't have a transaction key of 0
	if cKey != 0 {
		DBtx.Commit(cKey)
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "{ \"ferry_status\": \"success\" }")
	}
	return
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
			fmt.Fprintf(w,"{ \"ferry_error\": \"Invalid value for active. Must be true or false (or omit it from the query).\" }")
			return
		}
	}
	
	lastupdate, parserr :=  stringToParsedTime(strings.TrimSpace(q.Get("last_updated")))
	
	if parserr != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error parsing provided update time: " + parserr.Error())
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error parsing last_updated time. Check ferry logs. If provided, it should be an integer representing an epoch time.\"}")
		return
	}

	rows, err := DBptr.Query(`select uname, uid, full_name, status, expiration_date from users where (status=$1 or not $1) and (last_updated>=$2 or $2 is null) order by uname`,strconv.FormatBool(activeonly),lastupdate)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		log.WithFields(QueryFields(r, startTime)).Error("Error in DB query: " + err.Error())
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error in DB query.\" }")
		return
	}
	defer rows.Close()
	
	type jsonout struct {
		Uname string `json:"username"`
		UID int `json:"uid"`
		Fullname string `json:"full_name"`
		Status bool `json:"status"`
		ExpDate string `json:"expiration_date"`
	} 
	var Out []jsonout
	
	for rows.Next() {
		var tmpout jsonout
		rows.Scan(&tmpout.Uname, &tmpout.UID, &tmpout.Fullname, &tmpout.Status, &tmpout.ExpDate)
		Out = append(Out, tmpout)
	}

	var output interface{}	
	if len(Out) == 0 {
		type jsonerror struct {
			Error string `json:"ferry_error"`
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

func getAllUsersFQANs(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	
	lastupdate, parserr :=  stringToParsedTime(strings.TrimSpace(q.Get("last_updated")))
	if parserr != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error parsing provided update time: " + parserr.Error())
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error parsing last_updated time. Check ferry logs. If provided, it should be an integer representing an epoch time.\"}")
		return
	}

	rows, err := DBptr.Query(`select uname, fqan, name from grid_access as ga
							  join grid_fqan as gf on ga.fqanid = gf.fqanid
							  join users as u on ga.uid = u.uid
							  join affiliation_units as au on gf.unitid = au.unitid
							  where (ga.last_updated>=$1 or gf.last_updated>=$1 or
									  u.last_updated>=$1 or au.last_updated>=$1 or $1 is null) order by uname;`, lastupdate)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		log.WithFields(QueryFields(r, startTime)).Error("Error in DB query: " + err.Error())
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error in DB query.\" }")
		return
	}
	defer rows.Close()
	
	type jsonfqan struct {
		FQAN string `json:"fqan"`
		Unit string `json:"unitname"`
	} 
	Out := make(map[string][]jsonfqan)
	
	for rows.Next() {
		var tmpUname, tmpFQAN, tmpUnit sql.NullString
		rows.Scan(&tmpUname, &tmpFQAN, &tmpUnit)
		Out[tmpUname.String] = append(Out[tmpUname.String], jsonfqan{tmpFQAN.String, tmpUnit.String})
	}

	var output interface{}	
	if len(Out) == 0 {
		type jsonerror struct {
			Error string `json:"ferry_error"`
		}
		var queryErr []jsonerror
		queryErr = append(queryErr, jsonerror{"Query returned no FQANs."})
		log.WithFields(QueryFields(r, startTime)).Error("Query returned no FQANs.")
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
