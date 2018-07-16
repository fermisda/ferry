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

	authorized, authout := authorize(r, AuthorizedDNs)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"ferry_error\": \""+authout+"not authorized.\" }")
		return
	}

	rows, err := DBptr.Query(`select uname, dn, user_exists, unit_exists from (
								select distinct 1 as key, uname, dn
								from affiliation_unit_user_certificate as ac
								join affiliation_units as au on ac.unitid = au.unitid
								join user_certificates as uc on ac.dnid = uc.dnid
								join users as u on uc.uid = u.uid 
								where u.uname like $1 and ac.unitid in (select unitid from grid_fqan where fqan like $3)
								order by uname
							) as t right join (
								select 1 as key,
								$1 in (select uname from users) as user_exists,
								$2 in (select name from affiliation_units) as unit_exists
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
			queryErr.Error = append(queryErr.Error, "User does not have any certifcates registered.")
			log.WithFields(QueryFields(r, startTime)).Error("User does not have any certifcates registered.")
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
	
	authorized, authout := authorize(r, AuthorizedDNs)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"ferry_error\": \""+authout+"not authorized.\" }")
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
	authorized, authout := authorize(r, AuthorizedDNs)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"ferry_error\": \""+authout+"not authorized.\" }")
		return
	}

	q := r.URL.Query()
	uName := q.Get("username")
	unitName := q.Get("unitname")
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

	DBtx, cKey, err := LoadTransaction(r, DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"ferry_error\": \"Unable to start database transaction.\" }")
		return
	}
	_, err = DBtx.Exec(fmt.Sprintf(`do $$
										declare usid int;
										declare unid int;
										declare FqanList int;
									begin
										select uid into usid from users where uname = '%s';
										select unitid into unid from affiliation_units where name = '%s';

										if usid is null then raise 'User does not exist'; end if;
										if unid is null then raise 'Unit does not exist'; end if;

										update grid_access set is_superuser = true, last_updated = NOW()
                                        where uid = usid and fqanid in (select fqanid from grid_fqan where unitid = unid);
									end $$;`, uName, unitName))
	if err == nil {
		DBtx.Commit(cKey)
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		fmt.Fprintf(w, "{ \"ferry_status\": \"success\" }")
	} else {
		if strings.Contains(err.Error(), `User does not exist`) {
			log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"User does not exist.\" }")
		} else if strings.Contains(err.Error(), `Unit does not exist`) {
			log.WithFields(QueryFields(r, startTime)).Error("Unit does not exist.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"Unit does not exist.\" }")
		} else {
			log.WithFields(QueryFields(r, startTime)).Error(err.Error())
			fmt.Fprintf(w, "{ \"ferry_error\": \"Something went wrong.\" }")
		}
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

	uName := q.Get("username")
	gName := q.Get("groupname")
	gType := q.Get("grouptype")
	isLeader := q.Get("is_leader")

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

	authorized, authout := authorize(r, AuthorizedDNs)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"ferry_error\": \""+authout+"not authorized.\" }")
		return
	}

	DBtx, cKey, err := LoadTransaction(r, DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
	}

	_, err = DBtx.Exec(fmt.Sprintf(`do $$
										declare uid int;
										declare groupid int;
									begin
										select u.uid into uid from users as u where uname = '%s';
										select g.groupid into groupid from groups as g where name = '%s' and type = '%s';
										
										insert into user_group (uid, groupid, is_leader, last_updated)
														values (uid, groupid, %s, NOW());
									end $$;`, uName, gName, gType, isLeader))

	if err == nil {
		if cKey != 0 {
			log.WithFields(QueryFields(r, startTime)).Info("Success!")
			fmt.Fprintf(w, "{ \"ferry_status\": \"success\" }")
		}
	} else {
		if strings.Contains(err.Error(), `duplicate key value violates unique constraint`) {
			log.WithFields(QueryFields(r, startTime)).Error("User already belongs to this group.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"User already belongs to this group.\" }")
		} else if strings.Contains(err.Error(), `null value in column "uid" violates not-null constraint`) {
			log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"User does not exist.\" }")
		} else if strings.Contains(err.Error(), `null value in column "groupid" violates not-null constraint`) {
			log.WithFields(QueryFields(r, startTime)).Error("Group does not exist.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"Group does not exist.\" }")
		} else if strings.Contains(err.Error(), `invalid input value for enum`) {
			log.WithFields(QueryFields(r, startTime)).Error("Invalid group type.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"Invalid group type.\" }")
		} else {
			log.WithFields(QueryFields(r, startTime)).Error(err.Error())
			fmt.Fprintf(w, "{ \"ferry_error\": \"Something went wrong.\" }")
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

	user := q.Get("username")
	group := q.Get("groupname")
	gtype := q.Get("grouptype")

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
	authorized,authout := authorize(r,AuthorizedDNs)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w,"{ \"ferry_error\": \"" + authout + "not authorized.\" }")
		return
	}

	DBtx, cKey, err := LoadTransaction(r, DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
	}

	query := fmt.Sprintf(`do $$
						  declare
							cUser constant text := '%s';
							cGroup constant text := '%s';
							cGtype constant groups_group_type := '%s';

							vUid int;
							vGroupid int;
							vError text;
						  begin
							select uid into vUid from users where uname = cUser;
							select groupid into vGroupid from groups where name = cGroup and type = cGtype;

							if vUid is null then vError = concat(vError, 'noUser,'); end if;
							if vGroupid is null then vError = concat(vError, 'noGroup,'); end if;
							if (vUid, vGroupid) not in (select uid, groupid from user_group) then vError = concat(vError, 'user_group,'); end if;
							vError = trim(both ',' from vError);

							if vError is not null then raise '%%', vError; end if;
							
							delete from user_group where uid = vUid and groupid = vGroupid;
						  end $$;`, user, group, gtype)
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

	authorized, authout := authorize(r, AuthorizedDNs)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"ferry_error\": \""+authout+"not authorized.\" }")
		return
	}

	var uid,fqanid int
	queryerr := DBptr.QueryRow(`select uid from users where uname=$1`,uName).Scan(&uid)

	switch {
	case queryerr == sql.ErrNoRows:
		log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"User does not exist.\" }")
		return
	case queryerr != nil:
		log.WithFields(QueryFields(r, startTime)).Error("Error during query:" + queryerr.Error())
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error during DB query; check logs.\" }")
		return	
	}
	
	queryerr = DBptr.QueryRow(`select fqanid from grid_fqan as gf join affiliation_units as au on gf.unitid=au.unitid where au.name=$1 and gf.fqan=$2`,unitName, fqan).Scan(&fqanid)
	switch {
	case queryerr == sql.ErrNoRows:
		log.WithFields(QueryFields(r, startTime)).Error("FQAN " + fqan + " not assigned to affiliation unit " + unitName + ".")
		fmt.Fprintf(w,"{ \"ferry_error\": \"FQAN not assigned to specified unit.\" }")
		return
	case queryerr != nil:
		log.WithFields(QueryFields(r, startTime)).Error("Error during query:" + queryerr.Error())
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error during DB query; check logs.\" }")
		return
	}
	
	DBtx, cKey, err := LoadTransaction(r, DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
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
			fmt.Fprintf(w, "{ \"ferry_error\": \"User does not exist.\" }")
		} else if strings.Contains(err.Error(), `null value in column "fqanid" violates not-null constraint`) {
			log.WithFields(QueryFields(r, startTime)).Error("FQAN does not exist.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"FQAN does not exist.\" }")
		} else if strings.Contains(err.Error(), `duplicate key value violates unique constraint`) {
			if cKey != 0 {
				log.WithFields(QueryFields(r, startTime)).Error("This association already exists.")
				fmt.Fprintf(w, "{ \"ferry_error\": \"This association already exists.\" }")
			}
		} else {
			log.WithFields(QueryFields(r, startTime)).Error(err.Error())
			fmt.Fprintf(w, "{ \"ferry_error\": \"Something went wrong.\" }")
		}
		return
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

	authorized, authout := authorize(r, AuthorizedDNs)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"ferry_error\": \""+authout+"not authorized.\" }")
		return
	}

	DBtx, cKey, err := LoadTransaction(r, DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
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
		fmt.Fprintf(w, "{ \"ferry_status\": \"success\" }")
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

	DBtx.Commit(cKey)
}

func setUserShell(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	aName := q.Get("unitname")
	uName := q.Get("username")
	shell := q.Get("shell")

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

	authorized, authout := authorize(r, AuthorizedDNs)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"ferry_error\": \""+authout+"not authorized.\" }")
		return
	}

	DBtx, cKey, err := LoadTransaction(r, DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
	}

	_, err = DBtx.Exec(fmt.Sprintf(`do $$
										declare cUnitName constant text := '%s';
										declare cUserName constant text := '%s';
										declare cShell    constant text := '%s';

										declare vUid int;
										declare vUnitid int;
									begin
										select uid into vUid from users where uname = cUserName;
										select unitid into vUnitid from affiliation_units where name = cUnitName;

										if vUid is null then raise 'User does not exist.'; end if;
										if vUnitid is null then raise 'Experiment does not exist.'; end if;
										
										if (vUid, vUnitid) not in
										(select uid, unitid from compute_access as ca left join compute_resources as cr on ca.compid = cr.compid)
										then raise 'User does not have access to this resource.';
										end if;

										update compute_access set shell = cShell, last_updated = NOW()
										where uid = vUid and compid in (
											select compid from compute_resources where unitid = vUnitid
										);
									end $$;`, aName, uName, shell))
	if err == nil {
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		fmt.Fprintf(w, "{ \"ferry_status\": \"success\" }")
	} else {
		if strings.Contains(err.Error(), `User does not exist.`) {
			log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"User does not exist.\" }")
		} else if strings.Contains(err.Error(), `Experiment does not exist.`) {
			log.WithFields(QueryFields(r, startTime)).Error("Experiment does not exist.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"Experiment does not exist.\" }")
		} else if strings.Contains(err.Error(), `User does not have access to this resource.`) {
			log.WithFields(QueryFields(r, startTime)).Error("User does not have access to this resource.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"User does not have access to this resource.\" }")
		} else {
			log.WithFields(QueryFields(r, startTime)).Error(err.Error())
			fmt.Fprintf(w, "{ \"ferry_error\": \"Something went wrong.\" }")
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
			output += `"ferry_error": "Resource does not exist.",`
			log.WithFields(QueryFields(r, startTime)).Error("Resource does not exist.")
		}
		if !userExists {
			output += `"ferry_error": "User does not exist.",`
			log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
		}
		output += `"ferry_error": "User doesn't have access to resource."`
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
	authorized, authout := authorize(r, AuthorizedDNs)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"ferry_error\": \""+authout+"not authorized.\" }")
		return
	}

	q := r.URL.Query()
	quota := strings.TrimSpace(q.Get("quota"))
	uName := strings.TrimSpace(q.Get("accountname"))
	unitName := strings.TrimSpace(q.Get("unitname"))
	unit := strings.TrimSpace(q.Get("unit"))
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
	if validtime == "" || strings.ToUpper(validtime) == "NULL" {
		validtime = "NULL"
	} else {
		validtime = "'" + validtime + "'"
	}
	if uName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No user name given.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No username provided.\" }")
		return
	}
	if rName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No resource type given.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No resourcename provided.\" }")
		return
	}
	if unitName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No affiliation unit given.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No unitname provided.\" }")
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
	
	if path == "" {
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



//var vSid,vId,vUnitid int
//var vPath vUntil sql.NullString
//
//querystr := 
//queryerr := DBtx.QueryRow(querystr,
//


	_, err = DBtx.Exec(fmt.Sprintf(`do $$
							declare
								vSid int;
								vUid int;
								vUnitid int;
								vGroupid int;
								vQuotaid int;
								vPath text;
								vUntil date;
         
								cSname constant text := '%s';
								cUname constant text := '%s';
								cEname constant text := '%s';
								cGname constant text := cUname;
								cValue constant text := '%s';
								cUnit constant text := '%s';
								cVuntil constant date := %s;
								cIsgrp constant boolean := %t;
								cPath constant text := '%s';

							begin
                                                                
								select storageid into vSid from storage_resources where name = cSname;
								select unitid into vUnitid from affiliation_units where name = cEname;
                                                                
								if vSid is null then raise 'Resource does not exist.'; end if;
								if vUnitid is null then raise 'Unit does not exist.'; end if;
								if cVuntil is null then vUntil = null ; else vUntil = cVuntil ; end if;
								if cIsgrp is FALSE then
								    select uid into vUid from users where uname = cUname;
								    if vUid is null then raise 'User does not exist.'; end if;
                                	select quotaid, path into vQuotaid, vPath from storage_quota where storageid = vSid and uid = vUid and unitid = vUnitid and ((valid_until is NULL and vUntil is NULL) or (valid_until is not null and vUntil is not null and valid_until = vUntil));
                                    if cPath = 'NULL' then vPath = null; elsif cPath is not null and cPath != '' then vPath = cPath; end if;
									if vQuotaid is not null then
										update storage_quota set value = cValue, unit = cUnit, valid_until = vUntil, last_updated = NOW(), path = vPath
										where quotaid = vQuotaid; 
									else
										insert into storage_quota (storageid, uid, unitid, value, unit, valid_until, path, last_updated)
										values (vSid, vUid, vUnitid, cValue, cUnit, cVuntil, cPath, NOW());
									end if;
                                else
									select groupid into vGroupid from groups where name = cGname and type = 'UnixGroup';
									if vGroupid is null then raise 'Group does not exist.'; end if;
									select quotaid,path into vQuotaid,vPath from storage_quota where storageid = vSid and groupid = vGroupid and unitid = vUnitid and ((valid_until is NULL and vUntil is NULL) or (valid_until is not null and vUntil is not null and valid_until = vUntil));
									if cPath = 'NULL' then vPath = null ; elsif cPath is not null and cPath != '' then vPath = cPath ; end if;
									if vQuotaid is not null then
										update storage_quota set value = cValue, unit = cUnit, valid_until = vUntil, last_updated = NOW(), path = vPath
										where quotaid = vQuotaid; 
									else
										insert into storage_quota (storageid, groupid, unitid, value, unit, valid_until, path, last_updated)
										values (vSid, vGroupid, vUnitid, cValue, cUnit, cVuntil, cPath, NOW());
									end if;
                            	end if;
							end $$;`, rName, uName, unitName, quota, unit, validtime, isGroup, spath.String))
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

	uName := q.Get("username")
	attribute := q.Get("attribute")
	value := q.Get("value")

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

	authorized, authout := authorize(r, AuthorizedDNs)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"ferry_error\": \""+authout+"not authorized.\" }")
		return
	}

	DBtx, cKey, err := LoadTransaction(r, DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
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

	uName := q.Get("username")
	attribute := q.Get("attribute")

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

	authorized, authout := authorize(r, AuthorizedDNs)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"ferry_error\": \""+authout+"not authorized.\" }")
		return
	}

	DBtx, cKey, err := LoadTransaction(r, DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
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

	authorized, authout := authorize(r, AuthorizedDNs)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"ferry_error\": \""+authout+"not authorized.\" }")
		return
	}

	q := r.URL.Query()
	uName := q.Get("username")
	unitName := q.Get("unitname")
	subjDN := q.Get("dn")
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

	_, err = DBtx.Exec(fmt.Sprintf(`do $$ 	
										declare u_uid int;
										declare au_unitid int;
										declare uc_dnid int;
										declare new_dn bool;
										u_dn constant text := '%s';
										u_uname constant text := '%s';
										au_name constant text := '%s';
									begin
										new_dn = false;
										select uid into u_uid from users where uname=u_uname;
										if u_dn not in (select dn from user_certificates) then
											new_dn = true;
											insert into user_certificates (dn, uid, last_updated) values (u_dn, u_uid, NOW());
										end if;
										if au_name != '' then
											select unitid into au_unitid from affiliation_units where name = au_name;
											select dnid into uc_dnid from user_certificates where dn = u_dn;
											insert into affiliation_unit_user_certificate (unitid, dnid, last_updated) values (au_unitid, uc_dnid, NOW());
										else
											if not new_dn then
												raise 'duplicated dn';
											end if;
										end if;
									end $$;`, subjDN, uName, unitName))
	if err == nil {
		if cKey != 0 {
			log.WithFields(QueryFields(r, startTime)).Info("Success!")
			fmt.Fprintf(w, "{ \"ferry_status\": \"success\" }")
		}
	} else {
		log.Print(err.Error())
		if strings.Contains(err.Error(), `pk_affiliation_unit_user_certificate`) {
			if cKey != 0 {
				log.WithFields(QueryFields(r, startTime)).Error("DN already exists and is assigned to this affiliation unit.")
				fmt.Fprintf(w, "{ \"ferry_error\": \"DN already exists and is assigned to this affiliation unit.\" }")
			}
		} else if strings.Contains(err.Error(), `duplicated dn`) {
			log.WithFields(QueryFields(r, startTime)).Error("DN already exists.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"DN already exists.\" }")
		} else if strings.Contains(err.Error(), `"uid" violates not-null constraint`) {
			log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"User does not exist.\" }")
		} else if strings.Contains(err.Error(), `"unitid" violates not-null constraint`) {
			log.WithFields(QueryFields(r, startTime)).Error("Affiliation unit does not exist.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"Affiliation unit does not exist.\" }")
		} else {
			log.WithFields(QueryFields(r, startTime)).Error(err.Error())
			fmt.Fprintf(w, "{ \"ferry_error\": \"Something went wrong.\" }")
		}
		return
	}

	DBtx.Commit(cKey)
}

func removeUserCertificateDN(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

	authorized, authout := authorize(r, AuthorizedDNs)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"ferry_error\": \""+authout+"not authorized.\" }")
		return
	}

	q := r.URL.Query()
	uName := q.Get("username")
	subjDN := q.Get("dn")
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

	_, err = DBtx.Exec(fmt.Sprintf(`do $$ 	
										declare  u_uid int;
										declare  u_dnid int;
										u_dn constant text := '%s';
										u_uname constant text := '%s';

									begin
										select uid into u_uid from users where uname=u_uname;
										if u_uid is null then
											raise 'uname does not exist';
										end if;
										select dnid into u_dnid from user_certificates where dn=u_dn;
										if u_dnid is null then
											raise 'dn does not exist';
										end if;
										if (u_dnid, u_uid) not in (select dnid, uid from user_certificates) then
											raise 'dnid uid association does not exist';
										end if;

										delete from affiliation_unit_user_certificate where dnid=u_dnid;
										delete from user_certificates where dnid=u_dnid and uid=u_uid;
									end $$;`, subjDN, uName))
	if err == nil {
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		fmt.Fprintf(w, "{ \"ferry_status\": \"success\" }")
	} else {
		if strings.Contains(err.Error(), `uname does not exist`) {
			log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"User does not exist.\" }")
		} else if strings.Contains(err.Error(), `dn does not exist`) {
			log.WithFields(QueryFields(r, startTime)).Error("Certificate DN does not exist.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"Certificate DN does not exist.\" }")
		} else if strings.Contains(err.Error(), `dnid uid association does not exist`) {
			log.WithFields(QueryFields(r, startTime)).Error("USER DN association does not exist.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"USER DN association does not exist.\" }")
		} else {
			log.WithFields(QueryFields(r, startTime)).Error(err.Error())
			fmt.Fprintf(w, "{ \"ferry_error\": \"Something went wrong.\" }")
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
		log.WithFields(QueryFields(r, startTime)).Error("No uid specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No uid specified.\" }")
		return
	}
	if uName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No username specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No username specified.\" }")
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
			log.WithFields(QueryFields(r, startTime)).Error("Invalid is_leader specified in http query.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"Invalid is_leader specified.\" }")
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
		fmt.Fprintf(w, "{ \"ferry_error\": \""+authout+"not authorized.\" }")
		return
	}

	DBtx, cKey, err := LoadTransaction(r, DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
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
		fmt.Fprintf(w, "{ \"ferry_status\": \"success\" }")
		DBtx.Commit(cKey)
	} else {
		if strings.Contains(err.Error(), `uid does not exist`) {
			log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"User does not exist.\" }")
		} else if strings.Contains(err.Error(), `invalid input syntax for type date`) ||
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

	authorized, authout := authorize(r, AuthorizedDNs)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"ferry_error\": \""+authout+"not authorized.\" }")
		return
	}

	q := r.URL.Query()
	uid := strings.TrimSpace(q.Get("uid"))
	uName := strings.TrimSpace(q.Get("username"))
	firstName :=strings.TrimSpace( q.Get("firstname"))
	lastName := strings.TrimSpace(q.Get("lastname"))
	status, err := strconv.ParseBool(strings.TrimSpace(q.Get("status")))
	expdate := strings.TrimSpace(q.Get("expirationdate"))

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
	if firstName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No first name specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No firstname specified.\" }")
		return
	}
	if lastName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No last name specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No lastname specified.\" }")
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
		DBtx, cKey, err := LoadTransaction(r, DBptr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error(err)
		}
		//actually insert
		_, err = DBtx.Exec(`insert into users (uname, uid, full_name, status, expiration_date, last_updated)
							values ($1, $2, $3, $4, $5, NOW())`, uName, uid, fullname, status, expdate)

	//	theStmt := fmt.Sprintf("insert into users (uname, uid, full_name, status, expiration_date, last_updated) values ('%s',%d,'%s','%s','%s',NOW())", uName, uid, fullname, status, expdate)
	//	fmt.Println(theStmt)

	//	_, err = DBtx.Exec(fmt.Sprintf("insert into users (uname, uid, full_name, status, expiration_date, last_updated) values ('%s',%s,'%s',%t,'%s', NOW())", uName, uid, fullname, status, expdate))

		if err == nil {
			if cKey != 0 {
				log.WithFields(QueryFields(r, startTime)).Info("Success!")
				fmt.Fprintf(w, "{ \"ferry_status\": \"success\" }")
			}
			DBtx.Commit(cKey)
			return
		} else {
			log.WithFields(QueryFields(r, startTime)).Error(err.Error())
			fmt.Fprintf(w, "{ \"ferry_error\": \""+err.Error()+"\" }")
		}
	case checkerr != nil:
		log.WithFields(QueryFields(r, startTime)).Error(checkerr.Error())
		fmt.Fprintf(w, "{ \"ferry_error\": \""+checkerr.Error()+"\" }")
	default:
		log.WithFields(QueryFields(r, startTime)).Error("user "+uName+" already exists.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"user "+uName+" already exists.\"}")
	}

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
									select 1 as key, * from (
										select distinct * from (select au.name, au.alternative_name from affiliation_units as au
																right join grid_fqan as gf on au.unitid = gf.unitid
																right join grid_access as ga on gf.fqanid = ga.fqanid
																left join users as u on ga.uid = u.uid
																where u.uname = $1 and (((au.unitid in (select unitid from voms_url)) = $2) or not $2) and (ga.last_updated>=$3 or $3 is null)) as u

										union                  (select au.name, au.alternative_name from affiliation_units as au
																right join affiliation_unit_user_certificate as ac on au.unitid = ac.unitid
																left join user_certificates as uc on ac.dnid = uc.dnid
																left join users as u on uc.uid = u.uid
																where u.uname = $1 and (((au.unitid in (select unitid from voms_url)) = $2) or not $2) and (ac.last_updated>=$3 or $3 is null))

										union                  (select au.name, au.alternative_name from affiliation_units as au
																right join affiliation_unit_group as ag on au.unitid = ag.unitid
																join user_group as ug on ag.groupid = ug.groupid
																left join users as u on ug.uid = u.uid
																where u.uname = $1 and (((au.unitid in (select unitid from voms_url)) = $2) or not $2) and (ag.last_updated>=$3 or $3 is null))
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

	authorized, authout := authorize(r, AuthorizedDNs)
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
		myStmt,myStmterr := DBptr.Prepare(fmt.Sprintf("delete from users where uname='%s'",uName))
		if myStmterr != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Error creating prepared statement for deleteUser(" + uName + ").")	
		}
		_, err = myStmt.Exec() 
		if err == nil {	
			fmt.Fprintf(w, "{ \"ferry_status\": \"success\" }")
			log.WithFields(QueryFields(r, startTime)).Info("Success!")
			DBtx.Commit(cKey)
			myStmt.Close()
			return
		} else {
			fmt.Fprintf(w, "{ \"ferry_error\": \"%s\" }",err.Error())
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
	
	authorized, authout := authorize(r, AuthorizedDNs)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"ferry_error\": \""+authout+"not authorized.\" }")
		return
	}

	DBtx, cKey, err := LoadTransaction(r, DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
	}
	
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
			fmt.Fprintf(w, "{ \"ferry_error\": \"Error checking user_group table. Aborting.\" }")	
			return	
		}
	} else if err != nil {
		
		log.WithFields(QueryFields(r, startTime)).Error("Error checking user_group: " + err.Error())
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")	
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
			fmt.Fprintf(w, "{ \"ferry_error\": \"Resource does not exist.\" }")
			if cKey != 0 {
				DBtx.Rollback()
			}
			return	
		}
		//check if the query specified a shell or directory value
		if shell != "" {
			defShell.Valid = true
			defShell.String = strings.TrimSpace(shell)
		}
		if homedir != "" {
			defhome.Valid = true
			defhome.String = strings.TrimSpace(homedir)
		}
		// now, do the actual insert
		
		_, inserr := DBtx.Exec(`insert into compute_access (compid, uid, shell, home_dir)
								values ((select compid from compute_resources where name = $1),
										(select uid from users where uname = $2), $3, $4)`,
			rName, uname, defShell, defhome)
		if inserr != nil {
			if cKey != 0 {
				DBtx.Rollback()
			}
			log.WithFields(QueryFields(r, startTime)).Error("Error in DB insert: " + inserr.Error())
			// now we also need to do a bunch of other checks here
			if strings.Contains(inserr.Error(),"null value in column \"compid\"") {
				fmt.Fprintf(w, "{ \"ferry_error\": \"Resource does not exist.\" }")
				return	
				
			} else if strings.Contains(inserr.Error(),"null value in column \"uid\"") {
				fmt.Fprintf(w, "{ \"ferry_error\": \"User does not exist.\" }")
				return	
			} else {
				w.WriteHeader(http.StatusNotFound)
				fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB insert.\" }")
				return		
			}
		} else {
			log.WithFields(QueryFields(r, startTime)).Info(fmt.Sprintf("Successfully inserted (%s,%s,%s,%s) into compute_access.",rName, uname, defShell, defhome))		
		}
		
	case err != nil:
		log.WithFields(QueryFields(r, startTime)).Error("Error in DB query: " + err.Error()) 
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
		if cKey != 0 {
			DBtx.Rollback()
		}
		return		
		
	default: // OK, we already have this user/group/resource combo. We just need to check if the call is trying to change the shell or home dir. If neither option was provided, that implies we're just keeping what is already there, so just log that nothing is changing and return success.
		
		if "" == shell && "" == homedir {
			// everything in the DB is already the same as the request, so don't do anything
			log.WithFields(QueryFields(r, startTime)).Print("The request already exists in the database. Nothing to do.")
			if cKey != 0 {
				fmt.Fprintf(w, "{ \"ferry_error\": \"The request already exists in the database.\" }")
			}
			DBtx.Report("The request already exists in the database.")
		} else {
			_, moderr := DBtx.Exec(`update compute_access set shell=$1,home_dir=$2,last_updated=NOW() where uid=$3 and compid=$4`,defShell,defhome,uid,compid)
			if moderr != nil {
				log.WithFields(QueryFields(r, startTime)).Error("Error in DB update: " + err.Error()) 
				if cKey != 0 {
				DBtx.Rollback()	
				}
				fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB update.\" }")
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
				fmt.Fprintf(w, "{ \"ferry_error\": \"Error updating is_primary value for pre-existing compute_access_group entries. See ferry log.\" }")
				if cKey != 0 {
					DBtx.Rollback()
				}
				return
			}
		}
		
		_, inserr := DBtx.Exec(`insert into compute_access_group (compid, uid, groupid, last_updated, is_primary) values ( (select compid from compute_resources where name=$1), (select uid from users where uname=$2), (select groupid from groups where groups.name=$3 and groups.type = 'UnixGroup'), NOW(), $4)`, rName, uname, gName, cagPrimary)
		if inserr != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Error in DB insert: " + inserr.Error())
			// now we also need to do a bunch of other checks here
			if strings.Contains(inserr.Error(),"null value in column \"compid\"") {
				fmt.Fprintf(w, "{ \"ferry_error\": \"Resource does not exist.\" }")
				return	
				
			} else if strings.Contains(inserr.Error(),"null value in column \"uid\"") {
				fmt.Fprintf(w, "{ \"ferry_error\": \"User does not exist.\" }")
				return	
			} else if strings.Contains(inserr.Error(),"null value in column \"groupid\"") {
				fmt.Fprintf(w, "{ \"ferry_error\": \"Group does not exist.\" }")
				return		
			} else {
				fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB insert.\" }")
				return		
			}
			
		} else {
			log.WithFields(QueryFields(r, startTime)).Info(fmt.Sprintf("Successfully inserted (%s,%s,%s) into compute_access_group.",rName, uname, gName))
		}
		
	case err != nil:
		log.WithFields(QueryFields(r, startTime)).Error("Error in DB query: " + err.Error()) 
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
		return		
		
	default: // OK, we already have this user/group/resource combo. We just need to check if the call is trying to change is_primary from what it is. If is_primary was not provided, that implies we're just keeping what is already there, so just log that nothing is changing and return success.
		
		if (cagPrimary.Valid && cagPrimary.Bool == ispri) || is_primary == "" {
			// everything in the DB is already the same as the request, so don't do anything
			log.WithFields(QueryFields(r, startTime)).Print("The request already exists in the database. Nothing to do.")
			if cKey != 0 {
				fmt.Fprintf(w, "{ \"ferry_error\": \"The request already exists in the database.\" }")
			}
			DBtx.Report("The request already exists in the database.")
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
						fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB update.\" }")
						if cKey != 0 {
							DBtx.Rollback()
						}
						return		
					} else {
						
						log.WithFields(QueryFields(r, startTime)).Info(fmt.Sprintf("Successfully updated (%s,%s) entries in compute_access_group.",rName, uname))					
					}
					_, moderr = DBtx.Exec(`update compute_access_group set is_primary=$1,last_updated=NOW() where groupid=$2 and uid=$3 and compid=$4`,cagPrimary,grpid,uid,compid)
					if moderr != nil {
						log.WithFields(QueryFields(r, startTime)).Error("Error in DB update: " + err.Error()) 
						fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB update.\" }")
						if cKey != 0 {
							DBtx.Rollback()
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
