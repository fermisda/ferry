package main

import (
	"errors"
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

// IncludeUserAPIs includes all APIs described in this file in an APICollection
func IncludeUserAPIs(c *APICollection) {
	getUserInfo := BaseAPI {
		InputModel {
			Parameter{UserName, true},
		},
		getUserInfo,
	}
	c.Add("getUserInfo", &getUserInfo)

	setUserInfo := BaseAPI {
		InputModel {
			Parameter{UserName, true},
			Parameter{FullName, false},
			Parameter{Status, false},
			Parameter{GroupAccount, false},
			Parameter{ExpirationDate, false},
		},
		setUserInfo,
	}
	c.Add("setUserInfo", &setUserInfo)

	getSuperUserList := BaseAPI {
		InputModel {
			Parameter{UnitName, true},
		},
		getSuperUserList,
	}
	c.Add("getSuperUserList", &getSuperUserList)

	addCertificateDNToUser := BaseAPI {
		InputModel {
			Parameter{UserName, true},
			Parameter{UnitName, true},
			Parameter{DN, true},
		},
		addCertificateDNToUser,
	}
	c.Add("addCertificateDNToUser", &addCertificateDNToUser)

	getUserExternalAffiliationAttributes := BaseAPI {
		InputModel {
			Parameter{UserName, false},
			Parameter{LastUpdated, false},
		},
		getUserExternalAffiliationAttributes,
	}
	c.Add("getUserExternalAffiliationAttributes", &getUserExternalAffiliationAttributes)

	setUserExternalAffiliationAttribute := BaseAPI {
		InputModel {
			Parameter{UserName, true},
			Parameter{UserAttribute, true},
			Parameter{Value, true},
		},
		setUserExternalAffiliationAttribute,
	}
	c.Add("setUserExternalAffiliationAttribute", &setUserExternalAffiliationAttribute)

	setUserStorageQuota := BaseAPI {
		InputModel {
			Parameter{UserName, false},
			Parameter{GroupName, false},
			Parameter{UnitName, true},
			Parameter{ResourceName, true},
			Parameter{Quota, true},
			Parameter{QuotaUnit, true},
			Parameter{Path, false},
			Parameter{GroupAccount, false},
			Parameter{ExpirationDate, false},
		},
		setUserStorageQuota,
	}
	c.Add("setUserStorageQuota", &setUserStorageQuota)

	getStorageQuotas := BaseAPI {
		InputModel {
			Parameter{UserName, false},
			Parameter{GroupName, false},
			Parameter{ResourceName, false},
			Parameter{LastUpdated, false},
		},
		getStorageQuotas,
	}
	c.Add("getStorageQuotas", &getStorageQuotas)

	setUserAccessToComputeResource := BaseAPI {
		InputModel {
			Parameter{UserName, true},
			Parameter{GroupName, true},
			Parameter{ResourceName, true},
			Parameter{Shell, false},
			Parameter{HomeDir, false},
			Parameter{Primary, false},
		},
		setUserAccessToComputeResource,
	}
	c.Add("setUserAccessToComputeResource", &setUserAccessToComputeResource)

	setUserExperimentFQAN := BaseAPI {
		InputModel {
			Parameter{UserName, true},
			Parameter{UnitName, true},
			Parameter{FQAN, false},
			Parameter{Role, false},
		},
		setUserExperimentFQAN,
	}
	c.Add("setUserExperimentFQAN", &setUserExperimentFQAN)

	getUserCertificateDNs := BaseAPI {
		InputModel {
			Parameter{UserName, false},
			Parameter{UnitName, false},
		},
		getUserCertificateDNs,
	}
	c.Add("getUserCertificateDNs", &getUserCertificateDNs)

	getAllUsersCertificateDNs := BaseAPI {
		InputModel {
			Parameter{UnitName, false},
			Parameter{Status, false},
			Parameter{LastUpdated, false},
		},
		getAllUsersCertificateDNs,
	}
	c.Add("getAllUsersCertificateDNs", &getAllUsersCertificateDNs)
}

func getUserCertificateDNs(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	uid := NewNullAttribute(UID)
	unitid := NewNullAttribute(UnitID)
	queryerr := c.DBtx.QueryRow(`select (select uid from users where uname=$1),
									    (select unitid from affiliation_units where name=$2)`,
							  i[UserName], i[UnitName]).Scan(&uid, &unitid)
	if queryerr != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(queryerr)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !uid.Valid && i[UserName].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
	}
	if !unitid.Valid && i[UnitName].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	rows, queryerr := c.DBtx.Query(`select distinct uname, dn from
							   user_certificates as uc
							   join users as u using(uid)
							   join affiliation_unit_user_certificate ac using(dnid)
							   where uid = coalesce($1, uid) and unitid = coalesce($2, unitid)
							   order by uname`,
							uid, unitid)
	if queryerr != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(queryerr)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	const Certificates Attribute = "certificates"

	type jsonentry map[Attribute]interface{}
	type jsoncerts []interface{}

	entry := jsonentry{
		UserName: 		"",
		Certificates:	make(jsoncerts, 0),
	}
	var out jsoncerts

	row := NewMapNullAttribute(UserName, DN)
	for rows.Next() {
		rows.Scan(row[UserName], row[DN])
		if row[DN].Valid {
			if entry[UserName] == "" {
				entry[UserName] = row[UserName].Data
			}
			if row[UserName].Data != entry[UserName] {
				newEntry := make(jsonentry)
				newEntry[UserName] = entry[UserName]
				newEntry[Certificates] = entry[Certificates]
				out = append(out, newEntry)
				entry[UserName] = row[UserName].Data
				entry[Certificates] = make(jsoncerts, 0)
			}
			entry[Certificates] = append(entry[Certificates].(jsoncerts), row[DN].Data)
		}
	}
	out = append(out, entry)

	return out, nil
}

func getAllUsersCertificateDNs(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	unitid := NewNullAttribute(UnitID)

	queryerr := c.DBtx.QueryRow(`select unitid from affiliation_units where name=$1`,
								i[UnitName]).Scan(&unitid)
	if queryerr != nil && queryerr != sql.ErrNoRows {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(queryerr)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !unitid.Valid && i[UnitName].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
		return nil, apiErr
	}

	activeOnly := i[Status].Default(false)
	
	rows, queryerr := c.DBtx.Query(`select uname, name, dn from
										affiliation_unit_user_certificate as ac
										join user_certificates using(dnid)
										join users using(uid)
										join affiliation_units using(unitid)
										where unitid = coalesce($1, unitid) and (status = $2 or not $2) and (ac.last_updated >= $3 or $3 is null)
									order by uname`, unitid, activeOnly, i[LastUpdated])
	if queryerr != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(queryerr)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	const Certificates Attribute = "certificates"

	type jsoncert map[Attribute]interface{}
	type jsonuser map[Attribute]interface{}
	var out []jsonuser

	prevUname := NewNullAttribute(UserName)
	for rows.Next() {
		row := NewMapNullAttribute(UserName, UnitName, DN)
		rows.Scan(row[UserName], row[UnitName], row[DN])
		if row[UserName].Valid {
			if prevUname != *row[UserName] {
				user := make(jsonuser)
				user[UserName] = row[UserName].Data
				user[Certificates] = make([]jsoncert, 0)
				out = append(out, user)
				prevUname = *row[UserName]
			}
			cert := make(jsoncert)
			cert[UnitName] = row[UnitName].Data
			cert[DN] = row[DN].Data
			out[len(out)-1][Certificates] = append(out[len(out)-1][Certificates].([]jsoncert), cert)
		}
	}

	return out, nil
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

	type jsonfqan struct {
		UnitName string `json:"unit_name"`
		Fqan     string `json:"fqan"`
	}
	var Out []jsonfqan

	for rows.Next() {
		var tmpUnitName, tmpFqan sql.NullString
		rows.Scan(&tmpUnitName, &tmpFqan, &userExists, &exptExists)
		if tmpFqan.Valid {
			Out = append(Out, jsonfqan{tmpUnitName.String, tmpFqan.String})
		}
	}

	var output interface{}	
	if len(Out) == 0 {
		type jsonerror struct {
			Error []string `json:"ferry_error"`
		}
		var queryErr jsonerror
		if !userExists {
			queryErr.Error = append(queryErr.Error, "User does not exist.")
			log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
		}
		if !exptExists {
			queryErr.Error = append(queryErr.Error, "Experiment does not exist.")
			log.WithFields(QueryFields(r, startTime)).Error("Experiment does not exist.")
		}
		if userExists && exptExists {
			queryErr.Error = append(queryErr.Error, "User do not have any assigned FQANs.")
			log.WithFields(QueryFields(r, startTime)).Error("User do not have any assigned FQANs.")
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

func getSuperUserList(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	unitID := NewNullAttribute(UnitID)
	err := c.DBtx.QueryRow(`select unitid from affiliation_units where name = $1`, i[UnitName]).Scan(&unitID)
	if err == sql.ErrNoRows {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
		return nil, apiErr
	} else if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	rows, err := c.DBtx.Query(`select distinct u.uname from
								users as u
								join grid_access as ga on u.uid=ga.uid
								join grid_fqan as gf on ga.fqanid = gf.fqanid
							   where ga.is_superuser=true and gf.unitid=$1
							   order by u.uname`, unitID)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	var out []interface{}

	for rows.Next() {
		row := NewNullAttribute(UserName)
		rows.Scan(&row)
		out = append(out, row.Data)
	}

	return out, nil
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
	var member bool
 
	DBtx, cKey, err := LoadTransaction(r, DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"ferry_error\": \"Unable to start database transaction.\" }")
		return
	}
	defer DBtx.Rollback(cKey)

	queryerr := DBtx.tx.QueryRow(`select (select uid from users where uname=$1),
										 (select unitid from affiliation_units au where au.name=$2),
										 (select ($1, $2) in (select distinct uname, name from users as u
																join grid_access as ga on u.uid = ga.uid
																join grid_fqan as gf on ga.fqanid = gf.fqanid
																join affiliation_units as au on gf.unitid = au.unitid) as member
															 )`, uName, unitName).Scan(&uid, &unitid, &member)
	
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
	if ! member {
		log.WithFields(QueryFields(r, startTime)).Error("User does not belong to unit.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"User does not belong to unit.\" }")
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

func removeSuperUser(w http.ResponseWriter, r *http.Request) {
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
	var member bool
 
	DBtx, cKey, err := LoadTransaction(r, DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"ferry_error\": \"Unable to start database transaction.\" }")
		return
	}
	defer DBtx.Rollback(cKey)

	queryerr := DBtx.tx.QueryRow(`select (select uid from users where uname=$1),
										 (select unitid from affiliation_units au where au.name=$2),
										 (select ($1, $2) in (select distinct uname, name from users as u
																join grid_access as ga on u.uid = ga.uid
																join grid_fqan as gf on ga.fqanid = gf.fqanid
																join affiliation_units as au on gf.unitid = au.unitid) as member
															 )`, uName, unitName).Scan(&uid, &unitid, &member)
	
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
	if ! member {
		log.WithFields(QueryFields(r, startTime)).Error("User does not belong to unit.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"User does not belong to unit.\" }")
		return
	}

	_, err = DBtx.Exec(`update grid_access set is_superuser = false, last_updated = NOW() 
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

func getUserInfo(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	rows, err := c.DBtx.Query(`select full_name, uid, status, is_groupaccount, expiration_date from users where uname=$1`, i[UserName])
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	out := make(map[Attribute]interface{})
	row := NewMapNullAttribute(FullName, UID, Status, GroupAccount, ExpirationDate)
	
	for rows.Next() {
		rows.Scan(row[FullName], row[UID], row[Status], row[GroupAccount], row[ExpirationDate])
		for _, column := range row {
			out[column.Attribute] = column.Data
		}
	}
	if len(out) == 0 {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
		return nil, apiErr
	} else {
		return out, nil
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

	DBtx.Savepoint("duplicateUser")
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
			DBtx.RollbackToSavepoint("duplicateUser")
			log.WithFields(QueryFields(r, startTime)).Error("User already belongs to this group.")
			if cKey != 0 {
				fmt.Fprintf(w, "{ \"ferry_status\": \"User already belongs to this group.\" }")
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

func setUserExperimentFQAN(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	fqan := NewNullAttribute(FQAN)
	if i[FQAN].Valid {
		fqan = i[FQAN]
	} else {
		if i[Role].Valid {
			fqan.Scan("%Role=" + strings.TrimSpace(i[Role].Data.(string)) + "%")
		} else if i[Role].AbsoluteNull {
			fqan.Scan("%Role=NULL%")
		} else {
			apiErr = append(apiErr, APIError{errors.New("no role or fqan specified"), ErrorAPIRequirement})
			return nil, apiErr
		}
	}

	uid := NewNullAttribute(UID)
	unitid := NewNullAttribute(UnitID)
	queryerr := c.DBtx.QueryRow(`select (select uid from users where uname=$1),
									    (select unitid from affiliation_units where name=$2)`,
							  i[UserName], i[UnitName]).Scan(&uid, &unitid)
	if queryerr != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(queryerr)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !uid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
	}
	if !unitid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	var hasCert bool
	queryerr = c.DBtx.QueryRow(`select count(*) > 0 from affiliation_unit_user_certificate as ac
							  join user_certificates as uc on ac.dnid = uc.dnid
							  where uid = $1 and unitid = $2`, uid, unitid).Scan(&hasCert)
	if queryerr != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(queryerr)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !hasCert {
		apiErr = append(apiErr, APIError{errors.New("the user is not a member of the affiliation unit"), ErrorAPIRequirement})
		return nil, apiErr
	}

	rows, queryerr := c.DBtx.Query(`select fqanid from grid_fqan where unitid = $1 and fqan like $2`, unitid, fqan)
	if queryerr != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(queryerr)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	var fqanids []int
	for rows.Next() {
		var fqanid int
		rows.Scan(&fqanid)
		fqanids = append(fqanids, fqanid)
	}
	rows.Close()
	if len(fqanids) == 0 {
		apiErr = append(apiErr, APIError{errors.New("no FQANs found for this query"), ErrorAPIRequirement})
		return nil, apiErr
	}

	for _, fqanid := range fqanids {
		_, queryerr = c.DBtx.Exec(`insert into grid_access (uid, fqanid, is_superuser, is_banned, last_updated)
								   values($1, $2, false, false, NOW())
								   on conflict (uid, fqanid) do nothing`, uid, fqanid)
		if queryerr != nil {
			log.WithFields(QueryFields(c.R, c.StartTime)).Error(queryerr)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
			return nil, apiErr
		}
	}

	return nil, nil
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

	rows, err := DBptr.Query(`select sq.path,sq.value, sq.unit, sq.valid_until from storage_quota sq
							  INNER JOIN affiliation_units on affiliation_units.unitid = sq.unitid
							  INNER JOIN storage_resources on storage_resources.storageid = sq.storageid
							  INNER JOIN users on users.uid = sq.uid
							  where affiliation_units.name=$1 AND storage_resources.type=$2 and users.uname=$3 and (valid_until is null or valid_until >= NOW())
							  order by valid_until desc`, unitName, rName, uName)
	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Error(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")

		return
	}
	defer rows.Close()

	type jsonentry struct {
		Path       string `json:"path"`
		Value      string `json:"value"`
		Unit       string `json:"unit"`
		ValidUntil string `json:"valid_until"`
	}
	var Out jsonentry
	for rows.Next() {
		var tmpPath, tmpUnit, tmpValue, tmpValid sql.NullString
		rows.Scan(&tmpPath, &tmpValue, &tmpUnit, &tmpValid)
		if tmpValue.Valid {
			Out = jsonentry{tmpPath.String, tmpValue.String, tmpUnit.String, tmpValid.String}
		}
	}

	var output interface{}	
	if Out.Value == "" {
		type jsonerror struct {
			Error string `json:"ferry_error"`
		}
		var queryErr []jsonerror
		queryErr = append(queryErr, jsonerror{"User has no quotas registered."})
		log.WithFields(QueryFields(r, startTime)).Error("User has no quotas registered.")
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

func setUserStorageQuota(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError
	var vStorageid, vDataid, vUnitid sql.NullInt64

	gAccount := i[GroupAccount].Default(false)

	var dataAttr Attribute
	if !gAccount.Data.(bool) {
		dataAttr = UserName
	} else {
		dataAttr = GroupName
	}

	if !i[dataAttr].Valid {
		apiErr = append(apiErr, APIError{fmt.Errorf("required parameter %s not provided", dataAttr), ErrorAPIRequirement})
		return nil, apiErr
	}
	
	// get storageID, unitid, uid/gid
	var querystr string
	if gAccount.Data.(bool) {
		querystr = `select (select storageid from storage_resources where name=$1), (select groupid as id from groups where name=$2), (select unitid from affiliation_units where name=$3)`
	} else {
		querystr = `select (select storageid from storage_resources where name=$1), (select uid as id from users where uname=$2), (select unitid from affiliation_units where name=$3)`
	}
	queryerr := c.DBtx.QueryRow(querystr, i[ResourceName], i[dataAttr], i[UnitName]).Scan(&vStorageid, &vDataid, &vUnitid)
	if queryerr != nil && queryerr != sql.ErrNoRows {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(queryerr)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	if !vDataid.Valid {
		var dataID Attribute
		if !gAccount.Data.(bool) {
			dataID = UserName
		} else {
			dataID = GroupName
		}
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, dataID))
	} 
	if !vStorageid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, ResourceName))
	}
	if !vUnitid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
	}

	// We want to store the value in the DB in bytes, no matter what the input unit is. Convert the value here and then set the unit of "B" for bytes	
	newquota, converr := convertValue(i[Quota].Data, i[QuotaUnit].Data.(string), "B")
	if converr != nil {
		apiErr = append(apiErr, APIError{converr, ErrorInvalidData})
	}

	// set the quota value to be stored to newquota, which is now in bytes
	quota := strconv.FormatFloat(newquota, 'f', 0, 64)
	unit := "B"

	if len(apiErr) > 0 {
		return nil, apiErr
	}
	
	var vPath sql.NullString
	var column string

	if gAccount.Data.(bool) {
		column = `groupid`
	} else { 
		column = `uid` 
	}

	if i[Path].Valid {
		vPath.String = i[Path].Data.(string)
		vPath.Valid = i[Path].Valid
	} else if !i[Path].AbsoluteNull {
		queryerr = c.DBtx.tx.QueryRow(`select path from storage_quota
									 where storageid = $1 and ` + column + ` = $2 and
									 unitid = $3 and valid_until is NULL`,
									 vStorageid, vDataid, vUnitid).Scan(&vPath)
		if queryerr != nil && queryerr != sql.ErrNoRows {
			log.WithFields(QueryFields(c.R, c.StartTime)).Error(queryerr)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
			return nil, apiErr
		}
	}
	if !gAccount.Data.(bool) && !vPath.Valid {
		var msg string
		if !i[ExpirationDate].Valid { 
			msg = "null path for user quota"
		} else {
			msg = "no permanent quota"
		}
		apiErr = append(apiErr, APIError{errors.New(msg), ErrorAPIRequirement})
		return nil, apiErr
	}

	var tmpNull string
	if i[ExpirationDate].Valid {
		tmpNull = "not "
	}

	c.DBtx.Exec(`insert into storage_quota (storageid, ` + column + `, unitid, value, unit, valid_until, path, last_updated)
				values ($1, $2, $3, $4, $5, $6, $7, NOW())
				on conflict (storageid, ` + column + `) where valid_until is ` + tmpNull + `null
				do update set value = $4, unit = $5, valid_until = $6, path = $7, last_updated = NOW()`,
				vStorageid, vDataid, vUnitid, quota, unit, i[ExpirationDate], vPath)
	if !i[ExpirationDate].Valid {
		c.DBtx.Exec(`delete from storage_quota where storageid = $1 and ` + column + ` = $2 and valid_until is not null`, vStorageid, vDataid)
	}
	
	if c.DBtx.Error() != nil {
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	return nil, nil
}

func setUserExternalAffiliationAttribute(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	uid := NewNullAttribute(UID)
	err := c.DBtx.QueryRow(`select uid from users where uname = $1`, i[UserName]).Scan(&uid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	if !uid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
	}

	var validAttribute bool
	err = c.DBtx.QueryRow(`select $1 = any (enum_range(null::external_affiliation_attribute_attribute_type)::text[])`, i[UserAttribute]).Scan(&validAttribute)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	if !validAttribute {
		apiErr = append(apiErr, DefaultAPIError(ErrorInvalidData, UserAttribute))
	}
	
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`insert into external_affiliation_attribute (uid, attribute, value) values ($1, $2, $3)
						on conflict (uid, attribute) do update set value = $3`, uid, i[UserAttribute], i[Value])
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, nil
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
	if queryerr != nil {
		if queryerr == sql.ErrNoRows {
			log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"User does not exist.\" }")
			return
		} else if strings.Contains(queryerr.Error(), "invalid input value for enum") {
			log.WithFields(QueryFields(r, startTime)).Error("Invalid attribute.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"Invalid attribute.\" }")
			return
		} else {
			log.WithFields(QueryFields(r, startTime)).Error("Error in DB query: " + queryerr.Error())
			fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query. Check logs.\" }")
			return
		}
	}

	if !att.Valid {
		log.WithFields(QueryFields(r, startTime)).Error("User doesn't have this attribute.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"User doesn't have this attribute.\" }")
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
func getUserExternalAffiliationAttributes(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	uid := NewNullAttribute(UID)

	err := c.DBtx.tx.QueryRow(`select uid from users where uname = $1`, i[UserName]).Scan(&uid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !uid.Valid && i[UserName].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
		return nil, apiErr
	}

	rows, err := DBptr.Query(`select uname, attribute, value from
								external_affiliation_attribute as a
								join users as u using(uid)
							  where u.uid = coalesce($1, uid) and (a.last_updated >= $2 or $2 is null)
							  order by uname`, uid, i[LastUpdated])
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	type jsonentry map[Attribute]interface{}
	out := make(map[string][]jsonentry)

	for rows.Next() {
		row := NewMapNullAttribute(UserName, UserAttribute, Value)
		err = rows.Scan(row[UserName], row[UserAttribute], row[Value])

		if row[UserAttribute].Valid {
			entry := make(jsonentry)
			entry[UserAttribute] = row[UserAttribute].Data
			entry[Value] = row[Value].Data
			out[row[UserName].Data.(string)] = append(out[row[UserName].Data.(string)], entry)
		}
	}

	return out, nil
}

func addCertificateDNToUser(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	// DN validation
	dn, err := ExtractValidDN(i[DN].Data.(string))
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err.Error())
		apiErr = append(apiErr, DefaultAPIError(ErrorInvalidData, DN))
		return nil, apiErr
	}

	uid    := NewNullAttribute(UID)
	dnid   := NewNullAttribute(DNID)
	unitid := NewNullAttribute(UnitID)

	err = c.DBtx.QueryRow(`select (select uid from users where uname = $1),
								  (select dnid from user_certificates where dn=$2),
								  (select unitid from affiliation_units where name=$3)`,
								i[UserName], dn, i[UnitName]).Scan(&uid, &dnid, &unitid)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !uid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
	}
	if !unitid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	if !dnid.Valid {
		_, err := c.DBtx.Exec(`insert into user_certificates (dn, uid, last_updated) values ($1, $2, NOW()) returning dnid`, dn, uid)
		if err != nil {
			log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
			return nil, apiErr
		}
		err = c.DBtx.QueryRow(`select dnid from user_certificates where dn=$1`, dn).Scan(&dnid)
		if err != nil {
			log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
			return nil, apiErr
		}
	}

	_, err = c.DBtx.Exec(`insert into affiliation_unit_user_certificate (unitid, dnid, last_updated) values ($1, $2, NOW())
						  on conflict (unitid, dnid) do nothing`, unitid, dnid)
	if err != nil && !strings.Contains(err.Error(), `pk_affiliation_unit_user_certificate`) {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, nil
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
	} else {
		dn, err := ExtractValidDN(subjDN)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error(err.Error())
			fmt.Fprintf(w, "{ \"ferry_error\": \"%s\" }", err.Error())
			return
		}
		subjDN = dn
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

func setUserInfo(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	if !i[FullName].Valid && !i[Status].Valid && !i[GroupAccount].Valid &&
	   !i[ExpirationDate].Valid && !i[ExpirationDate].AbsoluteNull {
		apiErr = append(apiErr, APIError{errors.New("not enough arguments"), ErrorAPIRequirement})
		return nil, apiErr
	}

	uid := NewNullAttribute(UID)
	expDate := NewNullAttribute(ExpirationDate)

	queryerr := c.DBtx.tx.QueryRow(`select uid, expiration_date from users where uname = $1`,
								   i[UserName]).Scan(&uid, &expDate)
	if queryerr == sql.ErrNoRows {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
		return nil, apiErr
	} else if queryerr != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(queryerr)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	expDate = i[ExpirationDate].Default(expDate.Data)

	_, queryerr = c.DBtx.Exec(`update users set	full_name = coalesce($2, full_name),
												status = coalesce($3, status),
												is_groupaccount = coalesce($4, is_groupaccount),
												expiration_date = $5,
												last_updated = NOW()
							   where uid = $1`,
							  uid, i[FullName], i[Status], i[GroupAccount], expDate)
	if queryerr != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(queryerr)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, nil
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

	var uid, uName, fullname, expdate, groupname sql.NullString
	var status sql.NullBool

	q := r.URL.Query()
	uid.Scan(strings.TrimSpace(q.Get("uid")))
	uName.Scan(strings.TrimSpace(q.Get("username")))
	fullname.Scan(strings.TrimSpace(q.Get("fullname")))
	expdate.Scan(strings.TrimSpace(q.Get("expirationdate")))
	groupname.Scan(strings.TrimSpace(q.Get("groupname")))
	tmpStatus, err := strconv.ParseBool(strings.TrimSpace(q.Get("status")))

	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Invalid status specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"Invalid status value. Must be true or false.\" }")
		return
	} else {
		status.Scan(tmpStatus)
	}
	if uName.String == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No username specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No username specified.\" }")
		return
	}
	if uid.String == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No UID specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No uid specified.\" }")
		return
	}
	if fullname.String == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No fullname specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No fullname specified.\" }")
		return
	}
	if expdate.String == "" {
		expdate.String = "2038-01-01"
	}
	if groupname.String == "" {
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

func getStorageQuotas(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	uid 		:= NewNullAttribute(UID)
	groupid		:= NewNullAttribute(GroupID)
	resourceid	:= NewNullAttribute(ResourceID)

	err := c.DBtx.QueryRow(`select
								(select uid from users where uname = $1),
								(select groupid from groups where name = $2 and type = 'UnixGroup'),
								(select storageid from storage_resources where name = $3)`,
						   i[UserName], i[GroupName], i[ResourceName]).Scan(&uid, &groupid, &resourceid)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !uid.Valid && i[UserName].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
	}
	if !groupid.Valid && i[GroupName].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, GroupName))
	}
	if !resourceid.Valid && i[ResourceName].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, ResourceName))
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	rows, err := DBptr.Query(`select uname, g.name, sr.name, path, value, unit, valid_until from
								storage_quota as sq
								left join users as u on sq.uid = u.uid
								left join groups as g on sq.groupid = g.groupid
								join storage_resources as sr on sq.storageid = sr.storageid
							  where
							  		(u.uid = $1 or $1 is null)
								and (g.groupid = $2 or $2 is null)
								and (sr.storageid = coalesce($3, sr.storageid))
								and (valid_until is null or valid_until >= NOW())
								and (sq.last_updated >= $4 or $4 is null)
							  order by uname asc, g.name asc, sr.name asc, valid_until desc`,
							 uid, groupid, resourceid, i[LastUpdated])
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	const Users Attribute = "users"
	const Groups Attribute = "groups"

	type jsonquota 		map[Attribute]interface{}
	type jsonstorage	map[string]jsonquota
	type jsonowner		map[string]jsonstorage
	
	out := make(map[Attribute]jsonowner)
	out[Users] 	= make(jsonowner)
	out[Groups]	= make(jsonowner)

	for rows.Next() {
		row := NewMapNullAttribute(UserName, GroupName, ResourceName, Path, Quota, QuotaUnit, ExpirationDate)
		rows.Scan(row[UserName], row[GroupName], row[ResourceName], row[Path], row[Quota], row[QuotaUnit], row[ExpirationDate])

		var ownerName, ownerType Attribute
		if row[UserName].Valid {
			ownerName = UserName
			ownerType = Users
		}
		if row[GroupName].Valid {
			ownerName = GroupName
			ownerType = Groups
		}

		if ownerName != "" {
			if _, ok := out[ownerType][row[ownerName].Data.(string)]; !ok {
				out[ownerType][row[ownerName].Data.(string)] = make(jsonstorage)
			}
			out[ownerType][row[ownerName].Data.(string)][row[ResourceName].Data.(string)] = jsonquota {
				Path: 			row[Path].Data,
				Quota: 			row[Quota].Data,
				QuotaUnit:		row[QuotaUnit].Data,
				ExpirationDate:	row[ExpirationDate].Data,
			}
		}
	}

	return out, nil
}

func setUserAccessToComputeResource(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	primary := i[Primary].Default(false)

	dShell	:= NewNullAttribute(Shell)
	dHome	:= NewNullAttribute(HomeDir)
	groupid	:= NewNullAttribute(GroupID)
	compid	:= NewNullAttribute(ResourceID)
	uid		:= NewNullAttribute(UID)

	err := c.DBtx.QueryRow(`select
								(select uid from users where uname = $1),
								(select groupid from groups where name = $2 and type = 'UnixGroup'),
								(select compid from compute_resources where name = $3)`,
						   i[UserName], i[GroupName], i[ResourceName]).Scan(&uid, &groupid, &compid)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !uid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
	}
	if !groupid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, GroupName))
	}
	if !compid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, ResourceName))
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	// Ensure user is member of group (user_group)
	_, err = c.DBtx.Exec(`insert into user_group (uid, groupid) values ($1, $2)
						  on conflict (uid, groupid) do nothing`, uid, groupid)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	
	// Grant user access to the resource (compute_access)
	err = c.DBtx.QueryRow(`select default_shell, default_home_dir from compute_resources where compid = $1`,
						 compid).Scan(&dShell, &dHome)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if dHome.Valid {
		homeStr := strings.TrimRight(dHome.Data.(string), "/")
		if homeStr == "/nashome" {
			homeStr = "/nashome/" + i[UserName].Data.(string)[0:1]
		}
		homeStr = homeStr + "/" + i[UserName].Data.(string)
		dHome.Scan(homeStr)
	}
	
	shell := i[Shell].Default(dShell.Data.(string))
	home  := i[HomeDir].Default(dHome.Data.(string))

	if !shell.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorInvalidData, Shell))
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`insert into compute_access as ca (compid, uid, shell, home_dir) values ($1, $2, $3, $4)
						  on conflict (compid, uid) do update set shell = coalesce($5, ca.shell), home_dir = coalesce($6, ca.home_dir)`,
						compid, uid, shell, home, i[Shell], i[HomeDir])
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	// Add user to group in the resource (compute_access_group)
	var priCount int
	err = c.DBtx.QueryRow(`select count(*) from compute_access_group where uid = $1 and compid = $2 and is_primary`,
							uid, groupid).Scan(&priCount)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	if priCount == 0 {
		primary.Scan(true)
	}

	if primary.Data.(bool) {
		_, err = c.DBtx.Exec(`update compute_access_group set is_primary = false where uid = $1 and compid = $2 and is_primary`,
							uid, compid)
		if err != nil {
			log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
			return nil, apiErr
		}
	}

	_, err = c.DBtx.Exec(`insert into compute_access_group as cg (compid, uid, groupid, is_primary)
						  values ($1, $2, $3, $4)
						  on conflict (compid, uid, groupid) do update set is_primary = coalesce($5, cg.is_primary)`,
						compid, uid, groupid, primary, i[Primary])
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, nil
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
