package main

import (
	"regexp"
	"strconv"
	"strings"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
	"net/http"
	log "github.com/sirupsen/logrus"
)

func getUserInfoLegacy(w http.ResponseWriter, r *http.Request) {
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
	rows, err := DBptr.Query(`select full_name, uid, status, is_groupaccount, expiration_date from users where uname=$1`, uname)
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
			GrpAcct  bool      `json:"groupaccount"`
			ExpDate  time.Time `json:"expiration_date"`
		}

		var Out jsonout

		for rows.Next() {
			if idx == 0 {
				fmt.Fprintf(w, "[ ")
			} else {
				fmt.Fprintf(w, ",")
			}
			rows.Scan(&Out.FullName, &Out.Uid, &Out.Status, &Out.GrpAcct, &Out.ExpDate)
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

func getSuperUserListLegacy(w http.ResponseWriter, r *http.Request) {
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

func addCertificateDNToUserLegacy(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

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
	} else {
		dn, err := ExtractValidDN(subjDN)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error(err.Error())
			if cKey != 0 {
				fmt.Fprintf(w, "{ \"ferry_error\": \"%s\" }", err.Error())
			} else {
				DBtx.Report(err.Error())
			}
			return
		}
		subjDN = dn
	}

	var uid, dnid sql.NullInt64
	queryerr := DBtx.tx.QueryRow(`select us.uid, uc.dnid from (select 1 as key, uid from users where uname=$1 for update) as us full outer join (select 1 as key, dnid from user_certificates where dn=$2 for update) as uc on uc.key=us.key`,uName, subjDN).Scan(&uid,&dnid)
	if queryerr == sql.ErrNoRows {
		log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
		if cKey != 0 {
			fmt.Fprintf(w, "{ \"ferry_error\": \"User does not exist.\" }")
		}
		DBtx.Report("User does not exist.")
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
		DBtx.Report("User does not exist.")
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

func setUserExternalAffiliationAttributeLegacy(w http.ResponseWriter, r *http.Request) {
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

func setUserStorageQuotaLegacy(w http.ResponseWriter, r *http.Request) {
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
	if validtime != "" {
		vUntil.Scan(validtime)
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
		fmt.Fprintf(w, "{ \"ferry_error\": \"No quota_unit provided.\" }")
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
	
	var vPath sql.NullString
	var column string

	if isGroup {
		column = `groupid`
	} else { 
		column = `uid` 
	}

	if !spath.Valid {
		queryerr = DBtx.tx.QueryRow(`select path from storage_quota
									 where storageid = $1 and ` + column + ` = $2 and
									 unitid = $3 and valid_until is NULL`,
									 vSid, vId, vUnitid).Scan(&vPath)
		if queryerr == sql.ErrNoRows {
			if !vUntil.Valid { 
				DBtx.Report("Null path for user quota.")
			} else {
				DBtx.Report("No permanent quota.")
			}
		}
	} else {
		vPath = spath
	}

	if vPath.Valid {
		var tmpNull string
		if vUntil.Valid {
			tmpNull = "not "
		}

		DBtx.Exec(`insert into storage_quota (storageid, ` + column + `, unitid, value, unit, valid_until, path, last_updated)
				   values ($1, $2, $3, $4, $5, $6, $7, NOW())
				   on conflict (storageid, ` + column + `) where valid_until is ` + tmpNull + `null
				   do update set value = $4, unit = $5, valid_until = $6, path = $7, last_updated = NOW()`,
				   vSid, vId, vUnitid, quota, unit, vUntil, vPath)
		if !vUntil.Valid {
			DBtx.Exec(`delete from storage_quota where storageid = $1 and ` + column + ` = $2 and valid_until is not null`, vSid, vId)
		}
	}
	
	if DBtx.Error() == nil {
		DBtx.Commit(cKey)
		
		if cKey != 0 {
			log.WithFields(QueryFields(r, startTime)).Info("Success!")
			fmt.Fprintf(w, "{ \"ferry_status\": \"success\" }")
		}
	} else {
		if strings.Contains(DBtx.Error().Error(), `User does not exist.`) {
			log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"User does not exist.\" }")
		} else if strings.Contains(DBtx.Error().Error(), `Resource does not exist.`) {
			log.WithFields(QueryFields(r, startTime)).Error("Resource does not exist.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"Resource does not exist.\" }")
		} else if strings.Contains(DBtx.Error().Error(), `Group does not exist.`) {
			log.WithFields(QueryFields(r, startTime)).Error("Group does not exist.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"Group does not exist.\" }")
		} else if strings.Contains(DBtx.Error().Error(), `Null path for user quota.`) {
			log.WithFields(QueryFields(r, startTime)).Error("Null path for user quota.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"No path given. It is required for permanent user quotas.\" }")
		} else if strings.Contains(DBtx.Error().Error(), `No permanent quota.`) {
			log.WithFields(QueryFields(r, startTime)).Error("No permanent quota.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"No permanent quota defined.\" }")
		} else if strings.Contains(DBtx.Error().Error(), `invalid input syntax for type date`) {
			log.WithFields(QueryFields(r, startTime)).Error("Invalid valid_until date.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"Invalid valid_until date.\" }")
		} else {
			log.WithFields(QueryFields(r, startTime)).Error(DBtx.Error().Error())
			fmt.Fprintf(w, "{ \"ferry_error\": \"Something went wrong.\" }")
		}
	}
}

func setUserAccessToComputeResourceLegacy(w http.ResponseWriter, r *http.Request) {
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

	// Check if the user has a primary group in the resource. Set is_primary=true if it's not the case.
	var priCount int
	err = DBtx.tx.QueryRow(`select count(*) from compute_access_group as cg
							join users as u on cg.uid = u.uid
							join compute_resources as cr on cg.compid = cr.compid
							where u.uname = $1 and cr.name = $2 and cg.is_primary`,
	uname, rName).Scan(&priCount)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
		return
	}
	if priCount == 0 {
		ispri = true
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
		if is_primary != "" || priCount == 0 {
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
		
		if ((cagPrimary.Valid && cagPrimary.Bool == ispri) || is_primary == "" && !ispri) && "" == shell && "" == homedir {
			// everything in the DB is already the same as the request, so don't do anything
			log.WithFields(QueryFields(r, startTime)).Print("The request already exists in the database. Nothing to do.")
			if cKey != 0 {
				fmt.Fprintf(w, "{ \"ferry_status\": \"success\" }")
			}
			DBtx.Report("The request already exists in the database.")
			return
		} else {
			if is_primary != "" || ispri {
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

func addUsertoExperimentLegacy(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	var duplicateCount, duplicateCountRef int

	var compResource string
	var compGroup string

	type jsonerror struct {
		Error string `json:"ferry_error"`
	}
	var inputErr []jsonerror

	unit := strings.TrimSpace(q.Get("unitname"))
	if unit == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No unitname specified in http query.")
		inputErr = append(inputErr, jsonerror{"No unitname specified."})
	}

	if len(inputErr) > 0 {
		jsonout, err := json.Marshal(inputErr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error(err)
		}
		fmt.Fprintf(w, string(jsonout))
		return
	}
	
    	authorized,authout := authorize(r)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w,"{ \"ferry_error\": \"" + authout + "not authorized.\" }")
		return
	}    
        
	var DBtx Transaction
	R := WithTransaction(r, &DBtx)
	
	key, err := DBtx.Start(DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error starting database transaction: " + err.Error())
		inputErr = append(inputErr, jsonerror{"Error starting database transaction." })
		jsonout, err := json.Marshal(inputErr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error(err)
		}
		fmt.Fprintf(w, string(jsonout))
		return
	}
	defer DBtx.Rollback(key)

	uName := strings.TrimSpace(q.Get("username"))
	if uName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No username specified.")
		inputErr = append(inputErr, jsonerror{"No username specified." })
		jsonout, err := json.Marshal(inputErr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error(err)
		}
		fmt.Fprintf(w, string(jsonout))
		return	
	}
	dnTemplate := "/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=%s/CN=UID:%s"
	var fullName string
	var valid bool

	rows, err := DBtx.Query("select full_name, status from users where uname = $1", uName)
	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Error(err)
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
		return
	}

	if !rows.Next() {
		log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"User does not exist.\" }")
		return
	}
	rows.Scan(&fullName, &valid)
	rows.Close()

	if !valid {
		log.WithFields(QueryFields(r, startTime)).Error("User status is not valid.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"User status is not valid.\" }")
		return
	}

	q.Set("dn", fmt.Sprintf(dnTemplate, fullName, uName))
	R.URL.RawQuery = q.Encode()

	duplicateCountRef ++
	DBtx.Savepoint("addCertificateDNToUser")
	addCertificateDNToUserLegacy(w, R)
	if !DBtx.Complete() {
		if !strings.Contains(DBtx.Error().Error(), "duplicate key value violates unique constraint") {
			log.WithFields(QueryFields(r, startTime)).Error("addCertificateDNToUser failed: DBtx.Error().Error()" )
			if  strings.Contains(DBtx.Error().Error(), `null value in column "unitid" violates not-null constraint`) {
				fmt.Fprintf(w, "{ \"ferry_error\": \"Affiliation unit does not exist.\" }")					
			} else {		
				fmt.Fprintf(w, "{ \"ferry_error\": \"addCertificateDNToUser failed. Last DB error: " + DBtx.Error().Error() + ". Rolling back transaction.\" }")
			}
			return
		}
		DBtx.RollbackToSavepoint("addCertificateDNToUser")
		duplicateCount ++
	}

	for _, fqan := range []string{"Analysis", "NULL"} {
		rows, err := DBtx.Query(`select fqan from grid_fqan
								 where (lower(fqan) like lower($1) or lower(fqan) like lower($2)) and mapped_user is null;`,
								 "/" + unit + "/Role=" + fqan + "%", "/fermilab/" + unit + "/Role=" + fqan + "%")
		if err != nil {
			defer log.WithFields(QueryFields(r, startTime)).Error(err)
			fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
			return
		}
		if !rows.Next() {
			log.WithFields(QueryFields(r, startTime)).Error("FQAN not found.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"FQAN not found.\" }")
			return
		}

		var fullFqan string
		rows.Scan(&fullFqan)

		if rows.Next() {
			rows.Scan(&fullFqan)
			log.WithFields(QueryFields(r, startTime)).Error("Found ambiguous FQANs.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"Found ambiguous FQANs.\" }")
			return
		}

		rows.Close()

		q.Set("fqan", fullFqan)
		R.URL.RawQuery = q.Encode()

		duplicateCountRef ++
		DBtx.Savepoint("setUserExperimentFQAN_" + fqan)
		setUserExperimentFQANLegacy(w, R)
		if !DBtx.Complete() {
			if !strings.Contains(DBtx.Error().Error(), "duplicate key value violates unique constraint") && !strings.Contains(DBtx.Error().Error(), "FQAN not assigned to specified unit") {
				log.WithFields(QueryFields(r, startTime)).Error("Failed to set FQAN to user: " + DBtx.Error().Error())
				fmt.Fprintf(w, "{ \"ferry_error\": \"setUserExperimentFQAN failed. Last DB error: " + DBtx.Error().Error() + ". Rolling back transaction.\" }")
				return	
			}
			DBtx.RollbackToSavepoint("setUserExperimentFQAN_" + fqan)
			duplicateCount ++
		}
	}

	rows, err = DBtx.Query(`select cr.name from compute_resources as cr
							 left join affiliation_units au on cr.unitid = au.unitid
							 where au.name = $1 and cr.type = 'Interactive';`, unit)
	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Error(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
		return
	}

	if !rows.Next() {
		log.WithFields(QueryFields(r, startTime)).Error("Compute resource not found.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"Compute resource not found.\" }")
		return
	}
	rows.Scan(&compResource)
	rows.Close()

	rows, err = DBtx.Query(`select gp.name from affiliation_unit_group as ag
							left join affiliation_units as au on ag.unitid = au.unitid
							left join groups as gp on ag.groupid = gp.groupid
							where ag.is_primary and au.name = $1;`, unit)
	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Error(err)
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
		return
	}

	if !rows.Next() {
		log.WithFields(QueryFields(r, startTime)).Error("Primary group not found for this unit.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"Primary group not found for this unit.\" }")
		return
	}
	rows.Scan(&compGroup)
	rows.Close()

	q.Set("resourcename", compResource)
	q.Set("groupname", compGroup)
	q.Set("is_primary", "true")
	R.URL.RawQuery = q.Encode()

	duplicateCountRef ++
	DBtx.Savepoint("setUserAccessToComputeResource_" + compResource)
	setUserAccessToComputeResourceLegacy(w, R)
	if !DBtx.Complete() {
		if !strings.Contains(DBtx.Error().Error(), "The request already exists in the database.") {
			log.WithFields(QueryFields(r, startTime)).Error("addUserToGroup failed: " + DBtx.Error().Error() )
			fmt.Fprintf(w, "{ \"ferry_error\": \"setUserAccessToComputeResource for " + compResource + " failed. Last DB error: " + DBtx.Error().Error() + ". Rolling back transaction.\" }")
			return
		}
		DBtx.RollbackToSavepoint("setUserAccessToComputeResource_" + compResource)
		duplicateCount ++
	}
	
	// now we need to do the storage resources. Comment out for now 20180813, until we figure out how to do it.
	
	var( 
		storageid int64
		srquota sql.NullInt64
		srname, srpath, srunit sql.NullString
	)
	
	if unit == "cms" {
		rows, err = DBtx.Query(`select sr.storageid, sr.name, sr.default_path, sr.default_quota, sr.default_unit from storage_resources as sr`)
		if err != nil {
			defer log.WithFields(QueryFields(r, startTime)).Error(err)
			fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
			return
		}
		
		type srinfo struct {
			SrID int64	
			SrName string
			SrPath string
			SrQuota int64
			SrUnit string
		}
		var tmpsr srinfo
		var sr []srinfo
		
		for rows.Next() {
			
			
			rows.Scan(&storageid, &srname, &srpath, &srquota, &srunit)
			if ! srunit.Valid {
				srunit.Valid = true
				srunit.String = "B" // if not default unit, set a default of bytes
				
			}
			if srname.Valid {
				tmpsr.SrID = storageid	
				tmpsr.SrName = srname.String
				tmpsr.SrPath = srpath.String
				tmpsr.SrQuota = srquota.Int64
				tmpsr.SrUnit = srunit.String
				sr = append(sr,tmpsr)	
			}
		}
		rows.Close()

		for isr := 0; isr<len(sr); isr++ {
			
			q.Set("resourcename", sr[isr].SrName)
			q.Set("path", sr[isr].SrPath + "/" + uName)
			q.Set("isGroup", "false")
			q.Set("valid_until", "")
			q.Set("quota", strconv.FormatInt(sr[isr].SrQuota, 10))
			q.Set("quota_unit", sr[isr].SrUnit)
			R.URL.RawQuery = q.Encode()

			duplicateCountRef ++
			var quotaCount int
			err = DBtx.QueryRow("select count(*) from storage_quota where path = $1", q["path"][0]).Scan(&quotaCount)
			if err != nil {
				defer log.WithFields(QueryFields(r, startTime)).Error(err)
				fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
				return
			}
			if quotaCount == 0 {
				DBtx.Savepoint("setUserStorageQuota_" + sr[isr].SrName)
				setUserStorageQuotaLegacy(w,R)
				if !DBtx.Complete() {
					log.WithFields(QueryFields(r, startTime)).Error("setUserStorageQuota on  " + sr[isr].SrName + "  failed: " + DBtx.Error().Error() )
					fmt.Fprintf(w, "{ \"ferry_error\": \"setUserStorageQuota for " + sr[isr].SrName + " failed. Last DB error: " + DBtx.Error().Error() + ". Rolling back transaction.\" }")
					return
				}
			} else {
				duplicateCount ++
			}
		}
	}
	//
	
	if duplicateCount == duplicateCountRef {
		fmt.Fprintf(w, "{ \"ferry_status\": \"User already belongs to the experiment.\" }")
	} else {
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		fmt.Fprintf(w, "{ \"ferry_status\": \"success\" }")

		DBtx.Commit(key)
	}
}

func setUserExperimentFQANLegacy(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	uName := strings.TrimSpace(q.Get("username"))
	unitName := strings.TrimSpace(q.Get("unitname"))
	fqan := strings.TrimSpace(q.Get("fqan"))

	if uName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No username specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No username specified.\" }")
		return
	}
	if unitName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No unitname specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No unitname specified.\" }")
		return
	}
	if fqan == "" {
		if strings.TrimSpace(q.Get("role")) != "" {
			fqan = "%Role=" + strings.TrimSpace(q.Get("role")) + "%"
		} else {
			log.WithFields(QueryFields(r, startTime)).Error("No role or fqan specified in http query.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"No role or fqan specified.\" }")
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

	var uid, unitid int
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

	var hasCert bool
	queryerr = DBtx.QueryRow(`select count(*) > 0 from affiliation_unit_user_certificate as ac
							   join user_certificates as uc on ac.dnid = uc.dnid
							   where uid = $1 and unitid = $2`, uid, unitid).Scan(&hasCert)
	switch {
	case queryerr == nil:
		if !hasCert {
			log.WithFields(QueryFields(r, startTime)).Error("User is not member of affiliation unit.")
			if cKey != 0 {
				fmt.Fprintf(w,"{ \"ferry_error\": \"User is not member of affiliation unit.\" }")
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

	rows, queryerr := DBtx.Query(`select fqanid from 
								  grid_fqan as gf join
								  affiliation_units as au on gf.unitid=au.unitid
								  where au.name=$1 and gf.fqan like $2`,unitName, fqan)
	if queryerr != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error during query:" + queryerr.Error())
		if cKey != 0 {
			fmt.Fprintf(w,"{ \"ferry_error\": \"Error during DB query; check logs.\" }")
		}
		return
	}

	var fqanids []int
	for rows.Next() {
		var fqanid int
		err = rows.Scan(&fqanid)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error(err.Error())
			fmt.Fprintf(w, "{ \"ferry_error\": \"Something went wrong.\" }")
			return
		}
		fqanids = append(fqanids, fqanid)
	}
	rows.Close()
	if len(fqanids) == 0 {
		log.WithFields(QueryFields(r, startTime)).Error("No FQANs found for this query.")
		if cKey != 0 {
			fmt.Fprintf(w,"{ \"ferry_error\": \"No FQANs found for this query.\" }")
		}
		return
	}

	var duplicate int
	for _, fqanid := range fqanids {
		DBtx.Savepoint("INSERT_" + strconv.Itoa(fqanid))
		_, err = DBtx.Exec(`insert into grid_access (uid, fqanid, is_superuser, is_banned, last_updated) values($1, $2, false, false, NOW())`, uid, fqanid)
		if err != nil {
			if strings.Contains(err.Error(), `duplicate key value violates unique constraint`) {
				DBtx.RollbackToSavepoint("INSERT_" + strconv.Itoa(fqanid))
				duplicate ++
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
				} else {
					log.WithFields(QueryFields(r, startTime)).Error(err.Error())
					if cKey != 0 {
						fmt.Fprintf(w, "{ \"ferry_error\": \"Something went wrong.\" }")
					}
				}
				return
			}
		}
	}

	if len(fqanids) == duplicate {
		if cKey != 0 {
			log.WithFields(QueryFields(r, startTime)).Error("This association already exists.")
			fmt.Fprintf(w, "{ \"ferry_status\": \"This association already exists.\" }")
		}
		return
	} else {
		if cKey != 0 {
			log.WithFields(QueryFields(r, startTime)).Info("Success!")
			fmt.Fprintf(w, "{ \"ferry_status\": \"success\" }")
		}
	}
	
	DBtx.Commit(cKey)
}

func getUserCertificateDNsLegacy(w http.ResponseWriter, r *http.Request) {
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
		type jsonstatus struct {
			Status []string `json:"ferry_status"`
		}
		var queryErr jsonerror
		var queryStatus jsonstatus
		if !userExists && uname != "%" {
			queryErr.Error = append(queryErr.Error, "User does not exist.")
			log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
		}
		if !exptExists && expt != "%" {
			queryErr.Error = append(queryErr.Error, "Experiment does not exist.")
			log.WithFields(QueryFields(r, startTime)).Error("Experiment does not exist.")
		}
		if userExists && exptExists {
			queryStatus.Status = append(queryErr.Error, "User does not have any certificates registered.")
			log.WithFields(QueryFields(r, startTime)).Info("User does not have any certificates registered.")
		}
		if len(queryErr.Error) > 0 {
			output = queryErr
		} else {
			output = queryStatus
		}
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

func getAllUsersCertificateDNsLegacy(w http.ResponseWriter, r *http.Request) {
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

func setUserInfoLegacy(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	var fName, status, gAccount, eDate sql.NullString

	uName := strings.TrimSpace(q.Get("username"))
	fName.String = strings.TrimSpace(q.Get("fullname"))
	status.String = strings.TrimSpace(q.Get("status"))
	gAccount.String = strings.TrimSpace(q.Get("groupaccount"))
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
	if gAccount.String == "" {
		gAccount.Valid = false
	} else {
		_, err := strconv.ParseBool(gAccount.String)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Invalid groupaccount specified in http query.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"Invalid groupaccount specified. Should be true or false.\" }")
			return
		}
		gAccount.Valid = true
	}
	if eDate.String == "" {
		eDate.Valid = false
	} else if strings.ToLower(eDate.String) == "null" {
		eDate.Valid = false
	} else {
		eDate.String = fmt.Sprintf("'%s'", eDate.String)
		eDate.Valid = true
	}

	if fName.String == "" && status.String == "" && gAccount.String == "" && eDate.String == "" {
		log.WithFields(QueryFields(r, startTime)).Error("Not enough arguments.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"Not enough arguments.\" }")
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
	query := `update users set 	full_name = coalesce($2, full_name),
								status = coalesce($3, status),
								is_groupaccount = coalesce($4, is_groupaccount),
								expiration_date = coalesce($5, expiration_date),
								last_updated = NOW()
			  where uid = $1`
	if strings.ToLower(eDate.String) == "null" {
		query = strings.Replace(query, "coalesce($5, expiration_date)", "$5", 1)
	}
	print(query)
	_, err = DBtx.Exec(query, uidint, fName, status, gAccount, eDate)

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

func getUserExternalAffiliationAttributesLegacy(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	user := strings.TrimSpace(q.Get("username"))

	if user == "" {
		user = "%"
	}
	lastupdate, parserr := stringToParsedTime(strings.TrimSpace(q.Get("last_updated")))
	if parserr != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error parsing provided update time: " + parserr.Error())
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error parsing last_updated time. Check ferry logs. If provided, it should be an integer representing an epoch time.\"}")
		return
	}

	rows, err := DBptr.Query(`select uname, attribute, value, user_exists from
							 (select 1 as key, a.attribute, a.value, u.uname, a.last_updated from external_affiliation_attribute as a 
							  left join users as u on a.uid = u.uid where uname like $1) as t right join
							 (select 1 as key, $1 in (select uname from users) as user_exists) as c on t.key = c.key where t.last_updated>=$2 or $2 is null;`, user, lastupdate)

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
	Out := make(map[string][]jsonentry)

	for rows.Next() {
		var tmpUname, tmpAttribute, tmpValue sql.NullString
		rows.Scan(&tmpUname, &tmpAttribute, &tmpValue, &userExists)

		if tmpAttribute.Valid {
			Entry.Attribute = tmpAttribute.String
			Entry.Value = tmpValue.String
			Out[tmpUname.String] = append(Out[tmpUname.String], Entry)
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

func getStorageQuotasLegacy(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	type jsonerror struct {
		Error []string `json:"ferry_error"`
	}
	var inputErr jsonerror

	user := q.Get("username")
	group := q.Get("groupname")
	resource := q.Get("resourcename")

	if user == "" {
		user = "%"
	}
	if group == "" {
		group = "%"
	}
	if resource == "" {
		resource = "%"
	}
	
	lastupdate, parserr := stringToParsedTime(strings.TrimSpace(q.Get("last_updated")))
	if parserr != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error parsing provided update time: " + parserr.Error())
		inputErr.Error = append(inputErr.Error, "Error parsing last_updated time. Check ferry logs. If provided, it should be an integer representing an epoch time.")
	}
	
	if len(inputErr.Error) > 0 {
		jsonout, err := json.Marshal(inputErr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error(err)
		}
		fmt.Fprintf(w, string(jsonout))
		return
	}

	rows, err := DBptr.Query(`select uname, gname, rname, path, value, unit, valid_until, user_exists, group_exists, resource_exists from
							(select 1 as key, u.uname as uname, g.name as gname, sr.name as rname, sr.type, sq.* from
								storage_quota as sq left join
								users as u on sq.uid = u.uid left join
								groups as g on sq.groupid = g.groupid left join
								storage_resources as sr on sq.storageid = sr.storageid
								where (u.uname like $1 or $1 = '%') and (g.name like $2 or $2 = '%') and (sr.name like $3 or $3 = '%')
								and (valid_until is null or valid_until >= NOW()) and (sq.last_updated >= $4 or $4 is null)
							  	order by uname asc, gname asc, rname asc, valid_until desc
							) as t 
							right join (
								select 1 as key,
								$1 in (select uname from users) as user_exists,
								$2 in (select name from groups) as group_exists,
								$3 in (select name from storage_resources) as resource_exists
							) as c on t.key = c.key;`, user, group, resource, lastupdate)

	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Error(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
		return
	}
	defer rows.Close()

	var userExists bool
	var groupExists bool
	var resourceExists bool

	type jsonquota struct {
		Path  string `json:"path"`
		Value string `json:"value"`
		Unit  string `json:"unit"`
		Until string `json:"validuntil"`
	}
	type outmap struct {
		Users  map[string]map[string]jsonquota `json:"user_quotas"`
		Groups map[string]map[string]jsonquota `json:"group_quotas"`
	}
	outMap := outmap{make(map[string]map[string]jsonquota), make(map[string]map[string]jsonquota)}

	for rows.Next() {
		var tmpUname, tmpGname, tmpRname, tmpPath, tmpValue, tmpUnit, tmpUntil sql.NullString
		rows.Scan(&tmpUname, &tmpGname, &tmpRname, &tmpPath, &tmpValue, &tmpUnit, &tmpUntil, &userExists, &groupExists, &resourceExists)

		if tmpUname.Valid {
			if _, ok := outMap.Users[tmpUname.String]; !ok {
				outMap.Users[tmpUname.String] = make(map[string]jsonquota)
			}
			outMap.Users[tmpUname.String][tmpRname.String] =
			jsonquota{tmpPath.String, tmpValue.String, tmpUnit.String, tmpUntil.String}
		}
		if tmpGname.Valid {
			if _, ok := outMap.Groups[tmpGname.String]; !ok {
				outMap.Groups[tmpGname.String] = make(map[string]jsonquota)
			}
			outMap.Groups[tmpGname.String][tmpRname.String] =
			jsonquota{tmpPath.String, tmpValue.String, tmpUnit.String, tmpUntil.String}
		}
	}

	var output interface{}
	if len(outMap.Users) == 0 && len(outMap.Groups) == 0 {
		var queryErr jsonerror
		if !userExists && user != "%" {
			log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
			queryErr.Error = append(queryErr.Error, "User does not exist.")
		} else if !groupExists && group != "%" {
			log.WithFields(QueryFields(r, startTime)).Error("Group does not exist.")
			queryErr.Error = append(queryErr.Error, "Group does not exist.")
		} else if !resourceExists && resource != "%" {
			log.WithFields(QueryFields(r, startTime)).Error("Storage resource does not exist.")
			queryErr.Error = append(queryErr.Error, "Storage resource does not exist.")
		} else {
			log.WithFields(QueryFields(r, startTime)).Error("No storage quotas were found for this query.")
			queryErr.Error = append(queryErr.Error, "No storage quotas were found for this query.")
		}
		output = queryErr
	} else {
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		output = outMap
	}
	jsonout, err := json.Marshal(output)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
	}
	fmt.Fprintf(w, string(jsonout))
}

func getUserFQANsLegacy(w http.ResponseWriter, r *http.Request) {
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

func getGroupMembersLegacy(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query() 
	groupname := q.Get("groupname")
	grouptype := q.Get("grouptype")
	//	//should be a bool

	if groupname == "" {	
		log.WithFields(QueryFields(r, startTime)).Print("No groupname specified.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No groupname specified\" }")
		return
	}
	if grouptype == "" {	
		grouptype = "UnixGroup"
	}
	
	getLeaders := false
	gl := q.Get("return_leaders")
	if gl != "" {
		getl,glerr := strconv.ParseBool(gl)	
		if glerr != nil {
			log.WithFields(QueryFields(r, startTime)).Print("Invalid value of return_leaders: " + gl + ". Must be true or false.")
			fmt.Fprintf(w,"{ \"ferry_error\": \"Invalid value for return_leaders. Must be true or false\" }")		
			return
		} else {
			getLeaders = getl
		}
	}
	
	lastupdate, parserr :=  stringToParsedTime(strings.TrimSpace(q.Get("last_updated")))
	if parserr != nil {
                log.WithFields(QueryFields(r, startTime)).Error("Error parsing provided update time: " + parserr.Error())
                fmt.Fprintf(w,"{ \"ferry_error\": \"Error parsing last_updated time. Check ferry logs. If provided, it should be an integer representing an epoch time.\"}")
                return
    }
	
	type jsonout struct {
		UID int `json:"uid"`
		Uname string `json:"username"`
		Leader string `json:"is_leader,omitempty"`
	}
	var grpid,tmpuid int
	var tmpuname string
	var tmpleader bool
	var tmpout jsonout
	var Out []jsonout

	err := DBptr.QueryRow(`select groupid from groups where (name, type) = ($1, $2)`, groupname, grouptype).Scan(&grpid)
	switch {
	case err == sql.ErrNoRows:
		log.WithFields(QueryFields(r, startTime)).Print("Group does not exist.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"Group does not exist.\" }")
		return

	case err != nil && strings.Contains(err.Error(), `invalid input value for enum`):
		log.WithFields(QueryFields(r, startTime)).Print("Invalid group type.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"Invalid group type.\" }")
		return
		
	case err != nil:
		log.WithFields(QueryFields(r, startTime)).Print("Group ID query error: " + err.Error())
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error in DB query.\" }")
		return
		
	default:
		rows, err := DBptr.Query(`select users.uname, users.uid, user_group.is_leader from user_group join users on users.uid=user_group.uid where user_group.groupid=$1 and (user_group.last_updated>=$2 or $2 is null)`, grpid, lastupdate)
		if err != nil {	
			log.WithFields(QueryFields(r, startTime)).Print("Database query error: " + err.Error())
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w,"{ \"ferry_error\": \"Error in DB query.\" }")		
			return
		}
		
		defer rows.Close()
		for rows.Next() {
			rows.Scan(&tmpuname,&tmpuid,&tmpleader)
			tmpout.Uname = tmpuname
			tmpout.UID = tmpuid
			if getLeaders == true {
				tmpout.Leader = strconv.FormatBool(tmpleader)
			}
			Out = append(Out,tmpout)
		}
		
		var output interface{}
		if len(Out) == 0 {
			type jsonerror struct {
				Error string `json:"ferry_error"`
			}
			var queryErr []jsonerror
			queryErr = append(queryErr, jsonerror{"This group has no members."})
			log.WithFields(QueryFields(r, startTime)).Error("Group has no members")
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
}

func IsUserMemberOfGroupLegacy(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	type jsonerror struct {
		Error string `json:"ferry_error"`
	}
	var inputErr []jsonerror

	user := q.Get("username")
	group := q.Get("groupname")
	gtype := q.Get("grouptype")

	if user == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No username specified in http query.")
		inputErr = append(inputErr, jsonerror{"No username specified."})
	}
	if group == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No groupname specified in http query.")
		inputErr = append(inputErr, jsonerror{"No groupname specified."})
	}
	if gtype == "" {	
		gtype = "UnixGroup"
	}

	if len(inputErr) > 0 {
		jsonout, err := json.Marshal(inputErr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error(err)
		}
		fmt.Fprintf(w, string(jsonout))
		return
	}

	typeExists := true
	rows, err := DBptr.Query(`select member, user_exists, group_exists from (
								select 1 as key, (
									(select uid from users where uname = $1),
									(select groupid from groups where (name, type) = ($2, $3))
								) in (select uid, groupid from user_group) as member
							) as t right join (
								select 1 as key, $1 in (select uname from users) as user_exists,
												 $2 in (select name from groups) as group_exists
							) as c on t.key = c.key;`, user, group, gtype)
	if err != nil {
		if strings.Contains(err.Error(), `invalid input value for enum`){
			typeExists = false
		} else {
			defer log.WithFields(QueryFields(r, startTime)).Error(err)
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w,"{ \"ferry_error\": \"Error in DB query.\" }")
			return
		}
	} else {
		defer rows.Close()
	}

	var userExists, groupExists bool

	type jsonentry struct {
		Member  bool `json:"member"`
	}
	var Out jsonentry

	var tmpMember sql.NullBool
	if rows != nil {
		for rows.Next() {
			rows.Scan(&tmpMember, &userExists, &groupExists)
			Out.Member = tmpMember.Bool
		}
	}

	var output interface{}
	if !tmpMember.Valid {
		var queryErr []jsonerror
		if !typeExists {
			log.WithFields(QueryFields(r, startTime)).Error("Invalid group type.")
			queryErr = append(queryErr, jsonerror{"Invalid group type."})
		} else {
			if !userExists {
				log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
				queryErr = append(queryErr, jsonerror{"User does not exist."})
			}
			if !groupExists {
				log.WithFields(QueryFields(r, startTime)).Error("Group does not exist.")
				queryErr = append(queryErr, jsonerror{"Group does not exist."})
			}
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

func IsUserLeaderOfGroupLegacy(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	uName := q.Get("username")
	groupname := q.Get("groupname")
	grouptype := q.Get("grouptype")
	
	if groupname == "" {	
		log.WithFields(QueryFields(r, startTime)).Print("No groupname specified.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No groupname specified\" }")
		return
	}
	if uName == "" {	
		log.WithFields(QueryFields(r, startTime)).Print("No username specified.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No username specified\" }")
		return
	}
	if grouptype == "" {
		grouptype = "UnixGroup"
	}
	var groupId, uId int
	grouperr := DBptr.QueryRow(`select groupid from groups where (name, type) = ($1, $2)`, groupname, grouptype).Scan(&groupId)
	switch {
	case grouperr == sql.ErrNoRows:
		log.WithFields(QueryFields(r, startTime)).Print("Group " + groupname + " does not exist.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"Group " + groupname + " does not exist.\" }")
		return
	case grouperr != nil && strings.Contains(grouperr.Error(), "invalid input value for enum"):
		log.WithFields(QueryFields(r, startTime)).Print("Invalid group type.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"Invalid group type.\" }")
		return
	case grouperr != nil:
		log.WithFields(QueryFields(r, startTime)).Print("Group ID query error: " + grouperr.Error())
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error in DB query.\" }")
		return
	default:
		// group is good, now make sure the user exists
		usererr := DBptr.QueryRow(`select uid from users where uname=$1`,uName).Scan(&uId)
		switch {
		case usererr == sql.ErrNoRows:
			log.WithFields(QueryFields(r, startTime)).Print("User " + uName + " does not exist.")
			fmt.Fprintf(w,"{ \"ferry_error\": \"User " + uName + " does not exist.\" }")
			return
		case usererr != nil:
			log.WithFields(QueryFields(r, startTime)).Print("User ID query error: " + usererr.Error())
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w,"{ \"ferry_error\": \"Error in DB query.\" }")
			return
		default:
			var isLeader bool
			checkerr := DBptr.QueryRow(`select is_leader from user_group as ug join users on users.uid=ug.uid join groups on groups.groupid=ug.groupid where users.uname=$1 and groups.name=$2`,uName,groupname).Scan(&isLeader)
			leaderstr := strconv.FormatBool(isLeader)
			switch {
			case checkerr != nil && checkerr != sql.ErrNoRows:
				log.WithFields(QueryFields(r, startTime)).Print("Group leader query error: " + checkerr.Error())
				w.WriteHeader(http.StatusNotFound)
				fmt.Fprintf(w,"{ \"ferry_error\": \"Error in DB query.\" }")
				return	
			default:
				w.WriteHeader(http.StatusOK)
				log.WithFields(QueryFields(r, startTime)).Print(uName + " is a leader of " + groupname + ": " + leaderstr)
				fmt.Fprintf(w,"{ \"leader\": \"" + leaderstr + "\" }")
				return
			}
		}
	}					
}

func setGroupLeaderLegacy(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	uName := q.Get("username")
	groupname := q.Get("groupname")
	grouptype := q.Get("grouptype")
	
	if groupname == "" {	
		log.WithFields(QueryFields(r, startTime)).Print("No groupname specified.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No groupname specified\" }")
		return
	}
	if grouptype == "" {	
		log.WithFields(QueryFields(r, startTime)).Print("No grouptype specified.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No grouptype specified\" }")
		return
	}
	if uName == "" {	
		log.WithFields(QueryFields(r, startTime)).Print("No username specified.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No username specified\" }")
		return
	}

	//requires authorization
	authorized,authout := authorize(r)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w,"{ \"ferry_error\": \"" + authout + "not authorized.\" }")
		return
	}
	
	DBtx, cKey, err := LoadTransaction(r, DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Print("Error starting DB transaction: " + err.Error())
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error starting database transaction.\" }")
		return
	}
	defer DBtx.Rollback(cKey)
	
	var groupId, uId int
	grouperr := DBptr.QueryRow(`select groupid from groups where (name, type) = ($1, $2)`, groupname, grouptype).Scan(&groupId)
	switch {
	case grouperr == sql.ErrNoRows:
		log.WithFields(QueryFields(r, startTime)).Print("Group " + groupname + " does not exist.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"Group " + groupname + " does not exist.\" }")
		return
	case grouperr != nil && strings.Contains(grouperr.Error(), "invalid input value for enum"):
		log.WithFields(QueryFields(r, startTime)).Print("Invalid group type.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"Invalid group type.\" }")
		return
	case grouperr != nil:
		log.WithFields(QueryFields(r, startTime)).Print("Group ID query error: " + grouperr.Error())
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error in DB query.\" }")
		return
	default:
		// group is good, now make sure the user exists
		usererr := DBptr.QueryRow(`select uid from users where uname=$1`,uName).Scan(&uId)
		switch {
		case usererr == sql.ErrNoRows:
			log.WithFields(QueryFields(r, startTime)).Print("User " + uName + " does not exist.")
			fmt.Fprintf(w,"{ \"ferry_error\": \"User " + uName + " does not exist.\" }")
			return
		case usererr != nil:
			log.WithFields(QueryFields(r, startTime)).Print("User ID query error: " + usererr.Error())
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w,"{ \"ferry_error\": \"Error in DB query.\" }")
			return
		default:
			setstr := fmt.Sprintf(`do $$
								   declare
										c_groupid constant int := '%d';
										c_uid constant int := '%d';
								   begin
										if exists (select uid, groupid from user_group where groupid = c_groupid and uid = c_uid) then
											update user_group set is_leader = true, last_updated = NOW() where groupid = c_groupid and uid = c_uid;
										else
											insert into user_group (uid, groupid, is_leader) values(c_uid, c_groupid, true);
										end if ;
								   end $$;`, groupId, uId)
			stmt, err := DBtx.Prepare(setstr)
			if err != nil {
				log.WithFields(QueryFields(r, startTime)).Print("Error preparing DB command: " + err.Error())
				w.WriteHeader(http.StatusNotFound)
				fmt.Fprintf(w,"{ \"ferry_error\": \"Error preparing database command.\" }")
				return
			}
			//run said statement and check errors
			_, err = stmt.Exec()
			if err != nil {
				w.WriteHeader(http.StatusNotFound)
				log.WithFields(QueryFields(r, startTime)).Print("Error setting " + uName + " leader of " + groupname + ": " + err.Error())
				fmt.Fprintf(w,"{ \"ferry_error\": \"Error executing DB update.\" }")		
				return
			} else {
				// error is nil, so it's a success. Commit the transaction and return success.
				DBtx.Commit(cKey)
				w.WriteHeader(http.StatusOK)
				log.WithFields(QueryFields(r, startTime)).Print("Successfully set " + uName + " as leader of " + groupname + ".")
				if cKey != 0 {
					fmt.Fprintf(w,"{ \"ferry_status\": \"success\" }")
				}
			}
			return
		}
	}
}

func removeGroupLeaderLegacy(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	uName := q.Get("username")
	groupname := q.Get("groupname")
	grouptype := q.Get("grouptype")
	
	if groupname == "" {	
		log.WithFields(QueryFields(r, startTime)).Print("No groupname specified.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No groupname specified\" }")
		return
	}
	if grouptype == "" {	
		log.WithFields(QueryFields(r, startTime)).Print("No grouptype specified.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No grouptype specified\" }")
		return
	}
	if uName == "" {	
		log.WithFields(QueryFields(r, startTime)).Print("No username specified.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No username specified\" }")
		return
	}

	//requires authorization
	authorized,authout := authorize(r)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w,"{ \"ferry_error\": \"" + authout + "not authorized.\" }")
		return
	}
	
	DBtx, cKey, err := LoadTransaction(r, DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Print("Error starting DB transaction: " + err.Error())
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error starting database transaction.\" }")
		return
	}
	defer DBtx.Rollback(cKey)
	
	var groupId, uId int
	grouperr := DBptr.QueryRow(`select groupid from groups where (name, type) = ($1, $2)`, groupname, grouptype).Scan(&groupId)
	switch {
	case grouperr == sql.ErrNoRows:
		log.WithFields(QueryFields(r, startTime)).Print("Group " + groupname + " does not exist.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"Group " + groupname + " does not exist.\" }")
		return
	case grouperr != nil && strings.Contains(grouperr.Error(), "invalid input value for enum"):
		log.WithFields(QueryFields(r, startTime)).Print("Invalid group type.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"Invalid group type.\" }")
		return
	case grouperr != nil:
		log.WithFields(QueryFields(r, startTime)).Print("Group ID query error: " + grouperr.Error())
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error in DB query.\" }")
		return
	default:
		// group is good, now make sure the user exists
		usererr := DBptr.QueryRow(`select uid from users where uname=$1`,uName).Scan(&uId)
		switch {
		case usererr == sql.ErrNoRows:
			log.WithFields(QueryFields(r, startTime)).Print("User " + uName + " does not exist.")
			fmt.Fprintf(w,"{ \"ferry_error\": \"User " + uName + " does not exist.\" }")
			return
		case usererr != nil:
			log.WithFields(QueryFields(r, startTime)).Print("User ID query error: " + usererr.Error())
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w,"{ \"ferry_error\": \"Error in DB query.\" }")
			return
		default:
			setstr := fmt.Sprintf(`do $$
								   declare
										c_groupid constant int := '%d';
										c_uid constant int := '%d';
								   begin
										if exists (select * from user_group where groupid = c_groupid and uid = c_uid and is_leader = true) then
											update user_group set is_leader = false, last_updated = NOW() where groupid = c_groupid and uid = c_uid;
										else
											raise 'User is not a leader of this group.';
										end if ;
								   end $$;`, groupId, uId)
			stmt, err := DBtx.Prepare(setstr)
			if err != nil {
				log.WithFields(QueryFields(r, startTime)).Print("Error preparing DB command: " + err.Error())
				w.WriteHeader(http.StatusNotFound)
				fmt.Fprintf(w,"{ \"ferry_error\": \"Error preparing database command.\" }")
				return
			}
			//run said statement and check errors
			_, err = stmt.Exec()
			DBtx.err = err
			if err != nil {
				if strings.Contains(err.Error(), "User is not a leader of this group.") {
					log.WithFields(QueryFields(r, startTime)).Error("User is not a leader of this group.")
					if cKey != 0 {
						fmt.Fprintf(w,"{ \"ferry_error\": \"User is not a leader of this group.\" }")
					}
				} else {
					w.WriteHeader(http.StatusNotFound)
					log.WithFields(QueryFields(r, startTime)).Print("Error setting " + uName + " leader of " + groupname + ": " + err.Error())
					fmt.Fprintf(w,"{ \"ferry_error\": \"Error executing DB update.\" }")
					return
				}
			} else {
				// error is nil, so it's a success. Commit the transaction and return success.
				DBtx.Commit(cKey)
				w.WriteHeader(http.StatusOK)
				log.WithFields(QueryFields(r, startTime)).Print("Successfully set " + uName + " as leader of " + groupname + ".")
				if cKey != 0 {
					fmt.Fprintf(w,"{ \"ferry_status\": \"success\" }")
				}
			}
			return
		}
	}
}

func getUserGroupsLegacy(w http.ResponseWriter, r *http.Request) {
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

func addUserToGroupLegacy(w http.ResponseWriter, r *http.Request) {
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

func removeUserFromGroupLegacy(w http.ResponseWriter, r *http.Request) {
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

func setUserShellAndHomeDirLegacy(w http.ResponseWriter, r *http.Request) {
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

func getUserShellAndHomeDirLegacy(w http.ResponseWriter, r *http.Request) {
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

func setUserShellLegacy(w http.ResponseWriter, r *http.Request) {
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

func getUserStorageQuotaLegacy(w http.ResponseWriter, r *http.Request) {
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

func createUserLegacy(w http.ResponseWriter, r *http.Request) {
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

func getUserUnameLegacy(w http.ResponseWriter, r *http.Request) {
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

func getUserUIDLegacy(w http.ResponseWriter, r *http.Request) {
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

func getMemberAffiliationsLegacy(w http.ResponseWriter, r *http.Request) {
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

func getAllUsersLegacy(w http.ResponseWriter, r *http.Request) {
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

func getAllUsersFQANsLegacy(w http.ResponseWriter, r *http.Request) {
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

func createGroupLegacy(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	gName := q.Get("groupname")
	gType := q.Get("grouptype")
	var gid sql.NullString

	if gName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No groupname specified in http query.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No groupname specified.\" }")
		return
	}
	if gType == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No grouptype specified in http query.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No grouptype specified.\" }")
		return
	}
	if q.Get("gid") != "" {
		gid.Scan(q.Get("gid"))
	}

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

	_, err = DBtx.Exec("insert into groups (gid, name, type, last_updated) values ($1, $2, $3, NOW())", gid, gName, gType)
	if err == nil {
		DBtx.Commit(cKey)
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		fmt.Fprintf(w,"{ \"ferry_status\": \"success\" }")
	} else {
		if strings.Contains(err.Error(), `invalid input value for enum groups_group_type`) {
			log.WithFields(QueryFields(r, startTime)).Error("Invalid grouptype specified in http query.")
			fmt.Fprintf(w,"{ \"ferry_error\": \"Invalid grouptype specified in http query.\" }")
		} else if strings.Contains(err.Error(), `duplicate key value violates unique constraint "idx_groups_gid"`) {
			log.WithFields(QueryFields(r, startTime)).Error("GID already exists.")
			fmt.Fprintf(w,"{ \"ferry_error\": \"GID already exists.\" }")
		} else if strings.Contains(err.Error(), `duplicate key value violates unique constraint`) {
			log.WithFields(QueryFields(r, startTime)).Error("Group already exists.")
			fmt.Fprintf(w,"{ \"ferry_error\": \"Group already exists.\" }")
		} else {
			log.WithFields(QueryFields(r, startTime)).Error(err.Error())
			fmt.Fprintf(w,"{ \"ferry_error\": \"Something went wrong.\" }")
		}
	}
}

func addGroupToUnitLegacy(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	groupname := strings.TrimSpace(q.Get("groupname"))
	grouptype := strings.TrimSpace(q.Get("grouptype"))
	unitName := strings.TrimSpace(q.Get("unitname"))
	isPrimarystr := strings.TrimSpace(q.Get("is_primary"))
	isPrimary := false
//if is_primary is not set in the query, assume it is false. Otherwise take the value from the query
	if isPrimarystr != "" {
		var converr error
		isPrimary, converr = strconv.ParseBool(isPrimarystr)	
		if converr != nil {
			log.WithFields(QueryFields(r, startTime)).Print("Invalid value of is_primary (Must be true or false).")
			fmt.Fprintf(w,"{ \"ferry_error\": \"Invalid value for is_primary (Must be true or false).\" }")
			return
		}
	}
	if groupname == "" {	
		log.WithFields(QueryFields(r, startTime)).Print("No groupname specified.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No groupname specified\" }")
		return
	}
	if grouptype == "" {	
		log.WithFields(QueryFields(r, startTime)).Print("No grouptype specified.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No grouptype specified\" }")
		return
	}
	if unitName == "" {	
		log.WithFields(QueryFields(r, startTime)).Print("No unitname specified.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No unitname specified\" }")
		return
	}
	
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

	err = addGroupToUnitDB(DBtx, groupname, grouptype, unitName, isPrimary)
	
	if err != nil {
		DBtx.Report(err.Error())
		if strings.Contains(err.Error(), `Group and unit combination already in DB`) {
			log.WithFields(QueryFields(r, startTime)).Print("Error adding " + groupname + " to " + unitName + "groups: " + err.Error())
			if cKey != 0 {
				fmt.Fprintf(w,"{ \"ferry_error\": \"Group already belongs to unit.\" }")
			}
		} else if strings.Contains(err.Error(), `unq_affiliation_unit_group_unitid_is_primary`) {
			log.WithFields(QueryFields(r, startTime)).Print("Error adding " + groupname + " to " + unitName + "groups: " + err.Error())
			if cKey != 0 {
				fmt.Fprintf(w,"{ \"ferry_error\": \"Unit can not have more then one primary group.\" }")
			}
		} else if strings.Contains(err.Error(), `invalid input value for enum`) {
			log.WithFields(QueryFields(r, startTime)).Print("Error adding " + groupname + " to " + unitName + "groups: " + err.Error())
			if cKey != 0 {
				fmt.Fprintf(w,"{ \"ferry_error\": \"Invalid group type.\" }")
			}
		} else {
			log.WithFields(QueryFields(r, startTime)).Print("Error adding " + groupname + " to " + unitName + "groups: " + err.Error())
			if cKey != 0 {
				fmt.Fprintf(w,"{ \"ferry_error\": \"Error executing DB insert.\" }")
			}
		}
		//				DBtx.Rollback(cKey) // COMMENT 2018-04-04
		return
	} else {
		w.WriteHeader(http.StatusOK)
		log.WithFields(QueryFields(r, startTime)).Print("Successfully added " + groupname + " to affiliation_unit_groups.")
		if cKey != 0 {
			DBtx.Commit(cKey)
			fmt.Fprintf(w,"{ \"ferry_status\": \"success\" }")
		}
	}
	return	
	
	//	} //end first switch COMMENT 2018-04-04
}

func addGroupToUnitDB(tx *Transaction, groupname, grouptype, unitName string, isPrimary bool) (error) {

	var unitId,groupId int
	checkerr := tx.tx.QueryRow(`select unitid from affiliation_units where name=$1`,unitName).Scan(&unitId)
	switch {
	case checkerr == sql.ErrNoRows:
//		log.WithFields(QueryFields(r, startTime)).Print("Affiliation unit " + unitName + " does not exist.")
	//	w.WriteHeader(http.StatusNotFound)
	//	fmt.Fprintf(w,"{ \"ferry_error\": \"Affiliation unit " + unitName + " does not exist.\" }")
		return checkerr
	case checkerr != nil:
//		log.WithFields(QueryFields(r, startTime)).Print("Affiliation unit query error: " + checkerr.Error())

		return checkerr
	default:
		grouperr := tx.tx.QueryRow(`select groupid from groups where name=$1 and type=$2`,groupname,grouptype).Scan(&groupId)
//		log.WithFields(QueryFields(r, startTime)).Print(" group ID = " + strconv.Itoa(groupId))
		switch {
		case grouperr == sql.ErrNoRows:
//			log.WithFields(QueryFields(r, startTime)).Print("Group " + groupname + " does not exist.")
//			w.WriteHeader(http.StatusNotFound)
//			fmt.Fprintf(w,"{ \"ferry_error\": \"Group " + groupname + " does not exist.\" }")
			return grouperr
		case grouperr != nil:
			return grouperr
		default:
			// OK, both group and unit exist. Let's get down to business. Check if it's already in affiliation_unit_groups
			
			// start a transaction
	//		DBtx, cKey, err := LoadTransaction(r, DBptr)
	//		if err != nil {
	//			log.WithFields(QueryFields(r, startTime)).Print("Error starting DB transaction: " + err.Error())
	//			w.WriteHeader(http.StatusNotFound)
	//			fmt.Fprintf(w,"{ \"ferry_error\": \"Error starting database transaction.\" }")
	//			return
	//		}
			
			addstr := fmt.Sprintf(`do $$ begin if exists (select groupid, unitid from affiliation_unit_group where groupid=%d and unitid=%d) then raise 'Group and unit combination already in DB.'; else 
insert into affiliation_unit_group (groupid, unitid, is_primary, last_updated) values (%d, %d, %t, NOW()); end if ; end $$;`, groupId, unitId, groupId, unitId, isPrimary)
			log.Print(addstr)
			stmt, err := tx.Prepare(addstr)
			if err != nil {
			//	log.WithFields(QueryFields(r, startTime)).Print("Error preparing DB command: " + err.Error())
			//	w.WriteHeader(http.StatusNotFound)
			//	fmt.Fprintf(w,"{ \"ferry_error\": \"Error preparing database command.\" }")
				//				DBtx.Rollback(cKey)
				return err
			}
			//run said statement and check errors
			_, err = stmt.Exec()
			defer stmt.Close()
			if err != nil {
//				if strings.Contains(err.Error(),`Group and unit combination already in DB`) {
//					log.WithFields(QueryFields(r, startTime)).Print("Error adding " + groupname + " to " + unitName + "groups: " + err.Error())
//				} else {
//					log.WithFields(QueryFields(r, startTime)).Print("Error adding " + groupname + " to " + unitName + "groups: " + err.Error())
//				}
				//				DBtx.Rollback(cKey)
				return err
			} else {
				// error is nil, so it's a success. Commit the transaction and return success.
				//				DBtx.Commit(cKey)
				
//				log.WithFields(QueryFields(r, startTime)).Print("Successfully added " + groupname + " to affiliation_unit_groups.")
				return nil	
			}
		}
	} //en
	
}

func setPrimaryStatusGroupLegacy(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	groupname := strings.TrimSpace(q.Get("groupname"))
	unitName := strings.TrimSpace(q.Get("unitname"))
	if groupname == "" {	
		log.WithFields(QueryFields(r, startTime)).Print("No groupname specified.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No groupname specified\" }")
		return
	}
	if unitName == "" {	
		log.WithFields(QueryFields(r, startTime)).Print("No unitname specified.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No unitname specified\" }")
		return
	}

	authorized,authout := authorize(r)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w,"{ \"ferry_error\": \"" + authout + "not authorized.\" }")
		return
	}
	
	DBtx, cKey, err := LoadTransaction(r, DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Print("Error starting DB transaction: " + err.Error())
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error starting database transaction.\" }")
		return
	}
	defer DBtx.Rollback(cKey)
	
	setstr := fmt.Sprintf(`do $FOO$
								declare grpid int;
								declare idunit int;
						   begin
								select groupid into grpid from groups where name = '%s' and type = 'UnixGroup';
								select unitid into idunit from affiliation_units where name = '%s';

								if grpid is null then
									raise 'Group does not exist.';
								elseif idunit is null then
									raise 'Unit does not exist.' ;
								else
									update affiliation_unit_group set is_primary = false, last_updated = NOW() where is_primary = true and unitid = idunit;
									update affiliation_unit_group set is_primary = true, last_updated = NOW() where groupid = grpid and unitid = idunit;
								end if ;
						   end $FOO$;`, groupname, unitName)
	stmt, err := DBtx.Prepare(setstr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Print("Error preparing DB command: " + err.Error())
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error preparing database command.\" }")
		return
	}
	//run said statement and check errors
	_, err = stmt.Exec()
//	_, err = DBtx.Exec(setstr,groupname,unitName)
	if err != nil {
		if strings.Contains(err.Error(),`Group does not exist`) {
			log.WithFields(QueryFields(r, startTime)).Print("Error adding " + groupname + " to " + unitName + "groups: " + err.Error())
			fmt.Fprintf(w,"{ \"ferry_error\": \"Group does not exist.\" }")
		} else if strings.Contains(err.Error(),`Unit does not exist`) {
			log.WithFields(QueryFields(r, startTime)).Print("Error adding " + groupname + " to " + unitName + "groups: " + err.Error())
			fmt.Fprintf(w,"{ \"ferry_error\": \"Unit does not exist.\" }")
		} else {
			w.WriteHeader(http.StatusNotFound)
			log.WithFields(QueryFields(r, startTime)).Print("Error adding " + groupname + " to " + unitName + "groups: " + err.Error())
			fmt.Fprintf(w,"{ \"ferry_error\": \"Error executing DB insert.\" }")		
		}
		stmt.Close()
		return
	} else {
		// error is nil, so it's a success. Commit the transaction and return success.
		DBtx.Commit(cKey)
		w.WriteHeader(http.StatusOK)
		log.WithFields(QueryFields(r, startTime)).Print("Successfully added " + groupname + " to affiliation_unit_groups.")
		fmt.Fprintf(w,"{ \"ferry_status\": \"success\" }")
	}
	stmt.Close()
	return
}

func getGroupUnitsLegacy(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	type jsonerror struct {
		Error string `json:"ferry_error"`
	}
	var inputErr []jsonerror

	group := q.Get("groupname")
	gtype := q.Get("grouptype")
	expOnly := false

	if group == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No group name specified in http query.")
		inputErr = append(inputErr, jsonerror{"No group name specified."})
	}
	if gtype == "" {
		gtype = "UnixGroup"
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

	rows, err := DBptr.Query(`select name, type, url, alternative_name from (
								select 1 as key, au.*, vu.url from
									affiliation_unit_group as ag left join
									groups as g on ag.groupid = g.groupid left join
									affiliation_units as au on ag.unitid = au.unitid left join
									voms_url as vu on au.unitid = vu.unitid
								where (g.name, g.type) = ($1, $2) and ((url is not null = $3) or not $3) and (vu.last_updated>=$4 or ag.last_updated>=$4 or $4 is null)
							) as t right join (
								select 1 as key, ($1, $2) in (select name, type from groups) as group_exists
							) as c on t.key = c.key;`, group, gtype, expOnly, lastupdate)
	if err != nil {
		if strings.Contains(err.Error(), "invalid input value for enum") {
			defer log.WithFields(QueryFields(r, startTime)).Error("Invalid group type.")
			fmt.Fprintf(w,"{ \"ferry_error\": \"Invalid group type.\" }")
		} else {
			defer log.WithFields(QueryFields(r, startTime)).Error(err)
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w,"{ \"ferry_error\": \"Error in DB query.\" }")
		}
		return
	}
	defer rows.Close()

	var groupExists bool

	type jsonentry struct {
		Unit  string `json:"unitname"`
		Type  string `json:"type"`
		Voms  string `json:"vomsurl"`
		Aname string `json:"alternativename"`
	}
	var Entry jsonentry
	var Out []jsonentry

	for rows.Next() {
		var tmpUnit, tmpType, tmpVoms, tmpAname sql.NullString
		rows.Scan(&tmpUnit, &tmpType, &tmpVoms, &tmpAname, &groupExists)

		if tmpUnit.Valid {
			Entry.Unit = tmpUnit.String
			Entry.Type = tmpType.String
			Entry.Voms = tmpVoms.String
			Entry.Aname = tmpAname.String
			Out = append(Out, Entry)
		}
	}

	var output interface{}
	if len(Out) == 0 {
		var queryErr []jsonerror
		if !groupExists {
			log.WithFields(QueryFields(r, startTime)).Error("Group does not exist.")
			queryErr = append(queryErr, jsonerror{"Group does not exist."})
		} else {
			log.WithFields(QueryFields(r, startTime)).Error("Group does not belong to any unit.")
			queryErr = append(queryErr, jsonerror{"Group does not belong to any unit."})
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

func getAllGroupsLegacy(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	lastupdate, parserr :=  stringToParsedTime(strings.TrimSpace(q.Get("last_updated")))
	if parserr != nil {
                log.WithFields(QueryFields(r, startTime)).Error("Error parsing provided update time: " + parserr.Error())
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error parsing last_updated time. Check ferry logs. If provided, it should be an integer representing an epoch time.\"}")
                return
        }

	rows, err := DBptr.Query(`select name, type, gid from groups where groups.last_updated>=$1 or $1 is null`, lastupdate)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		log.WithFields(QueryFields(r, startTime)).Error("Error in DB query: " + err.Error())
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error in DB query.\" }")
		return
	}
	defer rows.Close()
	
	type jsonout struct {
		Groupname string `json:"name"`
		Grouptype string `json:"type"`
		Grpid int `json:"gid"`
	} 
	var tmpout jsonout
	var Out []jsonout
	
	for rows.Next() {
		rows.Scan(&tmpout.Groupname,&tmpout.Grouptype,&tmpout.Grpid)
		Out = append(Out, tmpout)
	}

	var output interface{}	
	if len(Out) == 0 {
		type jsonerror struct {
			Error string `json:"ferry_error"`
		}
		var queryErr []jsonerror
		queryErr = append(queryErr, jsonerror{"Query returned no groups."})
		log.WithFields(QueryFields(r, startTime)).Error("Query returned no groups.")
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

func getAllGroupsMembersLegacy(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	type jsonerror struct {
		Error string `json:"ferry_error"`
	}
	var inputErr []jsonerror

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

	rows, err := DBptr.Query(`select g.name, g.type, g.gid, u.uname, u.uid
							  from user_group as ug
							  join users as u on ug.uid = u.uid
							  right join groups as g on ug.groupid = g.groupid
							  where ug.last_updated >= $1 or g.last_updated >= $1 or $1 is null
							  order by g.name, g.type;`, lastupdate)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		log.WithFields(QueryFields(r, startTime)).Error("Error in DB query: " + err.Error())
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error in DB query.\" }")
		return
	}
	defer rows.Close()

	type jsonuser struct {
		Uname string `json:"username"`
		Uid string `json:"uid"`
	}
	
	type jsongroup struct {
		Gname string `json:"name"`
		Gtype string `json:"type"`
		Gid int `json:"gid"`
		Members []jsonuser `json:"members"`
	}

	var tmpgroup, group jsongroup
	var Out []jsongroup
	
	for rows.Next() {
		var tmpuser jsonuser
		rows.Scan(&tmpgroup.Gname, &tmpgroup.Gtype, &tmpgroup.Gid, &tmpuser.Uname, &tmpuser.Uid)
		if tmpgroup.Gname != group.Gname {
			if group.Gname != "" {
				Out = append(Out, group)
			}
			group = tmpgroup
			if tmpuser.Uname != "" {
				group.Members = append(group.Members, tmpuser)
			}
		} else {
			group.Members = append(group.Members, tmpuser)
		}
	}
	Out = append(Out, group)

	var output interface{}	
	if len(Out) == 0 {
		type jsonerror struct {
			Error string `json:"ferry_error"`
		}
		var queryErr []jsonerror
		queryErr = append(queryErr, jsonerror{"Query returned no groups."})
		log.WithFields(QueryFields(r, startTime)).Error("Query returned no groups.")
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

func setGroupStorageQuota(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

	authorized,authout := authorize(r)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w,"{ \"ferry_error\": \"" + authout + "not authorized.\" }")
		return
	}

	q := r.URL.Query()
	gName := strings.TrimSpace(q.Get("groupname"))
	rName := strings.TrimSpace(q.Get("resourcename"))
	groupquota := strings.TrimSpace(q.Get("quota"))
	unitName := strings.TrimSpace(q.Get("unitname"))
	validtime := strings.TrimSpace(q.Get("valid_until"))
	unit := strings.TrimSpace(q.Get("quota_unit"))

	if gName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No group name specified in http query.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No group name specified.\" }")
		return
	}
	if rName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No storage resource specified in http query.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No storage resource specified.\" }")
		return
	}
	if unitName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No experiment specified in http query.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No experiment name specified.\" }")
		return
	}	
	if groupquota == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No quota value specified in http query.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No quota specified.\" }")
		return
	}
	if validtime == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No expire time given; assuming it is indefinite.")
		validtime = "NULL"
	} else {
		validtime = "'" + validtime + "'"
	}
	if unit == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No quota unit specified in http query.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No quota_unit specified.\" }")
		return
	}
	// We want to store the value in the DB in bytes, no matter what the input unit is. Convert the value here and then set the unit of "B" for bytes	
	newquota, converr := convertValue(groupquota, unit, "B")

	if converr != nil {
		log.WithFields(QueryFields(r, startTime)).Error(converr.Error())
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error converting unit value. It must be a number.\" }")
		return	
	}

	// set the quota value to be stored to newquota, which is now in bytes
	groupquota = strconv.FormatFloat(newquota, 'f', 0, 64)
	unit = "B"

	DBtx, cKey, err := LoadTransaction(r, DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error starting database transaction.\" }")
		return	
	}
	defer DBtx.Rollback(cKey)

	err = setGroupStorageQuotaDB(DBtx, gName, unitName, rName, groupquota, unit, validtime)

	if err == nil {
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		fmt.Fprintf(w,"{ \"ferry_status\": \"success\" }")
	} else {
		if strings.Contains(err.Error(), `Group does not exist.`) {
			log.WithFields(QueryFields(r, startTime)).Error("Group does not exist.")
			fmt.Fprintf(w,"{ \"ferry_error\": \"Group does not exist.\" }")
		} else if strings.Contains(err.Error(), `Resource does not exist.`) {
			log.WithFields(QueryFields(r, startTime)).Error("Resource does not exist.")
			fmt.Fprintf(w,"{ \"ferry_error\": \"Resource does not exist.\" }")
		} else {
			log.WithFields(QueryFields(r, startTime)).Error(err.Error())
			fmt.Fprintf(w,"{ \"ferry_error\": \"Something went wrong.\" }")
		}
	}

	DBtx.Commit(cKey)
}

func addLPCCollaborationGroupLegacy(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	quota := strings.TrimSpace(q.Get("quota"))
	quotaunit := strings.TrimSpace(q.Get("quota_unit"))
	gName := strings.TrimSpace(q.Get("groupname"))
	//We are not going to allow this API call to set a new primary group for CMS
	is_primary := false

	if gName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No groupname specified in http query.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No groupname specified.\" }")
		return
	}
	if gName[0:3] != "lpc" {
		log.WithFields(QueryFields(r, startTime)).Error("LPC groupnames must begin with \"lpc\".")
		fmt.Fprintf(w,"{ \"ferry_error\": \"groupname must begin with lpc.\" }")
		return	
	}
	if quota == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No quota specified in http query.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No quota specified.\" }")
		return
	}
	if quotaunit == "" {
		quotaunit = "B"
	}

	authorized,authout := authorize(r)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w,"{ \"ferry_error\": \"" + authout + "not authorized.\" }")
		return
	}
	
	var usrid, grpid, unitid, compid sql.NullInt64
	err := DBptr.QueryRow(`select (select uid from users where uname = $1),
								  (select groupid from groups where name = $1 and type = 'UnixGroup'),
								  (select unitid from affiliation_units where name = 'cms'),
								  (select compid from compute_resources where name = 'lpcinteractive');`,
						  gName).Scan(&usrid, &grpid, &unitid, &compid)
	switch {
	case err != nil:
		log.WithFields(QueryFields(r, startTime)).Print("Error in affiliation_unit_group DB query: "+err.Error())
		fmt.Fprintf(w,"{ \"ferry_error\": \"DB query error.\" }")
		return

	case !usrid.Valid:
		log.WithFields(QueryFields(r, startTime)).Print("LPC groups require a user with the same name.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"LPC groups require a user with the same name.\" }")
		return

	case !grpid.Valid:
		log.WithFields(QueryFields(r, startTime)).Print("Group does not exist.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"Group does not exist.\" }")
		return

	case !unitid.Valid:
		log.WithFields(QueryFields(r, startTime)).Print("Affiliation unit does not exist.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"Affiliation unit does not exist.\" }")
		return

	case !compid.Valid:
		log.WithFields(QueryFields(r, startTime)).Print("Compute resource does not exist.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"Compute resource does not exist.\" }")
		return
	}

	var inUsrCompRes, inGrpCompRes, inAffUnit bool
	err = DBptr.QueryRow(`select ($1, $2)     in (select compid, uid from compute_access),
								 ($1, $2, $3) in (select compid, uid, groupid from compute_access_group),
								 ($4, $3)     in (select unitid, groupid from affiliation_unit_group);`,
						  compid, usrid, grpid, unitid).Scan(&inUsrCompRes, &inGrpCompRes, &inAffUnit)
		
	if inGrpCompRes && inAffUnit {
		log.WithFields(QueryFields(r, startTime)).Print("Group "+ gName + " is already associated with CMS.")	
		fmt.Fprintf(w,"{ \"ferry_error\": \"Group already associated to CMS.\" }")
		return
	}

	cKey, terr := DBtx.Start(DBptr)
	if terr != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error starting DB transaction: " + terr.Error())
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error starting database transaction.\" }")
		return
	}
	defer DBtx.Rollback(cKey)
	
	
	// Now we need to call addGroupToUnit, *but* we need to tack unitname=cms onto the query part.
	r.URL.RawQuery = r.URL.RawQuery + "&" + "unitname=cms"
	
//	var w2 http.ResponseWriter
	if !inAffUnit {
		adderr := addGroupToUnitDB(&DBtx, gName, "UnixGroup", "cms", is_primary)
		if adderr != nil {
			log.WithFields(QueryFields(r, startTime)).Print("Error adding group to unit: " + adderr.Error() + ". Not continuing.")
			if adderr == sql.ErrNoRows {
				fmt.Fprintf(w,"{ \"ferry_error\": \"group does not exist in groups table.\" }")
				return
					} else {
				fmt.Fprintf(w,"{ \"ferry_error\": \"" + adderr.Error() + "\"}")
				return
			}
		}
	}

	if !inUsrCompRes {
		_, adderr := DBtx.Exec(`insert into compute_access (compid, uid, shell, home_dir, last_updated)
								values ($1, $2,
										(select default_shell from compute_resources where name = 'lpcinteractive'),
										(select default_home_dir from compute_resources where name = 'lpcinteractive') || '/' || $3,
									    NOW());`, compid, usrid, gName)
		if adderr != nil {
			log.WithFields(QueryFields(r, startTime)).Print("Error adding group to lpcinteractive: " + adderr.Error() + ". Not continuing.")
			fmt.Fprintf(w,"{ \"ferry_error\": \"Error adding group to lpcinteractive.\" }")
			return
		}
	}

	if !inGrpCompRes {
		_, adderr := DBtx.Exec("insert into compute_access_group (compid, uid, groupid, is_primary, last_updated) values ($1, $2, $3, true, NOW());", compid, usrid, grpid)
		if adderr != nil {
			log.WithFields(QueryFields(r, startTime)).Print("Error adding group to lpcinteractive: " + adderr.Error() + ". Not continuing.")
			fmt.Fprintf(w,"{ \"ferry_error\": \"Error adding group to lpcinteractive.\" }")
			return
		}
	}

	// We want to store the value in the DB in bytes, no matter what the input unit is. Convert the value here and then set the unit of "B" for bytes	
	newquota, converr := convertValue(quota, quotaunit, "B")
	if converr != nil {
		log.WithFields(QueryFields(r, startTime)).Error(converr.Error())
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error converting unit value. It must be a number.\" }")
		return	
	}
	
	quotaerr := setGroupStorageQuotaDB(&DBtx, gName, "cms", "EOS", strconv.FormatFloat(newquota, 'f', 0, 64), "B", "NULL")
	if quotaerr != nil {
		// print out the error
		// roll back transaction
		log.WithFields(QueryFields(r, startTime)).Print("Error adjusting quota for " + gName + ". Rolling back addition of " + gName + " to cms.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"" + quotaerr.Error() + "\"}")
		return
	} else {
		DBtx.Commit(cKey)
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		fmt.Fprintf(w,"{ \"ferry_status\": \"success\" }")
		return
	}
}

func getGroupAccessToResourceLegacy(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	rName := strings.TrimSpace(q.Get("resourcename"))
	unitName := strings.TrimSpace(q.Get("unitname"))

	type jsonerror struct {
		Error string `json:"ferry_error"`
	}
	var inputErr []jsonerror

	if unitName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No  unit name specified in http query.")
		inputErr = append(inputErr, jsonerror{"No unitname specified."})
	}
	if rName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No resource name specified in http query.")
		inputErr = append(inputErr, jsonerror{"No resourcename specified."})
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

	var unitid,compid int
	checkerr := DBptr.QueryRow(`select unitid from affiliation_units where name=$1`,unitName).Scan(&unitid)
	switch {
	case checkerr == sql.ErrNoRows:
		log.WithFields(QueryFields(r, startTime)).Error("Unit " + unitName + " does not exist.")
		inputErr = append(inputErr, jsonerror{"Unit " + unitName + " does not exist."})
	case checkerr != nil :
		log.WithFields(QueryFields(r, startTime)).Error("Error in affiliation_unit check: " + checkerr.Error())
		inputErr = append(inputErr, jsonerror{"Error in affiliation_unit check."})	
	}

	checkerr = DBptr.QueryRow(`select compid from compute_resources where name=$1`,rName).Scan(&compid)
	switch {
	case checkerr == sql.ErrNoRows:
		log.WithFields(QueryFields(r, startTime)).Error("Resource " + rName + " does not exist.")
		inputErr = append(inputErr, jsonerror{"Resource " + rName + " does not exist."})
	case checkerr != nil :
		log.WithFields(QueryFields(r, startTime)).Error("Error in compute_resource check: " + checkerr.Error())
		inputErr = append(inputErr, jsonerror{"Error in compute_resource check."})	
	}

	if len(inputErr) > 0 {
		jsonout, err := json.Marshal(inputErr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error(err)
		}
		fmt.Fprintf(w, string(jsonout))
		return
	}
	
	type jsonout struct {
		Groupname []string `json:"groups"`
	}
	
	var (
		grouplist jsonout
		scanname string
	)
	rows, dberr := DBptr.Query(`select groups.name from groups
								where groups.groupid in (
									select distinct cg.groupid from compute_access as ca
									join compute_access_group as cg on ca.compid = cg.compid and ca.uid = cg.uid
									join compute_resources as cr on cr.compid=ca.compid
									where ca.compid=$1 and cr.unitid=$2
									and (ca.last_updated>=$3 or $3 is null)
								) order by groups.name`, compid, unitid, lastupdate)
	if dberr != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error in DB query: " + dberr.Error())
		inputErr = append(inputErr, jsonerror{dberr.Error()})
		
		if len(inputErr) > 0 {
			errjson, err := json.Marshal(inputErr)
			if err != nil {
				log.WithFields(QueryFields(r, startTime)).Error(err)
			}
			fmt.Fprintf(w, string(errjson))
			return
		}
	}
	defer rows.Close()
	for rows.Next() {
		rows.Scan(&scanname)
		grouplist.Groupname = append(grouplist.Groupname,scanname)
	}
	
	var output interface{}	
	
	if len(grouplist.Groupname) == 0 {
		var queryErr []jsonerror
		queryErr = append(queryErr, jsonerror{"No groups for this unit have access to this resource."})
		log.WithFields(QueryFields(r, startTime)).Error("No groups for " + unitName + " on resource " + rName + ".")
		output = queryErr
		
	} else {
		output = grouplist
	}
	
	jsonoutput, err := json.Marshal(output)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err.Error())
	}
	fmt.Fprintf(w, string(jsonoutput))
}

func getGroupGIDLegacy(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	gName := q.Get("groupname")
	var iGid bool
	if gName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No groupname specified in http query.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No groupname specified.\" }")
		return
	}
	if q.Get("include_gid") != "" {
		var err error
		iGid, err = strconv.ParseBool(q.Get("include_gid"))
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Invalid include_gid specified in http query.")
			fmt.Fprintf(w,"{ \"ferry_error\": \"Invalid include_gid specified.\" }")
			return
		}
	}

	pingerr := DBptr.Ping()
	if pingerr != nil {
		log.WithFields(QueryFields(r, startTime)).Error(pingerr)
	}
	
	rows, err := DBptr.Query(`select groupid, gid from groups where name=$1 and type = 'UnixGroup'`, gName)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
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
				log.WithFields(QueryFields(r, startTime)).Error(jsonerr)
			}
			fmt.Fprintf(w,string(outline))
			idx++
		}
		if idx == 0 {
			log.WithFields(QueryFields(r, startTime)).Error("Group does not exist.")
			fmt.Fprintf(w, `{ "ferry_error": "Group does not exist." }`)
		} else {
			log.WithFields(QueryFields(r, startTime)).Info("Success!")
			fmt.Fprintf(w," ]")
		}		
	}
}

func getGroupNameLegacy(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	gid := q.Get("gid")
	if gid == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No gid specified in http query.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No gid specified.\" }")
		return
	} else if _, err := strconv.Atoi(gid); err != nil  {
		log.WithFields(QueryFields(r, startTime)).Error("Invalid gid specified in http query.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"Invalid gid specified.\" }")
		return
	}

	pingerr := DBptr.Ping()
	if pingerr != nil {
		log.WithFields(QueryFields(r, startTime)).Error(pingerr)
	}
	
	rows, err := DBptr.Query(`select name from groups where gid=$1`, gid)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
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
				log.WithFields(QueryFields(r, startTime)).Error(jsonerr)
				}
			fmt.Fprintf(w,string(outline))
			idx++
			}
		if idx == 0 {
			log.WithFields(QueryFields(r, startTime)).Error("Group does not exist.")
			fmt.Fprintf(w, `{ "ferry_error": "Group does not exist." }`)
		} else {
			log.WithFields(QueryFields(r, startTime)).Info("Success!")
			fmt.Fprintf(w," ]")
		}		
	}
}

func setAffiliationUnitInfoLegacy(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	unitName := strings.TrimSpace(q.Get("unitname"))
	voms_url := strings.TrimSpace(q.Get("voms_url"))
	altName := strings.TrimSpace(q.Get("alternative_name"))
	unitType := strings.TrimSpace(q.Get("type"))
//	unitId := q.Get("unitid")
//only unitName is required
	if unitName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No unitname specified in http query.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No unitname name specified.\" }")
		return
	}
	if unitType == "" && voms_url == "" && altName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No values specified to modify.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No values (voms_url, type, alternative_name) specified to modify.\" }")
		return
	}

	//require auth	
	authorized,authout := authorize(r)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w,"{ \"ferry_error\": \"" + authout + "not authorized.\" }")
		return
	}
//check if it is really there already
	// check if it already exists and grab current values
	var (
		tmpId int
		tmpaltName sql.NullString 
		tmpvoms sql.NullString
		tmpType sql.NullString
	)
	checkerr := DBptr.QueryRow(`select au.unitid, vu.url, au.alternative_name, au.type from affiliation_units as au
								left join voms_url as vu on au.unitid = vu.unitid where name=$1`,
								unitName).Scan(&tmpId, &tmpvoms, &tmpaltName, &tmpType)
	log.WithFields(QueryFields(r, startTime)).Info("unitID = " + strconv.Itoa(tmpId))
	switch {
	case checkerr == sql.ErrNoRows:
		// OK, it doesn't exist, bail out
		log.WithFields(QueryFields(r, startTime)).Error("Affiliation unit " + unitName + " not in database.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"Unit %s does not exist.\" }",unitName)
		return		
	case checkerr != nil:
		//other weird error
		w.WriteHeader(http.StatusNotFound)
		log.WithFields(QueryFields(r, startTime)).Error("Cannot update affiliation unit " + unitName + ": " + checkerr.Error())
		fmt.Fprintf(w,"{ \"ferry_error\": \"Database error; check logs.\" }")
		return
	default:
		// It exists, start updating

		// parse the values and get the quotes right. 
		// Keep the existing values for those fields that were not explicitly set by the API call.
		// If the new values are "null" then assume the API is trying to null out the existing values.

		// if options provided set the tmp* variables to be the new values.

		if voms_url != "" {
			if strings.ToUpper(voms_url) == "NULL" {
				tmpvoms.Valid = false
				tmpvoms.String = ""
			} else {
				tmpvoms.Valid = true
				tmpvoms.String = voms_url
			}
		}
		if altName != "" {
			if strings.ToUpper(altName) == "NULL" {
				tmpaltName.Valid = false
				tmpaltName.String = ""
			} else {
				tmpaltName.Valid = true
				tmpaltName.String = altName
			}
		}
		if unitType != "" {
			if strings.ToUpper(unitType) == "NULL" {
				tmpType.Valid = false
				tmpType.String = ""
			} else {
				tmpType.Valid = true
				tmpType.String = unitType
			}
		}
		

//		modstr := fmt.Sprintf(`do $$
//									declare v_unitid int;
//
//									declare c_uname text = %s;
//									declare c_aname text = %s;
//									declare c_type text = %s;
//									declare c_url text = %s;
//							   begin
//							   		select unitid into v_unitid from affiliation_units where name = c_uname;
//
//									update affiliation_units set alternative_name = c_aname, type = c_type, last_updated = NOW()
//									where unitid = v_unitid;
//
//									if c_url is not null and ((v_unitid, c_url) not in (select unitid, url from voms_url)) then
//										insert into voms_url (unitid, url) values (v_unitid, c_url);
//									end if;
//							   end $$;`,
//							unitName, altName, unitType, voms_url)
//
//		log.WithFields(QueryFields(r, startTime)).Info("Full string is " + modstr)

		// start DB transaction
		DBtx, cKey, err := LoadTransaction(r, DBptr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Error starting DB transaction: " + err.Error())
			fmt.Fprintf(w,"{ \"ferry_error\": \"Error starting database transaction.\" }")
			return
		}
		defer DBtx.Rollback(cKey)


// First update the affiliation_units table
		
		_, err = DBtx.Exec(`update affiliation_units set alternative_name = $1, type = $2, last_updated = NOW() where unitid = $3`, tmpaltName, tmpType, tmpId)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Error updating " + unitName + " in affiliation_units: " + err.Error())
			fmt.Fprintf(w,"{ \"ferry_error\": \"Error executing DB update.\" }")
		} else {

// Now try updating voms_url if needed. Do nothing if 
			if tmpvoms.Valid {

}
			// error is nil, so it's a success. Commit the transaction and return success.
			DBtx.Commit(cKey)
			log.WithFields(QueryFields(r, startTime)).Info("Successfully set values for " + unitName + " in affiliation_units.")
			fmt.Fprintf(w,"{ \"ferry_status\": \"success\" }")
		}
//		stmt.Close()
		return
	}
}

func getAffiliationUnitMembersLegacy(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	unitName := q.Get("unitname")

	if unitName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No unit name specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No unitname specified.\" }")
		return
	}
	lastupdate, parserr := stringToParsedTime(strings.TrimSpace(q.Get("last_updated")))
	if parserr != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error parsing provided update time: " + parserr.Error())
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error parsing last_updated time. Check ferry logs. If provided, it should be an integer representing an epoch time.\"}")
		return
	}

	var unitId int
	checkerr := DBptr.QueryRow(`select unitid from affiliation_units where name=$1`, unitName).Scan(&unitId)
	switch {
	case checkerr == sql.ErrNoRows:
		// set the header for success since we are already at the desired result
		fmt.Fprintf(w, "{ \"ferry_error\": \"Affiliation unit does not exist.\" }")
		log.WithFields(QueryFields(r, startTime)).Error("unit " + unitName + " not found in DB.")
		return
	case checkerr != nil:
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"ferry_error\": \"Database error.\" }")
		log.WithFields(QueryFields(r, startTime)).Error("deleteUser: Error querying DB for unit " + unitName + ".")
		return
	default:
		log.WithFields(QueryFields(r, startTime)).Info("Fetching members of unit " + unitName)
	}
	rows, err := DBptr.Query(`select ca.uid, users.uname from compute_access as ca join users on ca.uid = users.uid join compute_resources as cr on cr.compid = ca.compid where cr.unitid=$1 and (ca.last_updated>=$2 or $2 is null) order by ca.uid`, unitId, lastupdate)
	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Error(err.Error())
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
		return
	}

	defer rows.Close()
	type jsonout struct {
		UID   int    `json:"uid"`
		UName string `json:"username"`
	}
	var Entry jsonout
	var Out []jsonout
	namemap := make(map[int]string)
	var tmpUID int
	var tmpUname string
	for rows.Next() {
		rows.Scan(&tmpUID, &tmpUname)
		namemap[tmpUID] = tmpUname
	}

	rowsug, err := DBptr.Query(`select DISTINCT ug.uid, users.uname from user_group as ug join affiliation_unit_group as aug on aug.groupid = ug.groupid join users on ug.uid = users.uid where aug.unitid=$1 and (ug.last_updated>=$2 or $2 is null) order by ug.uid`, unitId, lastupdate)
	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Error(err.Error())
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
		return
	}

	defer rowsug.Close()
	for rowsug.Next() {
		rowsug.Scan(&tmpUID, &tmpUname)
		namemap[tmpUID] = tmpUname
	}
	for uid, uname := range namemap {
		Entry.UID = uid
		Entry.UName = uname
		Out = append(Out, Entry)
	}
	var output interface{}
	if len(Out) == 0 {
		type jsonstatus struct {
			Status []string `json:"ferry_status"`
		}
		var queryStat jsonstatus
		queryStat.Status = append(queryStat.Status, "No affiliation unit members found for this query.")
		log.WithFields(QueryFields(r, startTime)).Error("No affiliation unit members found for this query.")
		output = queryStat
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

func getGroupsInAffiliationLegacy(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	unitName := q.Get("unitname")

	if unitName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No unit name specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No unitname specified.\" }")
		return
	}
	lastupdate, parserr := stringToParsedTime(strings.TrimSpace(q.Get("last_updated")))
	if parserr != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error parsing provided update time: " + parserr.Error())
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error parsing last_updated time. Check ferry logs. If provided, it should be an integer representing an epoch time.\"}")
		return
	}

	var unitId int
	checkerr := DBptr.QueryRow(`select unitid from affiliation_units where name=$1`, unitName).Scan(&unitId)
	switch {
	case checkerr == sql.ErrNoRows:
		// set the header for success since we are already at the desired result
		fmt.Fprintf(w, "{ \"ferry_error\": \"Affiliation unit does not exist.\" }")
		log.WithFields(QueryFields(r, startTime)).Error("unit " + unitName + " not found in DB.")
		return
	case checkerr != nil:
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"ferry_error\": \"Database error.\" }")
		log.WithFields(QueryFields(r, startTime)).Error("deleteUser: Error querying DB for unit " + unitName + ".")
		return
	default:

		rows, err := DBptr.Query(`select gid, groups.name, groups.type, aug.is_primary 
								  from affiliation_unit_group as aug
								  join groups on aug.groupid = groups.groupid
								  where aug.unitid=$1 and (aug.last_updated>=$2 or $2 is null)`,
			unitId, lastupdate)
		if err != nil {
			defer log.WithFields(QueryFields(r, startTime)).Error(err.Error())
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
			return
		}

		defer rows.Close()
		type jsonout struct {
			GId      int    `json:"gid"`
			GName    string `json:"name"`
			GType    string `json:"type"`
			GPrimary bool   `json:"is_primary"`
		}
		var Entry jsonout
		var Out []jsonout

		for rows.Next() {
			var tmpGID int
			var tmpGname, tmpGtype string
			var tmpGprimary bool
			rows.Scan(&tmpGID, &tmpGname, &tmpGtype, &tmpGprimary)
			Entry.GId = tmpGID
			Entry.GName = tmpGname
			Entry.GType = tmpGtype
			Entry.GPrimary = tmpGprimary
			Out = append(Out, Entry)
		}
		var output interface{}
		if len(Out) == 0 {
			type jsonerror struct {
				Error string `json:"ferry_error"`
			}
			var queryErr []jsonerror
			queryErr = append(queryErr, jsonerror{"This affiliation unit has no groups."})
			log.WithFields(QueryFields(r, startTime)).Error("This affiliation unit has no groups.")
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
}

func getGroupLeadersinAffiliationUnitLegacy(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	unitName := q.Get("unitname")
	if unitName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No unit name specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No unitname specified.\" }")
		return
	}

	rows, err := DBptr.Query(`select DISTINCT groups.name, groups.type, user_group.uid, users.uname  from user_group join users on users.uid = user_group.uid join groups on groups.groupid = user_group.groupid where is_leader=TRUE and user_group.groupid in (select groupid from affiliation_unit_group left outer join affiliation_units as au on affiliation_unit_group.unitid= au.unitid where au.name=$1) order by groups.name, groups.type`, unitName)
	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Error(err.Error())
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
		return
	}
	defer rows.Close()
	type jsonout struct {
		GName string   `json:"groupname"`
		GType string   `json:"grouptype"`
		UID   []int    `json:"uid"`
		UName []string `json:"username"`
	}
	var Entry jsonout
	var Out []jsonout
	var (
		tmpUID                       int
		tmpUname, tmpGname, tmpGtype string
	)
	for rows.Next() {

		rows.Scan(&tmpGname, &tmpGtype, &tmpUID, &tmpUname)
		if Entry.GName == tmpGname && Entry.GType == tmpGtype {
			Entry.GName = tmpGname
			Entry.GType = tmpGtype
			Entry.UID = append(Entry.UID, tmpUID)
			Entry.UName = append(Entry.UName, tmpUname)
		} else {
			if Entry.GName != "" {
				Out = append(Out, Entry)
			}
			Entry.GName = tmpGname
			Entry.GType = tmpGtype
			Entry.UID = make([]int, 0)
			Entry.UID = append(Entry.UID, tmpUID)
			Entry.UName = make([]string, 0)
			Entry.UName = append(Entry.UName, tmpUname)
		}

	}
	if Entry.GName != "" {
		Out = append(Out, Entry)
	}

	//	Out = append(Out, Entry)
	var output interface{}
	if len(Out) == 0 {
		type jsonerror struct {
			Error string `json:"ferry_error"`
		}
		var queryErr []jsonerror
		queryErr = append(queryErr, jsonerror{"This affiliation unit has no groups with assigned leaders."})
		log.WithFields(QueryFields(r, startTime)).Error("This affiliation unit has no groups with assigned leaders.")
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

func getAffiliationUnitComputeResourcesLegacy(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	unitName := q.Get("unitname")
	if unitName == "" {
		log.WithFields(QueryFields(r, startTime)).Print("No unit name specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No unitname specified.\" }")
		return
	}
	lastupdate, parserr := stringToParsedTime(strings.TrimSpace(q.Get("last_updated")))
	if parserr != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error parsing provided update time: " + parserr.Error())
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error parsing last_updated time. Check ferry logs. If provided, it should be an integer representing an epoch time.\"}")
		return
	}

	rows, err := DBptr.Query(`select cr.name, cr.type, cr.default_shell, cr.default_home_dir from compute_resources as cr join affiliation_units as au on au.unitid = cr.unitid where au.name=$1 and (cr.last_updated>=$2 or $2 is null) order by name`, unitName, lastupdate)
	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Print(err.Error())
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
		return
	}
	defer rows.Close()
	type jsonout struct {
		Name     string `json:"name"`
		Type     string `json:"type"`
		Defshell string `json:"defaultshell"`
		Defdir   string `json:"defaulthomedir"`
	}
	var (
		Entry                     jsonout
		Out                       []jsonout
		tmpName                   string
		tmpType, tmpShell, tmpDir sql.NullString
	)
	for rows.Next() {
		rows.Scan(&tmpName, &tmpType, &tmpShell, &tmpDir)
		Entry.Name = tmpName
		if tmpType.Valid {
			Entry.Type = tmpType.String
		} else {
			Entry.Type = "NULL"
		}
		if tmpShell.Valid {
			Entry.Defshell = tmpShell.String
		} else {
			Entry.Defshell = "NULL"
		}
		if tmpDir.Valid {
			Entry.Defdir = tmpDir.String
		} else {
			Entry.Defdir = "NULL"
		}
		Out = append(Out, Entry)
	}
	var output interface{}
	if len(Out) == 0 {
		type jsonerror struct {
			Error string `json:"ferry_error"`
		}
		var queryErr []jsonerror
		queryErr = append(queryErr, jsonerror{"This affiliation unit has no compute resources."})
		output = queryErr
	} else {
		output = Out
	}
	jsonoutput, err := json.Marshal(output)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Print(err.Error())
	}
	fmt.Fprintf(w, string(jsonoutput))
}

func getAllAffiliationUnitsLegacy(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	voname := strings.TrimSpace(q.Get("voname"))

	//	querystr := `select name, voms_url from affiliation_units where voms_url is not null`
	//	if voname != "" {
	//		querystr := `select name, voms_url from affiliation_units where voms_url is not null and voms_url like %$1%`
	//	}
	lastupdate, parserr := stringToParsedTime(strings.TrimSpace(q.Get("last_updated")))
	if parserr != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error parsing provided update time: " + parserr.Error())
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error parsing last_updated time. Check ferry logs. If provided, it should be an integer representing an epoch time.\"}")
		return
	}

	rows, err := DBptr.Query(`select name, url from affiliation_units as au left join voms_url as vu on au.unitid = vu.unitid
							  where url is not null and url like $1 and (au.last_updated>=$2 or $2 is null)`, "%"+voname+"%", lastupdate)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		log.WithFields(QueryFields(r, startTime)).Error("Error in DB query: " + err.Error())
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
		return
	}
	defer rows.Close()

	type jsonout struct {
		Uname string `json:"name"`
		//		Unitid int `json:"unitid"`
		Voms string `json:"voms_url,omitempty"`
	}

	var tmpout jsonout
	var Out []jsonout

	for rows.Next() {
		//	rows.Scan(&tmpout.Uname,&tmpout.Unitid)
		rows.Scan(&tmpout.Uname, &tmpout.Voms)
		Out = append(Out, tmpout)
	}

	var output interface{}
	if len(Out) == 0 {
		type jsonerror struct {
			Error string `json:"ferry_error"`
		}
		var queryErr []jsonerror
		queryErr = append(queryErr, jsonerror{"Query returned no units."})
		log.WithFields(QueryFields(r, startTime)).Error("Query returned no units.")
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

func createAffiliationUnitLegacy(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	unitName := strings.TrimSpace(q.Get("unitname"))
	voms_url := strings.TrimSpace(q.Get("voms_url"))
	altName := strings.TrimSpace(q.Get("alternative_name"))
	unitType := strings.TrimSpace(q.Get("type"))
	//only the unit name is actually required; the others can be empty
	if unitName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No unitname specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No experiment name specified.\" }")
		return
	} // else {
	//		unitName = "'" + unitName + "'"
	//	}
	//	if voms_url == "" {
	//		voms_url = "NULL"
	//	} else if voms_url != "NULL" {
	//		voms_url = "'" + voms_url + "'"
	//	}
	//	if altName == "" {
	//		altName = "NULL"
	//	} else if altName != "NULL" {
	//		altName = "'" + altName + "'"
	//	}
	//	if unitType == "" {
	//		unitType = "NULL"
	//	} else if unitType != "NULL" {
	//		unitType = "'" + unitType + "'"
	//	}
	authorized, authout := authorize(r)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"ferry_error\": \""+authout+"not authorized.\" }")
		return
	}

	// start a transaction
	DBtx, cKey, err := LoadTransaction(r, DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error starting DB transaction: " + err.Error())
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error starting database transaction.\" }")
		return
	}
	defer DBtx.Rollback(cKey)

	// check if it already exists
	var unitId int
	checkerr := DBtx.QueryRow(`select unitid from affiliation_units where name=$1`, unitName).Scan(&unitId)
	switch {
	case checkerr == sql.ErrNoRows:
		// OK, it doesn't exist, let's add it now.

		log.WithFields(QueryFields(r, startTime)).Info("cKey = " + fmt.Sprintf("%d", cKey))

		_, inserr := DBtx.Exec(`insert into affiliation_units (name, alternative_name, type) values ($1, $2, $3)`, unitName, altName, unitType)
		if inserr != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Error adding " + unitName + " to affiliation_units: " + inserr.Error())
			if cKey != 0 {
				fmt.Fprintf(w, "{ \"ferry_error\": \"Error executing DB insert.\" }")
			}
			return
		} else {
			// check if voms_url was also supplied; insert there too if it was.
			if voms_url != "" {
				_, vomserr := DBtx.Exec(`insert into voms_url (unitid, url) values ((select unitid from affiliation_units where name = $1), $2)`, unitName, voms_url)
				if vomserr != nil {
					log.WithFields(QueryFields(r, startTime)).Error("Error adding " + unitName + " voms_url: " + vomserr.Error())
					if cKey != 0 {
						fmt.Fprintf(w, "{ \"ferry_error\": \"Error executing DB insert.\" }")
					}
					return
				}
			}
			// error is nil, so it's a success. Commit the transaction and return success.
			if cKey != 0 {
				DBtx.Commit(cKey)
			}
			log.WithFields(QueryFields(r, startTime)).Info("Successfully added " + unitName + " to affiliation_units.")
			if cKey != 0 {
				fmt.Fprintf(w, "{ \"ferry_status\": \"success\" }")
			}
		}
		//	stmt.Close()
		return
	case checkerr != nil:
		//other weird error
		log.WithFields(QueryFields(r, startTime)).Error("Cannot create affiliation unit " + unitName + ": " + checkerr.Error())
		if cKey != 0 {
			fmt.Fprintf(w, "{ \"ferry_error\": \"Database error; check logs.\" }")
		}
		return
	default:
		log.WithFields(QueryFields(r, startTime)).Error("Cannot create affiliation unit " + unitName + "; another unit with that name already exists.")
		if cKey != 0 {
			fmt.Fprintf(w, "{ \"ferry_error\": \"Unit %s already exists.\" }", unitName)
		}
		DBtx.Report("Unit %s already exists.")
		return
	}
}

func getBatchPrioritiesLegacy(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	uName := strings.TrimSpace(q.Get("unitname"))
	rName := strings.TrimSpace(q.Get("resourcename"))
//	expName := strings.TrimSpace(q.Get("unitname"))
	if uName == "" {
		uName = "%"
	}
	if rName == "" {
		rName = "%"
	}	
	lastupdate, parserr :=  stringToParsedTime(strings.TrimSpace(q.Get("last_updated")))
	if parserr != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error parsing provided update time: " + parserr.Error())
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error parsing last_updated time. Check ferry logs. If provided, it should be an integer representing an epoch time.\"}")
		return
	}

	rows, err := DBptr.Query(`select cb.name, cb.value, cb.valid_until
								from compute_batch as cb
								join compute_resources as cr on cb.compid = cr.compid
								join affiliation_units as au on cb.unitid = au.unitid
							  where cb.type = 'priority' and cr.name like $1 and au.name like $2
							  and (cr.last_updated >= $3 or $3 is null)`,rName, uName, lastupdate)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		log.WithFields(QueryFields(r, startTime)).Error("No resource name specified in DB query: " + err.Error())
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error in DB query.\" }")
		return
	}
	defer rows.Close()

	var tmpName string
	var tmpTime sql.NullString
	var tmpVal float64
	type jsonout struct {
		Name string `json:"name"`
		Value float64 `json:"priority"`
		Validtime string `json:"valid_until,omitempty"`
	}
	var tmpout jsonout
	var Out []jsonout
	for rows.Next() {
		rows.Scan(&tmpName,&tmpVal,&tmpTime)
		tmpout.Name = tmpName
		tmpout.Value = tmpVal
		if tmpTime.Valid {
			tmpout.Validtime = tmpTime.String 
		}
		Out = append(Out, tmpout)
	}
	var output interface{}	
	if len(Out) == 0 {
		type jsonerror struct {
			Error string `json:"ferry_error"`
		}
		var queryErr []jsonerror
		queryErr = append(queryErr, jsonerror{"Query returned no priorities."})
		log.WithFields(QueryFields(r, startTime)).Error("Query returned no priorities.")
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

func createFQANLegacy(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	fqan := strings.TrimSpace(q.Get("fqan"))
	mGroup := strings.TrimSpace(q.Get("mapped_group"))
	var mUser, unit string

	if fqan == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No fqan specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No fqan specified.\" }")
		return
	}
	if mGroup == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No mapped_group specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No mapped_group specified.\" }")
		return
	}
	if q.Get("mapped_user") != "" {
		mUser = strings.TrimSpace(q.Get("mapped_user"))
	} //else {
	//	mUser = `null`
	//}
	if q.Get("unitname") != "" {
		unit = strings.TrimSpace(q.Get("unitname"))
		if ok, _ := regexp.MatchString(fmt.Sprintf(`^\/(fermilab\/)?%s\/.*`, unit), fqan); !ok {
			log.WithFields(QueryFields(r, startTime)).Error("Invalid FQAN.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"Invalid FQAN.\" }")
			return
		}
	} //else {
	//	unit = `null`
	//}

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

	var uid, unitid, groupid sql.NullInt64
	queryerr := DBtx.tx.QueryRow(`select (select unitid from affiliation_units where name = $1), (select uid from users where uname=$2), (select groupid from groups where name=$3 and type = 'UnixGroup')`, unit, mUser, mGroup).Scan(&unitid, &uid, &groupid)
	if queryerr != nil && queryerr != sql.ErrNoRows {
		log.WithFields(QueryFields(r, startTime)).Error("Error in DB query: " + queryerr.Error())
		if cKey != 0 {
			fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
		}
		return
	}
	if groupid.Valid == false {
		log.WithFields(QueryFields(r, startTime)).Error("Specified mapped_group does not exist.")
		DBtx.Report("Specified mapped_group does not exist.")
		if cKey != 0 {
			fmt.Fprintf(w, "{ \"ferry_error\": \"Specified mapped_group does not exist.\" }")
		}
		return
	}
	if uid.Valid == false && mUser != "" {
		log.WithFields(QueryFields(r, startTime)).Error("Specified mapped_user does not exist.")
		DBtx.Report("Specified mapped_user does not exist.")
		if cKey != 0 {
			fmt.Fprintf(w, "{ \"ferry_error\": \"Specified mapped_user does not exist.\" }")
		}
		return
	}

	// check if the fqan and unit combo is already in the DB. Return an error if so advising the caller to use setFQANMappings instead
	var tmpfqanid int
	queryerr = DBtx.tx.QueryRow(`select fqanid from grid_fqan where unitid=$1 and fqan=$2`, unitid, fqan).Scan(&tmpfqanid)
	if queryerr != nil && queryerr != sql.ErrNoRows {
		log.WithFields(QueryFields(r, startTime)).Error("Query error: unable to verify whether FQAN/unit combo already in DB." + queryerr.Error())
		if cKey != 0 {
			fmt.Fprintf(w, "{ \"ferry_error\": \"Unable to verify whether FQAN/unit combo already in DB. Will not proceed.\" }")
		} else {
			DBtx.Report("Unable to verify whether FQAN/unit combo already in DB. Will not proceed.")
		}
		return
	} else if queryerr == nil {
		// if the error is nil, then it DID find the combo alreayd, and so we should exit.
		log.WithFields(QueryFields(r, startTime)).Error("Specified FQAN already associated with specified unit. Check specified values. Use setFQANMappings to modify an existing entry.")
		if cKey != 0 {
			fmt.Fprintf(w, "{ \"ferry_error\": \"Specified FQAN already associated with this unit. Use setFQANMappings to modify an existing entry.\" }")
		}
		DBtx.Report("Specified FQAN already associated with specified unit. Check specified values. Use setFQANMappings to modify an existing entry.")
		return
	}

	// Make sure that the user is actually in the unit and group in question, if we provided a user
	var tmpu, tmpg int
	if uid.Valid {
		ingrouperr := DBtx.tx.QueryRow(`select uid, groupid from user_group where uid=$1 and groupid=$2`, uid, groupid).Scan(&tmpu, &tmpg)
		if ingrouperr == sql.ErrNoRows {
			log.WithFields(QueryFields(r, startTime)).Error("User not a member of this group.")
			if cKey != 0 {
				fmt.Fprintf(w, "{ \"ferry_error\": \"User not a member of this group.\" }")
			}
			DBtx.Report("User not a member of this group.")
			return

		} else if ingrouperr != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Error in DB query: " + ingrouperr.Error())
			if cKey != 0 {
				fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
			}
			return
		}
		if unitid.Valid {
			var tmpc bool
			inuniterr := DBtx.tx.QueryRow(`	select count(*) > 0  from affiliation_unit_user_certificate as ac
							left join user_certificates as uc on ac.dnid = uc.dnid
                                   			where ac.unitid = $1 and uid = $2`, unitid, uid).Scan(&tmpc)
			if inuniterr == sql.ErrNoRows {
				log.WithFields(QueryFields(r, startTime)).Error("User not a member of this unit.")
				if cKey != 0 {
					fmt.Fprintf(w, "{ \"ferry_error\": \"User not a member of this unit.\" }")
				} else {
					DBtx.Report("User not a member of this unit.")
				}
				return

			} else if inuniterr != nil {
				log.WithFields(QueryFields(r, startTime)).Error("Error in DB query: " + inuniterr.Error())
				if cKey != 0 {
					fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
				}
				return
			}
		}
	}

	_, err = DBtx.Exec(`insert into grid_fqan (fqan, unitid, mapped_user, mapped_group, last_updated) values ($1, $2, $3, $4, NOW())`, fqan, unitid, uid, groupid)

	if err == nil {
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		if cKey != 0 {
			fmt.Fprintf(w, "{ \"ferry_status\": \"success\" }")
		}
	} else {
		if strings.Contains(err.Error(), `user does not exist`) {
			log.WithFields(QueryFields(r, startTime)).Error("User doesn't exist.")
			if cKey != 0 {
				fmt.Fprintf(w, "{ \"ferry_error\": \"User doesn't exist.\" }")
			}
		} else if strings.Contains(err.Error(), `group does not exist`) {
			log.WithFields(QueryFields(r, startTime)).Error("Group doesn't exist.")
			if cKey != 0 {
				fmt.Fprintf(w, "{ \"ferry_error\": \"Group doesn't exist.\" }")
			}
		} else if strings.Contains(err.Error(), `user does not belong to group`) {
			log.WithFields(QueryFields(r, startTime)).Error("User does not belong to group.")
			if cKey != 0 {
				fmt.Fprintf(w, "{ \"ferry_error\": \"User does not belong to group.\" }")
			}
		} else if strings.Contains(err.Error(), `user does not belong to experiment`) {
			log.WithFields(QueryFields(r, startTime)).Error("User does not belong to experiment.")
			if cKey != 0 {
				fmt.Fprintf(w, "{ \"ferry_error\": \"User does not belong to experiment.\" }")
			}
		} else if strings.Contains(err.Error(), `duplicate key value violates unique constraint`) {
			log.WithFields(QueryFields(r, startTime)).Error("FQAN already exists.")
			if cKey != 0 {
				fmt.Fprintf(w, "{ \"ferry_error\": \"FQAN already exists.\" }")
			}
		} else {
			log.WithFields(QueryFields(r, startTime)).Error(err.Error())
			if cKey != 0 {
				fmt.Fprintf(w, "{ \"ferry_error\": \""+err.Error()+"\" }")
			}
		}
		return
	}
	if cKey != 0 {
		DBtx.Commit(cKey)
	}
}

func getGroupFileLegacy(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	unit := strings.TrimSpace(q.Get("unitname"))
	comp := strings.TrimSpace(q.Get("resourcename"))

	if comp == "" {
		comp = "%"
	}
	lastupdate, parserr := stringToParsedTime(strings.TrimSpace(q.Get("last_updated")))
	if parserr != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error parsing provided update time: " + parserr.Error())
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error parsing last_updated time. Check ferry logs. If provided, it should be an integer representing an epoch time.\"}")
		return
	}

	//	rows, err := DBptr.Query(`select gname, gid, uname, unit_exists, comp_exists, last_updated, is_primary from (
	//								select 1 as key, g.name as gname, g.gid as gid, u.uname as uname, cag.last_updated, cag.is_primary
	//								from affiliation_unit_group as aug
	//								join affiliation_units as au using (unitid)
	//								join groups as g using (groupid)
	//								left join compute_resources as cr using (unitid)
	//                                                                left join compute_access_group as cag using (groupid,compid)
	//								left join users as u using (uid)
	//								where (au.name = $1 or $4) and g.type = 'UnixGroup' and (cr.name like $2) and (g.last_updated>=$3 or u.last_updated>=$3 or cag.last_updated>=$3 or au.last_updated>=$3 or $3 is null)
	//                                                                order by g.name,u.uname
	//							) as t
	//								right join (select 1 as key,
	//								$1 in (select name from affiliation_units) as unit_exists,
	//                                                   		$2 in (select name from compute_resources) as comp_exists
	// 							) as c on t.key = c.key;`, unit, comp, lastupdate, unit=="")
	//
	rows, err := DBptr.Query(`select gname, gid, uname, unit_exists, comp_exists, last_updated, is_primary from (
								select 1 as key, g.name as gname, g.gid as gid, u.uname as uname, cag.last_updated, cag.is_primary
                                        			from compute_access_group cag
								join compute_resources as cr using (compid)
								left join affiliation_unit_group aug using (groupid)
								left join affiliation_units as au on au.unitid=cr.unitid
								join groups as g on cag.groupid=g.groupid		
								join users as u on cag.uid=u.uid
								where (au.name = $1 or $4) and g.type = 'UnixGroup' and (cr.name like $2) and (cag.last_updated>=$3 or $3 is null)
                                                                order by g.name,u.uname
							) as t
								right join (select 1 as key,
								$1 in (select name from affiliation_units) as unit_exists,
                                                   		$2 in (select name from compute_resources) as comp_exists
 							) as c on t.key = c.key;`, unit, comp, lastupdate, unit == "")

	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Error(err)
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
		return
	}
	defer rows.Close()

	var unitExists bool
	var compExists bool

	type jsonentry struct {
		Gname    string   `json:"groupname"`
		Gid      int64    `json:"gid"`
		Lasttime int64    `json:"last_updated"`
		Unames   []string `json:"unames"`
	}
	var Entry jsonentry
	var Out []jsonentry
	var tmpGname, tmpUname, tmpTime sql.NullString
	var tmpGid sql.NullInt64
	var tmpPrimary sql.NullBool
	prevGname := ""
	prevUname := ""
	for rows.Next() {

		rows.Scan(&tmpGname, &tmpGid, &tmpUname, &unitExists, &compExists, &tmpTime, &tmpPrimary)
		if tmpGname.Valid {
			if prevGname == "" {
				Entry.Gname = tmpGname.String
				Entry.Gid = tmpGid.Int64
				if tmpPrimary.Valid && tmpPrimary.Bool == false && tmpUname.Valid && tmpUname.String != prevUname {
					Entry.Unames = append(Entry.Unames, tmpUname.String)
					prevUname = tmpUname.String
				}
			} else if prevGname != tmpGname.String {
				Out = append(Out, Entry)
				Entry.Gname = tmpGname.String
				Entry.Gid = tmpGid.Int64
				Entry.Unames = nil
				prevUname = ""
				if tmpPrimary.Valid && tmpPrimary.Bool == false && tmpUname.Valid && tmpUname.String != prevUname {
					Entry.Unames = append(Entry.Unames, tmpUname.String)
					prevUname = tmpUname.String
				}
			} else {
				if tmpPrimary.Valid && tmpPrimary.Bool == false && tmpUname.Valid && tmpUname.String != prevUname {
					Entry.Unames = append(Entry.Unames, tmpUname.String)
					prevUname = tmpUname.String
				}
			}
			prevGname = tmpGname.String
			if tmpTime.Valid {
				log.WithFields(QueryFields(r, startTime)).Debugln("tmpTime is valid" + tmpTime.String)

				parseTime, parserr := time.Parse(time.RFC3339, tmpTime.String)
				lasttime := &Entry.Lasttime
				if parserr != nil {
					log.WithFields(QueryFields(r, startTime)).Error("Error parsing last updated time of " + tmpTime.String)
				} else {
					if *lasttime == 0 || (parseTime.Unix() > *lasttime) {
						*lasttime = parseTime.Unix()
					}
				}
			} else {
				log.WithFields(QueryFields(r, startTime)).Debugln("tmpTime is not valid")
			}
		}
	}
	Out = append(Out, Entry)
	log.WithFields(QueryFields(r, startTime)).Debugln("Length: " + fmt.Sprintf("%d", len(Out)))
	var output interface{}
	if prevGname == "" {
		type jsonerror struct {
			Error string `json:"ferry_error"`
		}
		var Err []jsonerror
		if !unitExists && unit != "" {
			Err = append(Err, jsonerror{"Affiliation unit does not exist."})
			log.WithFields(QueryFields(r, startTime)).Error("Affiliation unit does not exist.")
		}
		if !compExists && comp != "%" {
			Err = append(Err, jsonerror{"Resource does not exist."})
			log.WithFields(QueryFields(r, startTime)).Error("Resource does not exist.")
		}
		if len(Out) == 1 && Out[0].Gname == "" {
			Err = append(Err, jsonerror{"No users or groups with the non-primary GID for this unit and resource(s). Nothing to do."})
			log.WithFields(QueryFields(r, startTime)).Error("No users with the non-primary GID for this unit and resource(s).")
		}
		if len(Err) == 0 {
			Err = append(Err, jsonerror{"Something went wrong."})
			log.WithFields(QueryFields(r, startTime)).Error("Something went wrong.")
		}
		output = Err
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

func lookupCertificateDNLegacy(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	type jsonerror struct {
		Error string `json:"ferry_error"`
	}
	var inputErr []jsonerror

	certdn := q.Get("certificatedn")

	if certdn == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No certificatedn name specified in http query.")
		inputErr = append(inputErr, jsonerror{"No certificatedn name specified."})
	} else {
		dn, err := ExtractValidDN(certdn)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error(err.Error())
			inputErr = append(inputErr, jsonerror{err.Error()})
		}
		certdn = dn
	}

	if len(inputErr) > 0 {
		jsonout, err := json.Marshal(inputErr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error(err)
		}
		fmt.Fprintf(w, string(jsonout))
		return
	}

	rows, err := DBptr.Query(`select u.uid, uname from user_certificates as uc left join users as u on uc.uid = u.uid where dn = $1;`, certdn)
	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Error(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
		return
	}
	defer rows.Close()

	type jsonentry struct {
		Uid   string `json:"uid"`
		Uname string `json:"uname"`
	}
	var Entry jsonentry
	var Out []jsonentry

	for rows.Next() {
		var tmpUid, tmpUname sql.NullString
		rows.Scan(&tmpUid, &tmpUname)

		if tmpUid.Valid {
			Entry.Uid = tmpUid.String
			Entry.Uname = tmpUname.String
			Out = append(Out, Entry)
		}
	}

	var output interface{}
	if len(Out) == 0 {
		var queryErr []jsonerror
		log.WithFields(QueryFields(r, startTime)).Error("Certificate DN does not exist.")
		queryErr = append(queryErr, jsonerror{"Certificate DN does not exist."})
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

func getMappedGidFileLegacy(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

	rows, err := DBptr.Query(`select fqan, uname, gid from grid_fqan as gf
							  left join groups as g on g.groupid = gf.mapped_group
							  left join users as u on u.uid = gf.mapped_user;`)

	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Error(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
		return
	}
	defer rows.Close()

	type jsonentry struct {
		Fqan string `json:"fqan"`
		User string `json:"mapped_uname"`
		Gid  string `json:"mapped_gid"`
	}
	var Entry jsonentry
	var Out []jsonentry

	for rows.Next() {
		var tmpFqan, tmpUser, tmpGid sql.NullString
		rows.Scan(&tmpFqan, &tmpUser, &tmpGid)

		if tmpFqan.Valid {
			Entry = jsonentry{tmpFqan.String, tmpUser.String, tmpGid.String}
			//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
			//!!REMOVE THIS EXCEPTION ONCE DCACHE RESOURCE EXISTS!!
			//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
			if !((strings.Contains(tmpFqan.String, "Role=Analysis") && tmpUser.String != "") ||
				(tmpFqan.String == "/des/Role=Production/Capability=NULL" && tmpUser.String == "des")) {
				Out = append(Out, Entry)
			}
		}
	}

	var output interface{}
	if len(Out) == 0 {
		type jsonerror struct {
			Error string `json:"ferry_error"`
		}
		var Err jsonerror
		Err = jsonerror{"Something went wrong."}
		log.WithFields(QueryFields(r, startTime)).Error("Something went wrong.")
		output = Err
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

func getStorageAuthzDBFileLegacy(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	type jsonerror struct {
		Error []string `json:"ferry_error"`
	}
	var inputErr jsonerror

	pMode := false
	if q.Get("passwdmode") != "" {
		if pBool, err := strconv.ParseBool(q.Get("passwdmode")); err == nil {
			pMode = pBool
		} else {
			log.WithFields(QueryFields(r, startTime)).Error("Error in DB query: " + err.Error())
			inputErr.Error = append(inputErr.Error, "Invalid value for passwdmode. Must be true or false (or omit it from the query).")
		}
	}
	lastupdate, parserr := stringToParsedTime(strings.TrimSpace(q.Get("last_updated")))
	if parserr != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error parsing provided update time: " + parserr.Error())
		inputErr.Error = append(inputErr.Error, "Error parsing last_updated time. Check ferry logs. If provided, it should be an integer representing an epoch time.")
	}

	if len(inputErr.Error) > 0 {
		jsonout, err := json.Marshal(inputErr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error(err)
		}
		fmt.Fprintf(w, string(jsonout))
		return
	}

	rows, err := DBptr.Query(`select u.full_name, u.uname, u.uid, g.gid, ug.last_updated from users as u
							  join user_group as ug on u.uid = ug.uid
							  join groups as g on ug.groupid = g.groupid
                              where g.type = 'UnixGroup' and (ug.last_updated>=$1 or $1 is null)
							  order by u.uname;`, lastupdate)

	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Error(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
		return
	}
	defer rows.Close()

	authMode := func(rows *sql.Rows) (interface{}, bool) {
		type jsonentry struct {
			Decision   string   `json:"decision"`
			User       string   `json:"username"`
			Privileges string   `json:"privileges"`
			Uid        string   `json:"uid"`
			Gid        []string `json:"gid"`
			Home       string   `json:"home"`
			Root       string   `json:"root"`
			FsPath     string   `json:"fs_path"`
		}
		var Entry jsonentry
		var Out []jsonentry

		prevUser := ""
		for rows.Next() {
			var tmpName, tmpUser, tmpUid, tmpGid, tmpTime sql.NullString
			rows.Scan(&tmpName, &tmpUser, &tmpUid, &tmpGid, &tmpTime)

			if tmpGid.Valid {
				if prevUser == "" {
					Entry.Decision = "authorize"
					Entry.User = tmpUser.String
					Entry.Privileges = "read-write"
					Entry.Uid = tmpUid.String
					Entry.Gid = append(Entry.Gid, tmpGid.String)
					Entry.Home = "/"
					Entry.Root = "/pnfs/fnal.gov/usr"
					Entry.FsPath = "/"
				} else if prevUser != tmpUser.String {
					Out = append(Out, Entry)
					Entry.Decision = "authorize"
					Entry.User = tmpUser.String
					Entry.Privileges = "read-write"
					Entry.Uid = tmpUid.String
					Entry.Gid = nil
					Entry.Gid = append(Entry.Gid, tmpGid.String)
					Entry.Home = "/"
					Entry.Root = "/pnfs/fnal.gov/usr"
					Entry.FsPath = "/"
				} else {
					Entry.Gid = append(Entry.Gid, tmpGid.String)
				}
				prevUser = tmpUser.String
			}
		}
		Out = append(Out, Entry)
		return Out, len(Out) > 0
	}

	passwdMode := func(rows *sql.Rows) (interface{}, bool) {
		type jsonuser struct {
			Uname string `json:"username"`
			Uid   string `json:"uid"`
			Gid   string `json:"gid"`
			Gecos string `json:"gecos"`
			Hdir  string `json:"homedir"`
			Shell string `json:"shell"`
		}
		type jsonunit struct {
			Resources map[string][]jsonuser `json:"resources"`
			Lasttime  int64                 `json:"last_updated"`
		}
		Out := make(map[string]jsonunit)

		tmpMap := make(map[string][]jsonuser)
		lasttime := int64(0)
		prevUname := ""
		for rows.Next() {
			var tmpName, tmpUser, tmpUid, tmpGid, tmpTime sql.NullString
			rows.Scan(&tmpName, &tmpUser, &tmpUid, &tmpGid, &tmpTime)

			if tmpTime.Valid {
				parseTime, parserr := time.Parse(time.RFC3339, tmpTime.String)
				if parserr != nil {
					log.WithFields(QueryFields(r, startTime)).Error("Error parsing last updated time of " + tmpTime.String)
				} else {
					if lasttime == 0 || (parseTime.Unix() > lasttime) {
						lasttime = parseTime.Unix()
					}
				}
			} else {
				log.WithFields(QueryFields(r, startTime)).Debugln("tmpTime is not valid")
			}

			if tmpUser.String != prevUname {
				tmpMap["all"] = append(tmpMap["all"], jsonuser{
					tmpUser.String,
					tmpUid.String,
					tmpGid.String,
					tmpName.String,
					"/home/" + tmpUser.String,
					"/sbin/nologin",
				})
				prevUname = tmpUser.String
			}
		}
		Out["fermilab"] = jsonunit{tmpMap, lasttime}

		return Out, len(Out) > 0
	}

	var Out interface{}
	var ok bool
	if !pMode {
		Out, ok = authMode(rows)
	} else {
		Out, ok = passwdMode(rows)
	}

	var output interface{}
	if !ok {
		type jsonerror struct {
			Error string `json:"ferry_error"`
		}
		var Err jsonerror
		Err = jsonerror{"Something went wrong."}
		log.WithFields(QueryFields(r, startTime)).Error("Something went wrong.")
		output = Err
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

func getAffiliationMembersRolesLegacy(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	unit := q.Get("unitname")
	role := q.Get("rolename")

	if unit == "" {
		unit = "%"
	}
	if role == "" {
		role = "%"
	} else {
		role = "%" + role + "%"
	}

	rows, err := DBptr.Query(`select t.name, t.fqan, t.uname, t.full_name, unit_exists, fqan_exists from (
								select 1 as key, au.name, gf.fqan, u.uname, u.full_name
								from grid_access as ga
								left join grid_fqan as gf on ga.fqanid = gf.fqanid
								left join users as u on ga.uid = u.uid
								left join affiliation_units as au on gf.unitid = au.unitid
								where au.name like $1 and gf.fqan like $2
							) as t right join (
								select 1 as key,
								$1 in (select name from affiliation_units) as unit_exists,
								$2 in (select fqan from grid_fqan) as fqan_exists
							) as c on t.key = c.key;`, unit, role)

	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Error(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
		return
	}
	defer rows.Close()

	var unitExists bool
	var roleExists bool

	type jsonentry struct {
		Fqan string `json:"fqan"`
		User string `json:"username"`
		Name string `json:"commonname"`
	}
	Out := make(map[string][]jsonentry)

	for rows.Next() {
		var tmpUnit, tmpFqan, tmpUser, tmpName sql.NullString
		rows.Scan(&tmpUnit, &tmpFqan, &tmpUser, &tmpName, &unitExists, &roleExists)

		if tmpFqan.Valid {
			Out[tmpUnit.String] = append(Out[tmpUnit.String], jsonentry{tmpFqan.String, tmpUser.String, tmpName.String})
		}
	}

	var output interface{}
	if len(Out) == 0 {
		type jsonerror struct {
			Error []string `json:"ferry_error"`
		}
		var Err jsonerror
		if !unitExists {
			Err.Error = append(Err.Error, "Experiment does not exist.")
			log.WithFields(QueryFields(r, startTime)).Error("Experiment does not exist.")
		}
		if !roleExists {
			Err.Error = append(Err.Error, "Role does not exist.")
			log.WithFields(QueryFields(r, startTime)).Error("Role does not exist.")
		}
		if len(Err.Error) == 0 {
			Err.Error = append(Err.Error, "No roles were found")
			log.WithFields(QueryFields(r, startTime)).Error("No roles were found")
		}
		output = Err
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

func getStorageAccessListsLegacy(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	resource := q.Get("resourcename")

	if resource == "" {
		resource = "%"
	}
	/*if resource == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No resourcename specified in http query.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No resourcename specified.\" }")
		return
	}*/

	rows, err := DBptr.Query(`select server, volume, access_level, host from nas_storage where server like $1;`, resource)

	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Error(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
		return
	}
	defer rows.Close()

	type jsonhost struct {
		Host   string `json:"host"`
		Access string `json:"accesslevel"`
	}
	Out := make(map[string][]map[string][]jsonhost)
	Entry := make(map[string][]jsonhost)

	prevServer := ""
	for rows.Next() {
		var tmpServer, tmpVolume, tmpAccess, tmpHost sql.NullString
		rows.Scan(&tmpServer, &tmpVolume, &tmpAccess, &tmpHost)

		if tmpVolume.Valid {
			if prevServer != "" && prevServer != tmpServer.String {
				Out[prevServer] = append(Out[prevServer], Entry)
				Entry = make(map[string][]jsonhost)
			}
			Entry[tmpVolume.String] = append(Entry[tmpVolume.String], jsonhost{tmpHost.String, tmpAccess.String})
		}
		prevServer = tmpServer.String
	}
	Out[prevServer] = append(Out[prevServer], Entry)

	var output interface{}
	if prevServer == "" {
		type jsonerror struct {
			Error string `json:"ferry_error"`
		}
		var Err jsonerror
		Err = jsonerror{"Storage resource does not exist."}
		log.WithFields(QueryFields(r, startTime)).Error("Storage resource does not exist.")
		output = Err
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

func createComputeResourceLegacy(w http.ResponseWriter, r *http.Request) {

	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	rName := q.Get("resourcename")
	unitName := q.Get("unitname")
	rType := q.Get("type")
	shell := q.Get("defaultshell")
	homedir := q.Get("defaulthomedir")

	var nullhomedir sql.NullString
	if rName == "" {
		log.WithFields(QueryFields(r, startTime)).Print("No resource name specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No resourcename specified.\" }")
		return
	}
	if rType == "" {
		log.WithFields(QueryFields(r, startTime)).Print("No resource type specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No type specified.\" }")
		return
	} else if strings.ToUpper(rType) == "NULL" {
		log.WithFields(QueryFields(r, startTime)).Print("'NULL' is an invalid resource type.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"Resource type of NULL is not allowed.\" }")
		return
	}
	//	if unitName == "" {
	//		unitName = "NULL"
	//		}
	if shell == "" || strings.ToUpper(strings.TrimSpace(shell)) == "NULL" {
		shell = "default"
	}
	if homedir == "" || strings.ToUpper(strings.TrimSpace(homedir)) == "NULL" {
		nullhomedir.Valid = false
		log.WithFields(QueryFields(r, startTime)).Print("No defaulthomedir specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No defaulthomedir specified.\" }")
		return
	} else {
		nullhomedir.Valid = true
		nullhomedir.String = homedir
	}

	//require auth
	authorized, authout := authorize(r)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"ferry_error\": \""+authout+"not authorized.\" }")
		return
	}

	var unitID sql.NullInt64

	DBtx, cKey, err := LoadTransaction(r, DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error starting DB transaction: " + err.Error())
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error starting database transaction.\" }")
		return
	}
	defer DBtx.Rollback(cKey)

	//figure out the unitID if we need it

	if unitName != "" {
		uniterr := DBtx.tx.QueryRow(`select unitid from affiliation_units where name=$1`, unitName).Scan(&unitID)
		if uniterr != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Error determining unitid for " + unitName + ": " + uniterr.Error())
			if cKey != 0 {
				fmt.Fprintf(w, "{ \"ferry_error\": \"Error determining unitID for "+unitName+". You cannot add a unit name that does not already exist in affiliation_units.\" }")
			}
			DBtx.Report("Error determining unitID for " + unitName + ".")
			return
		}
	}

	//now, make sure the the resource does not already exist. If it does, bail out. If it does not, do the insertion

	var compId int
	checkerr := DBtx.tx.QueryRow(`select compid from  compute_resources where name=$1`, rName).Scan(&compId)

	switch {
	case checkerr == sql.ErrNoRows:
		// OK, it does not already exist, so we start a transaction

		//	addstr := fmt.Sprintf(`do declare cmpid bigint;  begin select compid into cmpid from compute_resources order by compid desc limit 1; if exists (select name from compute_resources where name=$1) then raise 'resource already exists'; else insert into compute_resources (compid, name, default_shell, unitid, last_updated, default_home_dir, type) values (cmpid+1,$1,$2,$3,NOW(),$4,$5); end if ;  end ;`)
		var addstr string
		if shell == "default" {
			addstr = fmt.Sprintf(`insert into compute_resources (name, default_shell, unitid, last_updated, default_home_dir, type) values ($1, '/sbin/nologin', $2, NOW(), $3, $4)`)
			_, err = DBtx.Exec(addstr, rName, unitID, nullhomedir, rType)
		} else {
			addstr = fmt.Sprintf(`insert into compute_resources (name, default_shell, unitid, last_updated, default_home_dir, type) values ($1, $2, $3, NOW(), $4, $5)`)
			_, err = DBtx.Exec(addstr, rName, shell, unitID, nullhomedir, rType)
		}
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Error starting DB transaction: " + err.Error())
			w.WriteHeader(http.StatusNotFound)
			if cKey != 0 {
				fmt.Fprintf(w, "{ \"ferry_error\": \"Error in database transaction.\" }")
			}
			return
		} else {
			if cKey != 0 {
				DBtx.Commit(cKey)
			}
			log.WithFields(QueryFields(r, startTime)).Error("Added " + rName + " to compute_resources.")
			if cKey != 0 {
				fmt.Fprintf(w, "{ \"ferry_status\": \"success\" }")
			}
			return
		}

	case checkerr != nil:
		//some other error, exit with status 500
		log.WithFields(QueryFields(r, startTime)).Error("Error in DB query: " + checkerr.Error())
		w.WriteHeader(http.StatusNotFound)
		if cKey != 0 {
			fmt.Fprintf(w, "{ \"ferry_error\": \"Error in database check.\" }")
		}
		return
	default:
		// if we get here, it means that the unit already exists. Bail out.
		log.WithFields(QueryFields(r, startTime)).Error("Resource " + rName + " already exists.")
		if cKey != 0 {
			fmt.Fprintf(w, "{ \"ferry_error\": \"Resource already exists.\" }")
		}
		DBtx.Report("Resource already exists.")
		return
	}

}

func setComputeResourceInfoLegacy(w http.ResponseWriter, r *http.Request) {

	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	rName := strings.TrimSpace(q.Get("resourcename"))
	unitName := strings.TrimSpace(q.Get("unitname"))
	rType := strings.TrimSpace(q.Get("type"))
	shell := strings.TrimSpace(q.Get("defaultshell"))
	homedir := strings.TrimSpace(q.Get("defaulthomedir"))

	if rName == "" {
		log.WithFields(QueryFields(r, startTime)).Print("No resource name specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No resourcename specified.\" }")
		return
	}
	if strings.ToUpper(strings.TrimSpace(rType)) == "NULL" {
		log.WithFields(QueryFields(r, startTime)).Print("'NULL' is an invalid resource type.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"Resource type of NULL is not allowed.\" }")
		return
	}

	//require auth
	authorized, authout := authorize(r)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"ferry_error\": \""+authout+"not authorized.\" }")
		return
	}

	var (
		nullshell, nullhomedir sql.NullString
		unitID                 sql.NullInt64
		currentType            string
		compid                 int
	)

	//transaction start, and update command
	DBtx, cKey, err := LoadTransaction(r, DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error starting DB transaction: " + err.Error())
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error starting database transaction.\" }")
		return
	}
	defer DBtx.Rollback(cKey)

	// check if resource exists and grab existing values of everything if so
	err = DBtx.tx.QueryRow(`select distinct compid, default_shell, unitid, default_home_dir, type from compute_resources where name=$1`, rName).Scan(&compid, &nullshell, &unitID, &nullhomedir, &currentType)
	switch {
	case err == sql.ErrNoRows:
		// nothing returned from the select, so the resource does not exist.
		log.WithFields(QueryFields(r, startTime)).Print("compute resource with name " + rName + " not found in compute_resources table. Exiting.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"resource does not exist. Use createComputeResource to add a new resource.\" }")
		return
	case err != nil:
		w.WriteHeader(http.StatusNotFound)
		log.WithFields(QueryFields(r, startTime)).Print("Error in DB query: " + err.Error())
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
		return
	default:

		//actually change stuff
		// if you specfied a new type in the API call (not NULL, as checked for earlier), change it here. Otherwise we keep the existing one
		if rType != "" {
			currentType = rType
		}
		// if you are changing the shell type, do it here. Variations of "NULL" as the string will assume you want it to be null in the database. If you did not specify shell in the query, then we keep the existing value.
		if shell != "" {
			if strings.ToUpper(strings.TrimSpace(shell)) != "NULL" {
				nullshell.Valid = true
				nullshell.String = shell
			} else {
				nullshell.Valid = false
				nullshell.String = ""
			}
		}

		// and the same for default_home_dir, following the same rule as shell.
		if homedir != "" {
			if strings.ToUpper(strings.TrimSpace(homedir)) != "NULL" {
				nullhomedir.Valid = true
				nullhomedir.String = homedir
			} else {
				nullhomedir.Valid = false
				nullhomedir.String = ""
			}
		}

		// if you specified a new affiliation unit, find the new ID and change it. Otherwise keep whatever the select returned, even if it is null
		if unitName != "" {
			if strings.ToUpper(strings.TrimSpace(unitName)) != "NULL" {
				var tmpunitid sql.NullInt64
				iderr := DBtx.tx.QueryRow(`select unitid from affiliation_units where name=$1`, unitName).Scan(&tmpunitid)
				// FIX THIS
				if iderr != nil && iderr != sql.ErrNoRows {
					//some error selecting the new unit ID. Keep the old one!
				} else {
					unitID = tmpunitid
				}
			} else {
				//ah, so the "new" unitName is some variation of NULL, so that means you want to set unitid to null in the DB. Do that by setting unitID.Valid to false
				unitID.Valid = false
			}
		} // end if unitName != ""

		_, commerr := DBtx.Exec(`update compute_resources set default_shell=$1, unitid=$2, last_updated=NOW(), default_home_dir=$3, type=$4 where name=$5`, nullshell, unitID, nullhomedir, currentType, rName)
		if commerr != nil {
			w.WriteHeader(http.StatusNotFound)
			log.WithFields(QueryFields(r, startTime)).Error("Error during DB update " + commerr.Error())
			fmt.Fprintf(w, "{ \"ferry_error\": \"Database error during update.\" }")
			return
		} else {
			// if no error, commit and all that. If this is being called as part of a wrapper, however, cKey will be 0. So only commit if cKey is non-zero
			if cKey != 0 {
				DBtx.Commit(cKey)
			}
			log.WithFields(QueryFields(r, startTime)).Info("Successfully updated " + unitName + ".")
			fmt.Fprintf(w, "{ \"ferry_status\": \"success\" }")
		}
	} //end switch
}

func setUserGridAccessLegacy(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	uName := strings.TrimSpace(q.Get("username"))
	unitName := strings.TrimSpace(q.Get("unitname"))
	suspend := strings.TrimSpace(q.Get("suspend"))

	var suspBool bool

	if uName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No username specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No username specified.\" }")
		return
	}
	if unitName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No unitname specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No unitname specified.\" }")
		return
	}
	if suspend == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No suspend specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No suspend specified.\" }")
		return
	}
	if parsedbool, err := strconv.ParseBool(suspend) ; err == nil {
		suspBool = parsedbool
	} else {
		log.WithFields(QueryFields(r, startTime)).Error("Invalid value for active.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"Invalid value for active. Must be true or false (or omit it from the query).\" }")
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

	var uid, unitid int
	queryerr := DBtx.QueryRow(`select uid from users where uname=$1 for update`, uName).Scan(&uid)
	if queryerr == sql.ErrNoRows {
		log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"User does not exist.\" }")
		return
	}
	if queryerr != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error during query:" + queryerr.Error())
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error during DB query; check logs.\" }")
		return	
	}

	queryerr = DBtx.QueryRow(`select unitid from affiliation_units where name=$1 for update`, unitName).Scan(&unitid)
	if queryerr == sql.ErrNoRows {
		log.WithFields(QueryFields(r, startTime)).Error("Affiliation unit does not exist.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"Affiliation unit does not exist.\" }")
		return
	}
	if queryerr != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error during query:" + queryerr.Error())
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error during DB query; check logs.\" }")
		return	
	}

	_, err = DBtx.Exec(`update grid_access set is_banned = $1, last_updated = NOW()
						where uid = $2 and fqanid in (select fqanid from grid_fqan where unitid = $3)`,
					   suspBool, uid, unitid)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error during query:" + queryerr.Error())
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error during DB query; check logs.\" }")
		return
	}

	log.WithFields(QueryFields(r, startTime)).Info("Success!")
	fmt.Fprintf(w, "{ \"ferry_status\": \"success\" }")
	
	DBtx.Commit(cKey)
}