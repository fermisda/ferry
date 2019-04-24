package main

import (
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