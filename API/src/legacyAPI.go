package main

import (
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