package main
import (
	"regexp"
	"strings"
	"database/sql"
	"fmt"
	log "github.com/sirupsen/logrus"
	_ "github.com/lib/pq"
	"net/http"
	"encoding/json"
	"strconv"
	"time"
)

func createAffiliationUnit(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	unitName := q.Get("unitname")
	voms_url := q.Get("voms_url")
	altName := q.Get("alternative_name")
	unitType := q.Get("type") 
//only the unit name is actually required; the others can be empty
	if unitName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No unitname specified in http query.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No experiment name specified.\" }")
		return
	} else {
		unitName = "'" + unitName + "'"
	}
	if voms_url == "" {
		voms_url = "NULL"
	} else if voms_url != "NULL" {
		voms_url = "'" + voms_url + "'"
	}
	if altName == "" {
		altName = "NULL"
	} else if altName != "NULL" {
		altName = "'" + altName + "'"
	}
	if unitType == "" {
		unitType = "NULL"
	} else if unitType != "NULL" {
		unitType = "'" + unitType + "'"
	}
	authorized,authout := authorize(r,AuthorizedDNs)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w,"{ \"ferry_error\": \"" + authout + "not authorized.\" }")
		return
	}

	// check if it already exists
	var unitId int
	checkerr := DBptr.QueryRow(`select unitid from affiliation_units where name=$1`,unitName).Scan(&unitId)
	log.WithFields(QueryFields(r, startTime)).Info("unitID = " + strconv.Itoa(unitId))
	switch {
	case checkerr == sql.ErrNoRows:
		// OK, it doesn't exist, let's add it now.
		
		// start a transaction
		DBtx, cKey, err := LoadTransaction(r, DBptr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Error starting DB transaction: " + err.Error())
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w,"{ \"ferry_error\": \"Error starting database transaction.\" }")
			return
		}
		
		// string for the insert statement
		createstr := fmt.Sprintf(`do $$
									declare c_uname text = %s;
									declare c_aname text = %s;
									declare c_type text = %s;
									declare c_url text = %s;
								  begin
									insert into affiliation_units (name, alternative_name, type) values (c_uname, c_aname, c_type);
									if c_url is not null then
										insert into voms_url (unitid, url) values ((select unitid from affiliation_units where name = c_uname), c_url);
									end if;
								  end $$;`,
								  unitName, altName, unitType, voms_url)
		//create prepared statement
		stmt, err := DBtx.tx.Prepare(createstr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Error preparing DB command: " + err.Error())
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w,"{ \"ferry_error\": \"Error preparing database command.\" }")
			return
		}
		//run said statement and check errors
		_, err = stmt.Exec()
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Error adding " + unitName + " to affiliation_units: " + err.Error())
			fmt.Fprintf(w,"{ \"ferry_error\": \"Error executing DB insert.\" }")
			DBtx.Rollback()
		} else {
			// error is nil, so it's a success. Commit the transaction and return success.
			DBtx.Commit(cKey)
			log.WithFields(QueryFields(r, startTime)).Info("Successfully added " + unitName + " to affiliation_units.")
			fmt.Fprintf(w,"{ \"ferry_status\": \"success.\" }")
		}
		stmt.Close()
		return
	case checkerr != nil:
		//other weird error
		w.WriteHeader(http.StatusNotFound)
		log.WithFields(QueryFields(r, startTime)).Error("Cannot create affiliation unit " + unitName + ": " + checkerr.Error())
		fmt.Fprintf(w,"{ \"ferry_error\": \"Database error; check logs.\" }")
		return
	default:
		log.WithFields(QueryFields(r, startTime)).Error("Cannot create affiliation unit " + unitName + "; another unit with that name already exists.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"Unit %s already exists.\" }",unitName)
		return
	} 
}

func removeAffiliationUnit(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	unitName := q.Get("unitname")	
	if unitName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No experiment specified in http query.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No experiment name specified.\" }")
		return
	}
	//requires auth	
	authorized,authout := authorize(r,AuthorizedDNs)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w,"{ \"ferry_error\": \"" + authout + "not authorized.\" }")
		return
	}
//check if it is really there already
	// check if it already exists
	var unitId int
	checkerr := DBptr.QueryRow(`select unitid from affiliation_units where name=$1`,unitName).Scan(&unitId)
	log.WithFields(QueryFields(r, startTime)).Info("unitID = " + strconv.Itoa(unitId))
	switch {
	case checkerr == sql.ErrNoRows:
		// OK, it doesn't exist, let's add it now.
		log.WithFields(QueryFields(r, startTime)).Error("Cannot delete affiliation unit " + unitName + "; unit does not exist.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"Unit %s does not exist.\" }",unitName)
		return	
	case checkerr != nil:
		//other weird error
		w.WriteHeader(http.StatusNotFound)
		log.WithFields(QueryFields(r, startTime)).Error("Cannot remove affiliation unit " + unitName + ": " + checkerr.Error())
		fmt.Fprintf(w,"{ \"ferry_error\": \"Database error; check logs.\" }")
		return
	default:

		DBtx, cKey, err := LoadTransaction(r, DBptr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Error starting DB transaction: " + err.Error())
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w,"{ \"ferry_error\": \"Error starting database transaction.\" }")
			return
		}
			// string for the remove statement
		removestr := fmt.Sprintf("delete from affiliation_units where name='%s'", unitName)
		//create prepared statement
		stmt, err := DBtx.tx.Prepare(removestr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Error preparing DB command: " + err.Error())
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w,"{ \"ferry_error\": \"Error preparing database command.\" }")
			return
		}
		//run said statement and check errors
		_, err = stmt.Exec()
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Error adding " + unitName + " to affiliation_units: " + err.Error())
			fmt.Fprintf(w,"{ \"ferry_error\": \"Error executing DB insert.\" }")
		} else {
			// error is nil, so it's a success. Commit the transaction and return success.
			DBtx.Commit(cKey)
			log.WithFields(QueryFields(r, startTime)).Info("Successfully added " + unitName + " to affiliation_units.")
			fmt.Fprintf(w,"{ \"ferry_status\": \"success.\" }")
		}
		stmt.Close()
		return	
	}
}


func setAffiliationUnitInfo(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	unitName := q.Get("unitname")
	voms_url := q.Get("voms_url")
	altName := q.Get("alternative_name")
	unitType := q.Get("type")
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
	authorized,authout := authorize(r,AuthorizedDNs)
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
		unitName = "'" + unitName + "'"
		if voms_url == "" {
			if tmpvoms.Valid == false {
				voms_url = "NULL"
			} else {
				voms_url = "'" + tmpvoms.String + "'"
			}
		} else if voms_url != "NULL" {
			voms_url = "'" + voms_url + "'"
		}
		if altName == "" {
			if tmpaltName.Valid == false {
				altName = "NULL"
			} else {
				altName = "'" + tmpaltName.String + "'"
			}
		} else if altName != "NULL" {
			altName = "'" + altName + "'"
		}
		if unitType == "" {
			if tmpType.Valid == false {
				unitType = "NULL"
			} else {
				unitType = "'" + tmpType.String + "'"
			}
		} else if unitType != "NULL" {
			unitType = "'" + unitType + "'"
		}

		modstr := fmt.Sprintf(`do $$
									declare v_unitid int;

									declare c_uname text = %s;
									declare c_aname text = %s;
									declare c_type text = %s;
									declare c_url text = %s;
							   begin
							   		select unitid into v_unitid from affiliation_units where name = c_uname;

									update affiliation_units set alternative_name = c_aname, type = c_type, last_updated = NOW()
									where unitid = v_unitid;

									if c_url is not null and ((v_unitid, c_url) not in (select unitid, url from voms_url)) then
										insert into voms_url (unitid, url) values (v_unitid, c_url);
									end if;
							   end $$;`,
							unitName, altName, unitType, voms_url)

		log.WithFields(QueryFields(r, startTime)).Info("Full string is " + modstr)

		// start DB transaction
		DBtx, cKey, err := LoadTransaction(r, DBptr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Error starting DB transaction: " + err.Error())
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w,"{ \"ferry_error\": \"Error starting database transaction.\" }")
			DBtx.Rollback()
			return
		}
		
		stmt, err := DBtx.tx.Prepare(modstr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Error preparing DB command: " + err.Error())
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w,"{ \"ferry_error\": \"Error preparing database command.\" }")
			return
		}

	//run said statement and check errors
		_, err = stmt.Exec()
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Error updating " + unitName + " in affiliation_units: " + err.Error())
			fmt.Fprintf(w,"{ \"ferry_error\": \"Error executing DB update.\" }")
			rberr := DBtx.Rollback()
			if rberr != nil {
				log.WithFields(QueryFields(r, startTime)).Error("Error during rollback: " + rberr.Error())
			}
		} else {
			// error is nil, so it's a success. Commit the transaction and return success.
			DBtx.Commit(cKey)
			log.WithFields(QueryFields(r, startTime)).Info("Successfully set values for " + unitName + " in affiliation_units.")
			fmt.Fprintf(w,"{ \"ferry_status\": \"success.\" }")
		}
		stmt.Close()
		return
	}
}

func getAffiliationUnitMembers(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	unitName := q.Get("unitname")
	
	if unitName == "" {
                log.WithFields(QueryFields(r, startTime)).Error("No unit name specified in http query.")
                fmt.Fprintf(w, "{ \"ferry_error\": \"No unitname specified.\" }")
                return
        }
	lastupdate, parserr :=  stringToParsedTime(strings.TrimSpace(q.Get("last_updated")))
	if parserr != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error parsing provided update time: " + parserr.Error())
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error parsing last_updated time. Check ferry logs. If provided, it should be an integer representing an epoch time.\"}")
		return
	}

        var unitId int
        checkerr := DBptr.QueryRow(`select unitid from affiliation_units where name=$1`,unitName).Scan(&unitId)
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
		UID int  `json:"uid"`
		UName string `json:"username"`
	}
	var Entry jsonout
	var Out []jsonout
	namemap := make(map[int]string)
	var tmpUID int
	var tmpUname string		
	for rows.Next() {
		rows.Scan(&tmpUID,&tmpUname)
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
		rowsug.Scan(&tmpUID,&tmpUname)
		namemap[tmpUID] = tmpUname
	}
	for uid, uname := range namemap {		
		Entry.UID= uid
		Entry.UName = uname
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

func getGroupsInAffiliationUnit(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	unitName := q.Get("unitname")

	if unitName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No unit name specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No unitname specified.\" }")
		return	
	}
	lastupdate, parserr :=  stringToParsedTime(strings.TrimSpace(q.Get("last_updated")))
	if parserr != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error parsing provided update time: " + parserr.Error())
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error parsing last_updated time. Check ferry logs. If provided, it should be an integer representing an epoch time.\"}")
		return
	}

	var unitId int
	checkerr := DBptr.QueryRow(`select unitid from affiliation_units where name=$1`,unitName).Scan(&unitId)
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
		
		rows, err := DBptr.Query(`select gid, groups.name from affiliation_unit_group as aug join groups on aug.groupid = groups.groupid where aug.unitid=$1 and (aug.last_updated>=$2 or $2 is null)`, unitId, lastupdate)
		if err != nil {
			defer log.WithFields(QueryFields(r, startTime)).Error(err.Error())
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
			return
		}
		
		defer rows.Close()
		type jsonout struct {
			GId int  `json:"gid"`
			GName string `json:"name"`
		}
		var Entry jsonout
		var Out []jsonout
		
		for rows.Next() {
			var tmpGID int
			var tmpGname string
			rows.Scan(&tmpGID,&tmpGname)
			Entry.GId = tmpGID
			Entry.GName = tmpGname
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

func getGroupLeadersinAffiliationUnit(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	unitName := q.Get("unitname")
	if unitName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No unit name specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No unitname specified.\" }")
		return	
	}
	
	rows, err := DBptr.Query(`select DISTINCT groups.name, user_group.uid, users.uname  from user_group join users on users.uid = user_group.uid join groups on groups.groupid = user_group.groupid where is_leader=TRUE and user_group.groupid in (select groupid from affiliation_unit_group left outer join affiliation_units as au on affiliation_unit_group.unitid= au.unitid where au.name=$1) order by groups.name`,unitName)
	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Error(err.Error())
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
		return
	}
	defer rows.Close()	
	type jsonout struct {
		GName string `json:"groupname"`
		UID []int  `json:"uid"`
		UName []string `json:"username"`
	}
	var Entry jsonout
	var Out []jsonout
	var (
		tmpUID int
		tmpUname,tmpGname string
	)		
	for rows.Next() {
		
		rows.Scan(&tmpGname,&tmpUID,&tmpUname)
		if (Entry.GName == tmpGname) {
			Entry.GName = tmpGname
			Entry.UID = append(Entry.UID,tmpUID)
			Entry.UName = append(Entry.UName,tmpUname)
		} else {
			if ( Entry.GName != "" ) {
				Out  = append(Out,Entry)
			}
			Entry.GName = tmpGname
			Entry.UID = make([]int, 0)
			Entry.UID = append(Entry.UID,tmpUID)
			Entry.UName = make([]string, 0)
			Entry.UName = append(Entry.UName,tmpUname)
		}
	
	}
	if ( Entry.GName != "" ) {
		Out  = append(Out,Entry)
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

func getAffiliationUnitStorageResources(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query()
//	collabunit := q.Get("unitname")
	NotDoneYet(w, r, startTime)
}

func getAffiliationUnitComputeResources(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	unitName := q.Get("unitname")
	if unitName == "" {
		log.WithFields(QueryFields(r, startTime)).Print("No unit name specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No unitname specified.\" }")
		return	
	}
	lastupdate, parserr :=  stringToParsedTime(strings.TrimSpace(q.Get("last_updated")))
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
		Name string `json:"name"`
		Type string `json:"type"`
		Defshell string `json:"defaultshell"`
		Defdir string `json:"defaulthomedir"`
	}
	var (
		Entry jsonout
		Out []jsonout
		tmpName string
		tmpType,tmpShell,tmpDir sql.NullString
	)		
	for rows.Next() {
		rows.Scan(&tmpName,&tmpType,&tmpShell,&tmpDir)	
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

func createFQAN(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	fqan := q.Get("fqan")
	mGroup := q.Get("mapped_group")
	var mUser, unit string

	if fqan == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No fqan specified in http query.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No fqan specified.\" }")
		return
	}
	if mGroup == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No mapped_group specified in http query.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No mapped_group specified.\" }")
		return
	}
	if q.Get("mapped_user") != "" {
		mUser = `'` + q.Get("mapped_user") + `'`
	} else {
		mUser = `null`
	}
	if q.Get("unitname") != "" {
		unit = `'` + q.Get("unitname") + `'`
	} else {
		unit = `null`
	}

	DBtx, cKey, err := LoadTransaction(r, DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
	}

	query := fmt.Sprintf(`do $$
						  declare
								v_unitid int;

								c_fqan  constant text := %s;
								c_aname constant text := %s;
								c_uname constant text := %s;
								c_gname constant text := %s;
						  begin
								select unitid into v_unitid from affiliation_units where name = c_aname;

								if v_unitid is null and c_aname is not null then
									raise 'affiliation unit does not exist';
								end if;

								insert into grid_fqan (fqan, unitid, mapped_user, mapped_group, last_updated)
								values (c_fqan, v_unitid, c_uname, c_gname, NOW());
						  end $$;`, `'` + fqan + `'`, unit, mUser, `'` + mGroup + `'`)
	re := regexp.MustCompile(`[\s\t\n]+`)
	log.WithFields(QueryFields(r, startTime)).Debug(re.ReplaceAllString(query, " "))
	_, err = DBtx.Exec(query)
	if err == nil {
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		fmt.Fprintf(w,"{ \"ferry_status\": \"success\" }")
	} else {
		if strings.Contains(err.Error(), `violates foreign key constraint "fk_experiment_fqan_users"`) {
			log.WithFields(QueryFields(r, startTime)).Error("User doesn't exist.")
			fmt.Fprintf(w,"{ \"ferry_error\": \"User doesn't exist.\" }")
		} else if strings.Contains(err.Error(), `violates foreign key constraint "fk_experiment_fqan_groups"`) {
			log.WithFields(QueryFields(r, startTime)).Error("Group doesn't exist.")
			fmt.Fprintf(w,"{ \"ferry_error\": \"Group doesn't exist.\" }")
		} else if strings.Contains(err.Error(), `duplicate key value violates unique constraint`) {
			log.WithFields(QueryFields(r, startTime)).Error("FQAN already exists.")
			fmt.Fprintf(w,"{ \"ferry_error\": \"FQAN already exists.\" }")
		} else {
			log.WithFields(QueryFields(r, startTime)).Error(err.Error())
		}
	}

	DBtx.Commit(cKey)
}

func removeFQAN(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

	type jsonstatus struct {
		Status string `json:"ferry_status,omitempty"`
		Error string `json:"ferry_error,omitempty"`
	}
	var inputErr []jsonstatus

	q := r.URL.Query()
	fqan := q.Get("fqan")

	if fqan == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No fqan specified in http query.")
		inputErr = append(inputErr, jsonstatus{"", "No fqan specified."})
	}
	if len(inputErr) > 0 {
		jsonout, err := json.Marshal(inputErr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error(err)
		}
		fmt.Fprintf(w, string(jsonout))
		return
	}

	DBtx, cKey, err := LoadTransaction(r, DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
	}

	var aRows int64
	var res sql.Result
	res, err = DBtx.Exec("delete from grid_fqan where fqan = $1", fqan)
	if err == nil {
		aRows, _ = res.RowsAffected()
	} else {
		aRows = 0
	}

	var output interface{}
	if aRows == 1 {
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		output = jsonstatus{"success", ""}
		if cKey != 0 {
			DBtx.Commit(cKey)
		} else {
			return
		}
	} else {
		if aRows == 0 && err == nil {
			log.WithFields(QueryFields(r, startTime)).Error("FQAN doesn't exist.")
			output = jsonstatus{"", "FQAN doesn't exist."}
		} else {
			log.WithFields(QueryFields(r, startTime)).Error(err.Error())
			output = jsonstatus{"", err.Error()}
		}
	}

	out, err := json.Marshal(output)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err.Error())
	}
	fmt.Fprintf(w, string(out))
}

func setFQANMappings(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	fqan := q.Get("fqan")
	mUser := q.Get("mapped_user")
	mGroup := q.Get("mapped_group")

	var values []string

	type jsonerror struct {
		Error []string `json:"ferry_error"`
	}
	var inputErr jsonerror

	if fqan == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No fqan specified in http query.")
		inputErr.Error = append(inputErr.Error, "No fqan specified.")
	}
	if mUser != "" {
		if strings.ToLower(mUser) != "null" {
			values = append(values, fmt.Sprintf("mapped_user = '%s'", mUser))
		} else {
			values = append(values, "mapped_user = NULL")
		}
	}
	if mGroup != "" {
		if strings.ToLower(mGroup) != "null" {
			values = append(values, fmt.Sprintf("mapped_group = '%s'", mGroup))
		} else {
			values = append(values, "mapped_group = NULL")
		}
	}
	if len(values) == 0 {
		log.WithFields(QueryFields(r, startTime)).Error("No mapped_user or mapped_group specified in http query.")
		inputErr.Error = append(inputErr.Error, "No mapped_user or mapped_group specified.")
	}

	if len(inputErr.Error) > 0 {
		out, err := json.Marshal(inputErr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error(err.Error())
		}
		fmt.Fprintf(w, string(out))
		return
	}

	DBtx, cKey, err := LoadTransaction(r, DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
	}

	var res sql.Result
	var rowsErr error
	var rows int64
	res, err = DBtx.Exec(fmt.Sprintf("update grid_fqan set %s where fqan = '%s'", strings.Join(values, ", "), fqan))
	if err == nil {
		rows, rowsErr = res.RowsAffected()
		if rowsErr != nil {
			log.WithFields(QueryFields(r, startTime)).Error(err.Error())
		}
	} else {
		rows = 0
	}

	if rows == 1 {
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		fmt.Fprintf(w,"{ \"ferry_status\": \"success\" }")
	} else {
		var queryErr jsonerror
		if rows == 0 && err == nil {
			log.WithFields(QueryFields(r, startTime)).Error("FQAN doesn't exist.")
			queryErr.Error = append(queryErr.Error, "FQAN doesn't exist.")
		} else if strings.Contains(err.Error(), `violates foreign key constraint "fk_experiment_fqan_users"`) {
			log.WithFields(QueryFields(r, startTime)).Error("User doesn't exist.")
			queryErr.Error = append(queryErr.Error, "User doesn't exist.")
		} else if strings.Contains(err.Error(), `violates foreign key constraint "fk_experiment_fqan_groups"`) {
			log.WithFields(QueryFields(r, startTime)).Error("Group doesn't exist.")
			queryErr.Error = append(queryErr.Error, "Group doesn't exist.")
		} else if strings.Contains(err.Error(), `null value in column "mapped_group" violates not-null constraint`) {
			log.WithFields(QueryFields(r, startTime)).Error("Attribute mapped_group can not be NULL.")
			queryErr.Error = append(queryErr.Error, "Attribute mapped_group can not be NULL.")
		} else {
			log.WithFields(QueryFields(r, startTime)).Error(err.Error())
			queryErr.Error = append(queryErr.Error, err.Error())
		}
		out, err := json.Marshal(queryErr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error(err.Error())
		}
		fmt.Fprintf(w, string(out))
	}

	DBtx.Commit(cKey)
}

func getAllAffiliationUnits(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	voname := strings.TrimSpace(q.Get("voname"))

//	querystr := `select name, voms_url from affiliation_units where voms_url is not null`
//	if voname != "" {
//		querystr := `select name, voms_url from affiliation_units where voms_url is not null and voms_url like %$1%`
//	}
		lastupdate, parserr :=  stringToParsedTime(strings.TrimSpace(q.Get("last_updated")))
	if parserr != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error parsing provided update time: " + parserr.Error())
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error parsing last_updated time. Check ferry logs. If provided, it should be an integer representing an epoch time.\"}")
		return
	}

	rows, err := DBptr.Query(`select name, url from affiliation_units as au left join voms_url as vu on au.unitid = vu.unitid
							  where url is not null and url like $1 and (au.last_updated>=2 or $2 is null)`,"%" + voname + "%", lastupdate)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		log.WithFields(QueryFields(r, startTime)).Error("Error in DB query: " + err.Error())
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error in DB query.\" }")
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
		rows.Scan(&tmpout.Uname,&tmpout.Voms)
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
