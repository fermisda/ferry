package main
import (
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
		log.WithFields(QueryFields(r, startTime)).Error("No experiment specified in http query.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No experiment name specified.\" }")
		return
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
		cKey, err := DBtx.Start(DBptr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Error starting DB transaction: " + err.Error())
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w,"{ \"ferry_error\": \"Error starting database transaction.\" }")
			return
		}
		
		// string for the insert statement
		createstr := fmt.Sprintf("insert into affiliation_units (voms_url, alternative_name, last_updated, name ) values (%s, %s, NOW(), '%s')", voms_url, altName,unitName)
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
			fmt.Fprintf(w,"{ \"status\": \"success.\" }")
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

		cKey, err := DBtx.Start(DBptr)
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
			fmt.Fprintf(w,"{ \"status\": \"success.\" }")
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
		log.WithFields(QueryFields(r, startTime)).Error("No experiment specified in http query.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No experiment name specified.\" }")
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
	checkerr := DBptr.QueryRow(`select unitid, voms_url, alternative_name, type from affiliation_units where name=$1`,unitName).Scan(&tmpId, &tmpvoms, &tmpaltName, &tmpType)
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

		//start making our prepared statement string
		modstr := "update affiliation_units (voms_url, alternative_name, type, last_updated) values "

		// parse the values and get the quotes right. 
		// Keep the existing values for those fields that were not explicitly set by the API call.
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
	
		valstr := fmt.Sprintf("(%s, %s, %s, NOW()) where name='%s'", voms_url, altName, unitType, unitName)
		log.WithFields(QueryFields(r, startTime)).Info("Full string is " + modstr + valstr)
		return
		// start DB transaction
		
		cKey, err := DBtx.Start(DBptr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Error starting DB transaction: " + err.Error())
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w,"{ \"ferry_error\": \"Error starting database transaction.\" }")
			DBtx.Rollback()
			return
		}
		
		stmt, err := DBtx.tx.Prepare(modstr + valstr)
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
			fmt.Fprintf(w,"{ \"status\": \"success.\" }")
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
	rows, err := DBptr.Query(`select ca.uid, users.uname from compute_access as ca join users on ca.uid = users.uid join compute_resources as cr on cr.compid = ca.compid where cr.unitid=$1 order by ca.uid`, unitId)
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
	
	rowsug, err := DBptr.Query(`select DISTINCT ug.uid, users.uname from user_group as ug join affiliation_unit_group as aug on aug.groupid = ug.groupid join users on ug.uid = users.uid where aug.unitid=$1 order by ug.uid`,unitId)
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
		
		rows, err := DBptr.Query(`select gid, groups.name from affiliation_unit_group as aug join groups on aug.groupid = groups.groupid where aug.unitid=$1`, unitId)
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
	
	rows, err := DBptr.Query(`select cr.name, cr.type, cr.default_shell, cr.default_home_dir from compute_resources as cr join affiliation_units as au on au.unitid = cr.unitid where au.name=$1 order by name`, unitName)  
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
	var mUser sql.NullString

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
		mUser.Scan(q.Get("mapped_user"))
	}

	cKey, err := DBtx.Start(DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
	}

	_, err = DBtx.Exec("insert into grid_fqan (fqan, mapped_user, mapped_group, last_updated) values ($1, $2, $3, NOW())", fqan, mUser, mGroup)
	if err == nil {
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		fmt.Fprintf(w,"{ \"status\": \"success\" }")
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
//	q := r.URL.Query()
//	fqan := q.Get("fqan")
	NotDoneYet(w, r, startTime)
}

func setFQANMappings(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query()
//	fqan := q.Get("fqan")
//	mapuser := q.Get("mapped_user")
//	mapgroup := q.Get("mapped_group")
	NotDoneYet(w, r, startTime)
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
	
	rows, err := DBptr.Query(`select name, voms_url from affiliation_units where voms_url is not null and voms_url like $1`,"%" + voname + "%")
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
