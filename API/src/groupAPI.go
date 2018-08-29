package main
import (
	"regexp"
	"strings"
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"net/http"
	"encoding/json"
	"time"
	log "github.com/sirupsen/logrus"
	"strconv"
//	"io/ioutil"
	"errors"
)

func createGroup(w http.ResponseWriter, r *http.Request) {
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
		} else if strings.Contains(err.Error(), `duplicate key value violates unique constraint "idx_groups_group_name"`) {
			log.WithFields(QueryFields(r, startTime)).Error("Group already exists.")
			fmt.Fprintf(w,"{ \"ferry_error\": \"Group already exists.\" }")
		} else {
			log.WithFields(QueryFields(r, startTime)).Error(err.Error())
		}
	}
}

func deleteGroupt(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query()
//	groupname := q.Get("groupname")

	//require auth	
	authorized,authout := authorize(r)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w,"{ \"ferry_error\": \"" + authout + "not authorized.\" }")
		return
	}

	NotDoneYet(w, r, startTime)
}
func deleteGroup(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query()
// should be an int
//	gid := q.Get("gid")

	//require auth	
	authorized,authout := authorize(r)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w,"{ \"ferry_error\": \"" + authout + "not authorized.\" }")
		return
	}

	NotDoneYet(w, r, startTime) 
}
func addGroupToUnit(w http.ResponseWriter, r *http.Request) {
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
			fmt.Fprintf(w,"{ \"ferry_status\": \"success.\" }")
		}
	}
	return	
	
	//	} //end first switch COMMENT 2018-04-04
}

func removeGroupFromUnit(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

	type jsonstatus struct {
		Status string `json:"ferry_status,omitempty"`
		Error []string `json:"ferry_error,omitempty"`
	}
	var inputErr jsonstatus

	q := r.URL.Query()
	gName := strings.TrimSpace(q.Get("groupname"))
	gType := strings.TrimSpace(q.Get("grouptype"))
	uName := strings.TrimSpace(q.Get("unitname"))

	if gName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No groupname specified in http query.")
		inputErr.Error = append(inputErr.Error, "No groupname specified.")
	}
	if gType == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No grouptype specified in http query.")
		inputErr.Error = append(inputErr.Error, "No grouptype specified.")
	}
	if uName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No unitname specified in http query.")
		inputErr.Error = append(inputErr.Error, "No unitname specified.")
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

	typeExists := true
	var groupExists, unitExists bool
	rows, err := DBtx.Query(`select ($1, $2) in (select name, type from groups),
					   $3 in (select name from affiliation_units);`, gName, gType, uName)
	if err != nil {	
		if strings.Contains(err.Error(), "invalid input value for enum") {
			typeExists = false
		} else {
			defer log.WithFields(QueryFields(r, startTime)).Error(err)
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w,"{ \"ferry_error\": \"Error in DB query.\" }")
			return
		}
	} else {
		if rows.Next() {
			rows.Scan(&groupExists, &unitExists)
		}
		rows.Close()
	}

	aRows := int64(0)
	if typeExists {
		res, err := DBtx.Exec(`delete from affiliation_unit_group
							where groupid = (select groupid from groups where (name, type) = ($1, $2))
							and   unitid = (select unitid from affiliation_units where name = $3);`, gName, gType, uName);
		if err == nil {
			aRows, _ = res.RowsAffected()
		}
	}

	var output interface{}
	if aRows == 1 {
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		output = jsonstatus{"success", nil}
		if cKey != 0 {
			DBtx.Commit(cKey)
		} else {
			return
		}
	} else {
		var out jsonstatus
		if !typeExists {
			log.WithFields(QueryFields(r, startTime)).Error("Invalid group type.")
			out.Error = append(out.Error, "Invalid group type.")
		} else {
			if !groupExists {
				log.WithFields(QueryFields(r, startTime)).Error("Group does not exist.")
				out.Error = append(out.Error, "Group does not exist.")
			}
			if !unitExists {
				log.WithFields(QueryFields(r, startTime)).Error("Affiliation unit does not exist.")
				out.Error = append(out.Error, "Affiliation unit does not exist.")
			}
			if groupExists && unitExists {
				log.WithFields(QueryFields(r, startTime)).Error("Group does not belong to affiliation unit.")
				out.Error = append(out.Error, "Group does not belong to affiliation unit.")
			}
		}
		output = out
	}

	out, err := json.Marshal(output)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err.Error())
	}
	fmt.Fprintf(w, string(out))
}
func setPrimaryStatusGroup(w http.ResponseWriter, r *http.Request) {
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
		fmt.Fprintf(w,"{ \"ferry_status\": \"success.\" }")
	}
	stmt.Close()
	return
}

func removePrimaryStatusfromGroup(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query()
//	groupname := q.Get("groupname")
//	collabunit := q.Get("collaboration_unit")

	//require auth	
	authorized,authout := authorize(r)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w,"{ \"ferry_error\": \"" + authout + "not authorized.\" }")
		return
	}

	NotDoneYet(w, r, startTime)
}
func getGroupMembers(w http.ResponseWriter, r *http.Request) {
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

func IsUserMemberOfGroup(w http.ResponseWriter, r *http.Request) {
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

func IsUserLeaderOfGroup(w http.ResponseWriter, r *http.Request) {
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
func setGroupLeader(w http.ResponseWriter, r *http.Request) {
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
				fmt.Fprintf(w,"{ \"ferry_status\": \"success.\" }")
			}
			return
		}
	}
}

func removeGroupLeader(w http.ResponseWriter, r *http.Request) {
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
			if err != nil {
				if strings.Contains(err.Error(), "User is not a leader of this group.") {
					log.WithFields(QueryFields(r, startTime)).Error("User is not a leader of this group.")
					fmt.Fprintf(w,"{ \"ferry_error\": \"User is not a leader of this group.\" }")
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
				fmt.Fprintf(w,"{ \"ferry_status\": \"success.\" }")
			}
			return
		}
	}
}
func getGroupUnits(w http.ResponseWriter, r *http.Request) {
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

	rows, err := DBptr.Query(`select name, type, url, alternative_name, group_exists from (
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

func getBatchPriorities(w http.ResponseWriter, r *http.Request) {
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

func getCondorQuotas(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

	type jsonstatus struct {
		Status string `json:"ferry_status,omitempty"`
		Error string `json:"ferry_error,omitempty"`
	}

	q := r.URL.Query()
	uName := strings.TrimSpace(q.Get("unitname"))
	rName := strings.TrimSpace(q.Get("resourcename"))

	if uName == "" {
		uName = "%"
	}
	if rName == "" {
		rName = "%"
	}

	query := `select resourcename, unitname, condorgroup, value, type, valid_until, unit_exists, resource_exists from (
				select 1 as key, cr.name as resourcename, au.name as unitname, cb.name as condorgroup, cb.value, cb.type, cb.valid_until as valid_until
				from compute_batch as cb
				left join affiliation_units as au on cb.unitid = au.unitid
				join compute_resources as cr on cb.compid = cr.compid
				where cb.type in ('static', 'dynamic') and (au.name like $1 or $1 = '%' and au.name is null) and cr.name like $2
			  ) as T right join (
				select 1 as key,
				$1 in (select name from affiliation_units) as unit_exists,
				$2 in (select name from compute_resources) as resource_exists
			  ) as C on T.key = C.key;`
	re := regexp.MustCompile(`[\s\t\n]+`)
	log.Debug(re.ReplaceAllString(query, " "))

	rows, err := DBptr.Query(query, uName, rName)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		log.WithFields(QueryFields(r, startTime)).Error("No resource name specified in DB query: " + err.Error())
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error in DB query.\" }")
		return
	}
	defer rows.Close()

	type jsonquota struct {
		Group string `json:"condorgroup"`
		Value float64 `json:"value"`
		Qtype string `json:"type"`
		Unit  string `json:"unitname"`
		Vuntil string `json:"valid_until"`
	}
	out := make(map[string][]jsonquota)

	var tmpRname, tmpUname, tmpGroup, tmpType, tmpValid sql.NullString
	var tmpValue sql.NullFloat64
	var unitExists, resourceExists bool

	for rows.Next() {
		rows.Scan(&tmpRname, &tmpUname, &tmpGroup, &tmpValue, &tmpType, &tmpValid, &unitExists, &resourceExists)
		if tmpGroup.Valid {
			out[tmpRname.String] = append(out[tmpRname.String], jsonquota{tmpGroup.String, tmpValue.Float64, tmpType.String, tmpUname.String, tmpValid.String})
		}
	}

	var output interface{}
	if len(out) == 0 {
		type jsonerror struct {
			Error []string `json:"ferry_error"`
		}
		var queryErr jsonerror
		if !unitExists && uName != "%" {
			log.WithFields(QueryFields(r, startTime)).Error("Affiliation unit does not exist.")
			queryErr.Error = append(queryErr.Error, "Affiliation unit does not exist.")
		}
		if !resourceExists && rName != "%" {
			log.WithFields(QueryFields(r, startTime)).Error("Resource does not exist.")
			queryErr.Error = append(queryErr.Error, "Resource does not exist.")
		}
		if len(queryErr.Error) == 0 {
			log.WithFields(QueryFields(r, startTime)).Error("Query returned no quotas.")
			queryErr.Error = append(queryErr.Error, "Query returned no quotas.")
		}
		output = queryErr
	} else {
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		output = out
	}
	jsonoutput, err := json.Marshal(output)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err.Error())
	}
	fmt.Fprintf(w, string(jsonoutput))
}

func setGroupBatchPriority(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query()
//	groupname := q.Get("groupname")
//	resource  := q.Get("resourcename")
//	// should be an int
//	prio := q.Get("priority")

	authorized,authout := authorize(r)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w,"{ \"ferry_error\": \"" + authout + "not authorized.\" }")
		return
	}

	NotDoneYet(w, r, startTime)
}
func setCondorQuota(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	group := strings.TrimSpace(q.Get("condorgroup"))
	comp  := strings.TrimSpace(q.Get("resourcename"))
	quota := strings.TrimSpace(q.Get("quota"))
	until := strings.TrimSpace(q.Get("validuntil"))

	authorized,authout := authorize(r)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w,"{ \"ferry_error\": \"" + authout + "not authorized.\" }")
		return
	}

	if group == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No condorgroup specified in http query.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No condorgroup specified.\" }")
		return
	}
	if comp == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No resourcename specified in http query.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No resourcename specified.\" }")
		return
	}
	if quota == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No quota specified in http query.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No quota specified.\" }")
		return
	}
	if until == "" {
		until = "null"
	} else {
		until = "'" + until + "'"
	}

	uName := strings.Split(group, ".")[0]

	var name, qType string
	if strings.Contains(quota, ".") {
		name = "GROUP_QUOTA_DYNAMIC_group_" + group
		qType = "dynamic"
	} else {
		name = "GROUP_QUOTA_group_" + group
		qType = "static"
	}

	DBtx, cKey, err := LoadTransaction(r, DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
	}
	defer DBtx.Rollback(cKey)

	_, err = DBtx.Exec(fmt.Sprintf(`do $$
									declare 
									    v_unitid int;
										v_compid int;
											
										c_uname constant text := '%s';
										c_compres constant text := '%s';
										c_qname constant text := '%s';
										c_qvalue constant numeric := %s;
										c_qtype constant text := '%s';
										c_valid constant date := %s;
									begin
										select unitid into v_unitid from affiliation_units where name = c_uname;
										select compid into v_compid from compute_resources where name = c_compres;

										if v_compid is null then raise 'null value in column "compid"'; end if;
										
										if (v_compid, c_qname) not in (select compid, name from compute_batch) then
										    insert into compute_batch (compid, name, value, type, unitid, valid_until, last_updated)
															   values (v_compid, c_qname, c_qvalue, c_qtype, v_unitid, c_valid, NOW());
										else
											update compute_batch set value = c_qvalue, valid_until = c_valid, last_updated = NOW()
											where compid = v_compid and name = c_qname;
										end if;
									end $$;`, uName, comp, name, quota, qType, until))

	if err == nil {
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		fmt.Fprintf(w,"{ \"ferry_status\": \"success\" }")
	} else {
		if strings.Contains(err.Error(), `duplicate key value violates unique constraint`) {
			log.WithFields(QueryFields(r, startTime)).Error("This quota already exists.")
			fmt.Fprintf(w,"{ \"ferry_error\": \"This quota already exists.\" }")
		} else if strings.Contains(err.Error(), `null value in column "compid"`) {
			log.WithFields(QueryFields(r, startTime)).Error("Resource does not exist.")
			fmt.Fprintf(w,"{ \"ferry_error\": \"Resource does not exist.\" }")
		} else if strings.Contains(err.Error(), `invalid input syntax for type date`) ||
				  strings.Contains(err.Error(), `date/time field value out of range`) {
			log.WithFields(QueryFields(r, startTime)).Error("Invalid expiration date.")
			fmt.Fprintf(w,"{ \"ferry_error\": \"Invalid expiration date.\" }")
		} else {
			log.WithFields(QueryFields(r, startTime)).Error(err.Error())
			fmt.Fprintf(w,"{ \"ferry_error\": \"Something went wrong.\" }")
		}
	}

	DBtx.Commit(cKey)
}

func getGroupStorageQuotas(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	type jsonerror struct {
		Error string `json:"ferry_error"`
	}
	var inputErr []jsonerror

	groupname := q.Get("groupname")
	resource := q.Get("resourcename")
	exptname := q.Get("unitname")
	quota_unit := strings.TrimSpace(strings.ToUpper(q.Get("quota_unit")))
	if quota_unit != "" {
	okunit := checkUnits(quota_unit)	
		if !okunit {
			log.WithFields(QueryFields(r, startTime)).Error("Invalid unit specified in http query.")
			inputErr = append(inputErr, jsonerror{"Invalid unit specified."})	
		}
	}
	if groupname == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No groupname specified in http query.")
		inputErr = append(inputErr, jsonerror{"No groupname specified."})
	}
	if resource == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No resourcename specified in http query.")
		inputErr = append(inputErr, jsonerror{"No resourcename specified."})
	}
	if exptname == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No unitname specified in http query.")
		inputErr = append(inputErr, jsonerror{"No unitname name specified."})
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

	rows, err := DBptr.Query(`select value, unit, valid_until, group_exists, resource_exists, unit_exists from (
								select 1 as key, sq.value, sq.unit, sq.valid_until from storage_quota sq
							  	join affiliation_units on affiliation_units.unitid = sq.unitid
							  	join storage_resources on storage_resources.storageid = sq.storageid
							  	join groups on groups.groupid = sq.groupid
								where affiliation_units.name = $4 AND storage_resources.name = $3 and (groups.name, groups.type) = ($1, $2) and (sq.last_updated>=$5 or $5 is null)
							) as t right join (
								select 1 as key, 
								($1, $2) in (select name, type from groups) as group_exists,
								$3 in (select name from storage_resources) as resource_exists,
								$4 in (select name from affiliation_units) as unit_exists
							) as c on t.key = c.key;`, groupname, "UnixGroup", resource, exptname, lastupdate)
	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Print("Error in DB query: " + err.Error())
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error in DB query.\" }")
		
		return
	}		
	defer rows.Close()	
	
	type jsonentry struct {
		Value string `json:"value"`
		Unit string `json:"quota_unit"`
		ValidUntil string `json:"valid_until"`
	}
	var Out []jsonentry
	var groupExists, resourceExists, unitExists bool
	
	for rows.Next() {
		var tmpValue,tmpUnit,tmpValid sql.NullString
		rows.Scan(&tmpValue, &tmpUnit, &tmpValid, &groupExists, &resourceExists, &unitExists)
		if tmpValue.Valid {
			
			if quota_unit != "" && quota_unit != tmpUnit.String {
				newval, converr := convertValue(tmpValue.String,tmpUnit.String,quota_unit)
				if converr != nil {
					log.WithFields(QueryFields(r, startTime)).Error("Error converting quota value: " + converr.Error())
					inputErr = append(inputErr, jsonerror{"Error converting quota value to desired unit."})	
				} else {
					tmpValue.String = strconv.FormatFloat(newval, 'f', -1, 64)
				}
			}
			Out = append(Out, jsonentry{tmpValue.String, tmpUnit.String, tmpValid.String})
		}
		}
	
	var output interface{}
	if len(Out) == 0 {
		type jsonerror struct {
			Error string `json:"ferry_error"`
		}
		var queryErr []jsonerror
		if !groupExists {
			log.WithFields(QueryFields(r, startTime)).Error("Group does not exist.")
			queryErr = append(queryErr, jsonerror{"Group does not exist."})
		}
		if !resourceExists {
			log.WithFields(QueryFields(r, startTime)).Error("Resource does not exist.")
			queryErr = append(queryErr, jsonerror{"Resource does not exist."})
		}
		if !unitExists {
			log.WithFields(QueryFields(r, startTime)).Error("Experiment does not exist.")
			queryErr = append(queryErr, jsonerror{"Experiment does not exist."})
		}
		if len(queryErr) == 0 {
			log.WithFields(QueryFields(r, startTime)).Error("Group has no quotas registered.")
			queryErr = append(queryErr, jsonerror{"Group has no quotas registered."})
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

func removeUserAccessFromResource(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	
	type jsonstatus struct {
		Status string `json:"ferry_status,omitempty"`
		Error string `json:"ferry_error,omitempty"`
	}
	var inputErr []jsonstatus

	uName := q.Get("username")
	gName := q.Get("groupname")
	rName := q.Get("resourcename")

	if uName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No username specified in http query.")
		inputErr = append(inputErr, jsonstatus{"", "No username specified."})
	}
	if gName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No groupname specified in http query.")
		inputErr = append(inputErr, jsonstatus{"", "No groupname specified."})
	}
	if rName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No resourcename specified in http query.")
		inputErr = append(inputErr, jsonstatus{"", "No resourcename name specified."})
	}

	if len(inputErr) > 0 {
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
		log.WithFields(QueryFields(r, startTime)).Error("Error starting DB transaction: " + err.Error())
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error starting database transaction.\" }")
		return
	}
	defer DBtx.Rollback(cKey)

	query := `select $1 in (select uname from users),
					 $2 in (select name from groups),
					 $3 in (select name from compute_resources);`
	re := regexp.MustCompile(`[\s\t\n]+`)
	log.Debug(re.ReplaceAllString(query, " "))
	var rows *sql.Rows
	rows, err = DBtx.Query(query, uName, gName, rName)
	if err != nil {	
		defer log.WithFields(QueryFields(r, startTime)).Error(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error in DB query.\" }")
		return
	}
	var userExists, groupExists, resourceExists bool
	if rows.Next() {
		rows.Scan(&userExists, &groupExists, &resourceExists)
	}
	rows.Close()

	query = `delete from compute_access_group where
				uid = (select uid from users where uname = $1) and
				groupid = (select groupid from groups where name = $2 and type = 'UnixGroup') and
				compid = (select compid from compute_resources where name = $3);`
	log.Debug(re.ReplaceAllString(query, " "))

	var res sql.Result
	res, err = DBtx.Exec(query, uName, gName, rName)
	var nRows int64
	if err == nil {
		nRows, _ = res.RowsAffected()
	}

	if err == nil && nRows > 0 {
		query = `select * from compute_access_group where
				uid = (select uid from users where uname = $1) and
				compid = (select compid from compute_resources where name = $2);`
		log.Debug(re.ReplaceAllString(query, " "))
		rows, err = DBtx.Query(query, uName, rName)
		
		if !rows.Next() {
			query = `delete from compute_access where
					uid = (select uid from users where uname = $1) and
					compid = (select compid from compute_resources where name = $2);`
			log.Debug(re.ReplaceAllString(query, " "))
			_, err = DBtx.Exec(query, uName, rName)
		}
	}

	var output interface{}
	if err != nil || nRows == 0 {
		var queryStatus []jsonstatus
		if userExists && groupExists && resourceExists {
			queryStatus = append(queryStatus, jsonstatus{"", "User does not have access to this group in the compute resource."})
			log.WithFields(QueryFields(r, startTime)).Error("User does not have access to this group in the compute resource.")
		}
		if !userExists {
			queryStatus = append(queryStatus, jsonstatus{"", "User does not exist."})
			log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
		}
		if !groupExists {
			queryStatus = append(queryStatus, jsonstatus{"", "Group does not exist."})
			log.WithFields(QueryFields(r, startTime)).Error("Grpup does not exist.")
		}
		if !resourceExists {
			queryStatus = append(queryStatus, jsonstatus{"", "Compute resource does not exist."})
			log.WithFields(QueryFields(r, startTime)).Error("Compute resource does not exist.")
		}
		output = queryStatus
	} else {
		log.WithFields(QueryFields(r, startTime)).Info(fmt.Sprintf("Successfully deleted (%s,%s,%s) from compute_access.", uName, gName, rName))
		if cKey != 0 {
			log.WithFields(QueryFields(r, startTime)).Info("Success!")
			output = jsonstatus{"Success!", ""}
		}
		DBtx.Commit(cKey)
	}

	jsonoutput, err := json.Marshal(output)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err.Error())
	}
	fmt.Fprintf(w, string(jsonoutput))
}

func setGroupAccessToResource(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	rName := q.Get("resourcename")
	gName := q.Get("groupname")
	if gName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No groupname specified in http query.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No value for groupname specified.\" }")
		return
	}
	if rName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No compute resource specified in http query.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No value for resourcename specified.\" }")
		return
	}
	shell := q.Get("default_shell")
	homedir := q.Get("default_home_dir")
	var nullshell,nullhomedir sql.NullString
	var gid,compid int
	
	//require auth	
	authorized,authout := authorize(r)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w,"{ \"ferry_error\": \"" + authout + "not authorized.\" }")
		return
	}
	

	type jsonout struct {
	Uid int `json:"uid"`
	Uname string `json:"username"`
	}



	//first thing we do is check that that resource exists
	err := DBptr.QueryRow(`select compid from compute_resources where name=$1`,rName).Scan(&compid)
	switch {
	case err == sql.ErrNoRows:
		log.WithFields(QueryFields(r, startTime)).Print("Compute resource " + rName + " does not exist.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"Compute resource " + rName + " does not exist.\" }")
		return
	case err != nil:
		log.WithFields(QueryFields(r, startTime)).Print("Error in compute resource DB query: "+err.Error())
		fmt.Fprintf(w,"{ \"ferry_error\": \"Compute resource DB query error.\" }")
		return
		
	default:
		log.WithFields(QueryFields(r, startTime)).Print("Resource "+ rName + "has compid " + strconv.Itoa(compid))	
	}




	//now, get all users is this group
//	rows, err := DBptr.Query(`select users.uid,users.uname, groupid, shell, home_dir from compute_access as ca join groups on groups.groupid=ca.groupid join where groups.name=$1 and cr.compid=$2`,gName,compid)
// if the query expects to change the existing values, set them up now
	if shell != "" { 
		nullshell.Valid = true
		nullshell.String = shell 
	}
	if homedir != "" {
		nullhomedir.Valid = true
		nullhomedir.String = homedir
	}
	
	switch {
		// does not exist already, so do an insert
	case err == sql.ErrNoRows:
		//start yer transaction
		cKey, terr := DBtx.Start(DBptr)
		if terr != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Error starting DB transaction: " + terr.Error())
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w,"{ \"ferry_error\": \"Error starting database transaction.\" }")
			return
		}
		defer DBtx.Rollback(cKey)
		
		_, inserr := DBtx.Exec(`insert into compute_access (compid, groupid, last_updated, shell, home_dir) values ($1,$2,NOW(),$3,$4)`, compid, gid, nullshell, nullhomedir)
		if inserr != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Error in database insert: " + inserr.Error())
			fmt.Fprintf(w,"{ \"ferry_error\": \"Error in database insertion.\" }")
			return
		} else {
			err = DBtx.Commit(cKey)
			log.WithFields(QueryFields(r, startTime)).Error("Set access for " + gName + " in " + rName)
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w,"{ \"result\": \"success.\" }")
			return		
		}
	case err != nil:
		log.WithFields(QueryFields(r, startTime)).Error("Error checking database: " + err.Error())
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error querying database.\" }")
		return
		
	default:
		//already exists, so we are just changing the shell and/or home dir values
		
		//start transaction
		// start a transaction
		DBtx, cKey, err := LoadTransaction(r, DBptr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Error starting DB transaction: " + err.Error())
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w,"{ \"ferry_error\": \"Error starting database transaction.\" }")
			return
		}
		defer DBtx.Rollback(cKey)
		
		execstmt:= `update compute_access (shell, home_dir) values ($1,$2) where compid=$3 and groupid=$4`
		_, moderr := DBtx.Exec(execstmt,nullshell, nullhomedir, compid, gid)
		if moderr != nil {
			log.WithFields(QueryFields(r, startTime)).Print("Error from Update: " + moderr.Error())
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w,"{ \"ferry_error\": \"Error in database transaction.\" }")
			return	
			
		} else {
			commerr := DBtx.Commit(cKey)
			if commerr != nil {
				log.WithFields(QueryFields(r, startTime)).Error("Problem with committing addition of " + rName + " to compute_resources.")
			} else {
				log.WithFields(QueryFields(r, startTime)).Info("Added " + rName + " to compute_resources.")
			}
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w,"{ \"result\": \"success.\" }")
			return			
		}	
	}
	
}

func getAllGroups(w http.ResponseWriter, r *http.Request) {
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

func getAllGroupsMembers(w http.ResponseWriter, r *http.Request) {
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

func addLPCCollaborationGroup(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	quota := strings.TrimSpace(q.Get("quota"))
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

	authorized,authout := authorize(r)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w,"{ \"ferry_error\": \"" + authout + "not authorized.\" }")
		return
	}
	
	var grpid int
	err := DBptr.QueryRow(`select distinct aug.groupid from affiliation_unit_group as aug join groups as g on g.groupid=aug.groupid join affiliation_units as au on au.unitid=aug.unitid where au.name='cms' and g.name=$1 and g.type='UnixGroup'`,gName).Scan(&grpid)
	switch {
	case err == sql.ErrNoRows:
		log.WithFields(QueryFields(r, startTime)).Print("Adding " + gName + " to affiliation_unit_groups.")
	case err != nil:
		log.WithFields(QueryFields(r, startTime)).Print("Error in affiliation_unit_group DB query: "+err.Error())
		fmt.Fprintf(w,"{ \"ferry_error\": \"DB query error.\" }")
		return
		
	default:
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
	
	quotaerr := setGroupStorageQuotaDB(&DBtx, gName, "cms", "EOS", quota, "B", "NULL")
	if quotaerr != nil {
		//print out the error
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

func setGroupStorageQuotaDB(tx *Transaction, gName, unitname, rName, groupquota, quotaunit, valid_until string) (error) {

// since this function is not directly web accessible we don't do as much parameter checking/validation here.
// We assume that the inputs have already been sanitized by the calling function.
// 2018-07-20 Let's not make that a blanket assumption
	
	// quotaunit is known to be OK because it is explicitly set to "B" for internal DB storeage.
	// ditto groupquota because the value passed in is derived from the unit conversion function already
	
	var reterr error
	var vSid, vGid, vUnitid sql.NullInt64
	
	reterr = nil
	
	queryerr := tx.tx.QueryRow(`select storageid, groupid, unitid from (select 1 as key, storageid from storage_resources where name = $1) as sr full outer join (select 1as key, groupid from groups where name = $2 and type = 'UnixGroup') as g on g.key=sr.key right join (select 1as key, unitid from affiliation_units where name = $3) as au on au.key=sr.key`, rName, gName, unitname).Scan(&vSid, &vGid, &vUnitid)
	if queryerr != nil && queryerr != sql.ErrNoRows {
		return queryerr	
	}
	
	if !vSid.Valid {
		reterr = errors.New("Resource does not exist.")
	}
	if !vGid.Valid {
		reterr  = errors.New("Group does not exist.")
	}
	if !vUnitid.Valid {
		reterr  = errors.New("Unit does not exist.")
	}
	
	if reterr != nil {
		return reterr
	}
	
	var vValid sql.NullString
	if valid_until != "" && strings.ToUpper(valid_until) != "NULL" {
		queryerr = tx.tx.QueryRow(`select valid_until from storage_quota where storageid = $1 and unitid = $2 and groupid = $3 and valid_until = $4`,vSid, vUnitid, vGid, valid_until).Scan(&vValid)
	} else {
		queryerr = tx.tx.QueryRow(`select valid_until from storage_quota where storageid = $1 and unitid = $2 and groupid = $3 and valid_until is null`,vSid, vUnitid, vGid).Scan(&vValid)		
	}
	
	if queryerr == sql.ErrNoRows {
		// we did not have this comb in the DB, so it is an insert
		if valid_until != "" && strings.ToUpper(valid_until) != "NULL" {
			vValid.Valid = true
			vValid.String = valid_until
			
			_, reterr = tx.Exec(`insert into storage_quota (storageid, groupid, unitid, value, unit, valid_until)
			             values ($1, $2, $3, $4, $5, $6)`, vSid, vGid, vUnitid, groupquota, quotaunit, vValid)
		} else {
			_, reterr = tx.Exec(`insert into storage_quota (storageid, groupid, unitid, value, unit)
			             values ($1, $2, $3, $4, $5)`, vSid, vGid, vUnitid, groupquota, quotaunit)
		}
	} else if queryerr != nil {
		//some other odd problem, fall back
		return queryerr
	} else {
		// we need to update the existing DB entry
		if valid_until != "" && strings.ToUpper(valid_until) != "NULL" {
			vValid.Valid = true
			vValid.String = valid_until
			
			_, reterr = tx.Exec(`update storage_quota set value = $1, unit = $2, last_updated = NOW()
				   where storageid = $3 and groupid = $4 and unitid = $5 and valid_until = $6`, groupquota, quotaunit, vSid, vGid, vUnitid, vValid)
		} else {

	_, reterr = tx.Exec(`update storage_quota set value = $1, unit = $2, last_updated = NOW()
				   where storageid = $3 and groupid = $4 and unitid = $5 and valid_until is null`, groupquota, quotaunit, vSid, vGid, vUnitid)
} 
	}
	

//	_, err := tx.Exec(fmt.Sprintf(`do $$
//							declare 
//								vSid int;
//								vGid int;
//								vUnitid int;
//
//								cSname constant text := '%s';
//								cGname constant text := '%s';
//								cGtype constant groups_group_type := '%s';
//								cUname constant text := '%s';
//								cValue constant text := '%s';
//								cUnit constant text := '%s';
//								cVuntil constant date := %s;
//							begin
//								select storageid into vSid from storage_resources where name = cSname;
//								select groupid into vGid from groups where (name, type) = (cGname, cGtype);
//								select unitid into vUnitid from affiliation_units where name = cUname;
//
//								if vSid is null then raise 'Resource does not exist.'; end if;
//								if vGid is null then raise 'Group does not exist.'; end if;
//								if vUnitid is null then raise 'Unit does not exist.'; end if;
//								
//								if (vSid, vGid, vUnitid) in (select storageid, groupid, unitid from storage_quota) and cVuntil is NULL then
//									update storage_quota set value = cValue, unit = cUnit, valid_until = cVuntil, last_updated = NOW()
//									where storageid = vSid and groupid = vGid and unitid = vUnitid and valid_until is NULL;
//								else
//									insert into storage_quota (storageid, groupid, unitid, value, unit, valid_until)
//									values (vSid, vGid, vUnitid, cValue, cUnit, cVuntil);
//								end if;
//							end $$;`, rName, gName, "UnixGroup", unitname, groupquota, quotaunit, valid_until))
//	
	//move all error handling to the outside calling function and just return the err itself here
	return reterr
}

func getGroupAccessToResource(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	unitName := strings.TrimSpace(q.Get("unitname"))
	rName := strings.TrimSpace(q.Get("resourcename"))

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
	rows, dberr := DBptr.Query(`select groups.name from groups where groups.groupid in (select distinct ca.groupid from compute_access as ca join compute_resources as cr on cr.compid=ca.compid where ca.compid=$1 and cr.unitid=$2 and (ca.last_updated>=$3 or $3 is null)) order by groups.name`, compid, unitid, lastupdate)
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
