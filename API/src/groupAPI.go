package main
import (
	"strings"
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"net/http"
	"encoding/json"
	"time"
	log "github.com/sirupsen/logrus"
	"strconv"
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

	authorized,authout := authorize(r,AuthorizedDNs)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w,"{ \"ferry_error\": \"" + authout + "not authorized.\" }")
		return
	}

	cKey, err := DBtx.Start(DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
	}

	_, err = DBtx.Exec("insert into groups (gid, name, type, last_updated) values ($1, $2, $3, NOW())", gid, gName, gType)
	if err == nil {
		DBtx.Commit(cKey)
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		fmt.Fprintf(w,"{ \"status\": \"success\" }")
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
	NotDoneYet(w, r, startTime)
}
func deleteGroup(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query()
// should be an int
//	gid := q.Get("gid")
	NotDoneYet(w, r, startTime) 
}
func addGroupToUnit(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	groupname := q.Get("groupname")
	unitName := q.Get("unitname")
	isPrimarystr := q.Get("is_primary")
	isPrimary := 0
//if is_primary is not set in the query, assume it is false. Otherwise take the value from the query
	if isPrimarystr != "" {	
		isPrimary,err := strconv.Atoi(isPrimarystr)
		if err != nil || isPrimary > 1 || isPrimary < 0 {
			log.WithFields(QueryFields(r, startTime)).Print("Invalid value of is_primary (must be 0 or 1).")
			fmt.Fprintf(w,"{ \"ferry_error\": \"Invalid value for is_primary (must be 0 or 1).\" }")
			return
		}
	}
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

	authorized,authout := authorize(r,AuthorizedDNs)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w,"{ \"ferry_error\": \"" + authout + "not authorized.\" }")
		return
	}
	//make sure that the requested group and unit exist; bail out if they don't
	var unitId,groupId int
	checkerr := DBptr.QueryRow(`select unitid from affiliation_units where name=$1`,unitName).Scan(&unitId)
	log.WithFields(QueryFields(r, startTime)).Print("unitID = " + strconv.Itoa(unitId))
	switch {
	case checkerr == sql.ErrNoRows:
		log.WithFields(QueryFields(r, startTime)).Print("Affiliation unit " + unitName + " does not exist.")
	//	w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"ferry_error\": \"Affiliation unit " + unitName + " does not exist.\" }")
		return
	case checkerr != nil:
		log.WithFields(QueryFields(r, startTime)).Print("Affiliation unit query error: " + checkerr.Error())
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error in DB query.\" }")
		return
	default:
		grouperr := DBptr.QueryRow(`select groupid from groups where name=$1`,groupname).Scan(&groupId)
		log.WithFields(QueryFields(r, startTime)).Print(" group ID = " + strconv.Itoa(groupId))
		switch {
		case grouperr == sql.ErrNoRows:
			log.WithFields(QueryFields(r, startTime)).Print("Group " + groupname + " does not exist.")
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w,"{ \"ferry_error\": \"Group " + groupname + " does not exist.\" }")
			return
		case grouperr != nil:
			log.WithFields(QueryFields(r, startTime)).Print("Group query error: " + checkerr.Error())
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w,"{ \"ferry_error\": \"Error in DB query.\" }")
			return
		default:
			// OK, both group and unit exist. Let's get down to business. Check if it's already in affiliation_unit_groups
			
			// start a transaction
			cKey, err := DBtx.Start(DBptr)
			if err != nil {
				log.WithFields(QueryFields(r, startTime)).Print("Error starting DB transaction: " + err.Error())
				w.WriteHeader(http.StatusNotFound)
				fmt.Fprintf(w,"{ \"ferry_error\": \"Error starting database transaction.\" }")
				return
			}
			
			addstr := fmt.Sprintf(`do $$ begin if exists (select groupid, unitid from affiliation_unit_group where groupid=%d and unitid=%d) then raise 'Group and unit combination already in DB.'; else 
insert into affiliation_unit_group (groupid, unitid, is_primary, last_updated) values (%d, %d, %d, NOW()); end if ; end $$;`, groupId, unitId, groupId,unitId,isPrimary)
			stmt, err := DBtx.tx.Prepare(addstr)
			if err != nil {
				log.WithFields(QueryFields(r, startTime)).Print("Error preparing DB command: " + err.Error())
				w.WriteHeader(http.StatusNotFound)
				fmt.Fprintf(w,"{ \"ferry_error\": \"Error preparing database command.\" }")
//				DBtx.Rollback()
				return
			}
			//run said statement and check errors
			_, err = stmt.Exec()
			if err != nil {
				if strings.Contains(err.Error(),`Group and unit combination already in DB`) {
					log.WithFields(QueryFields(r, startTime)).Print("Error adding " + groupname + " to " + unitName + "groups: " + err.Error())
					fmt.Fprintf(w,"{ \"ferry_error\": \"Group already belongs to unit.\" }")
				} else {
					w.WriteHeader(http.StatusNotFound)
					log.WithFields(QueryFields(r, startTime)).Print("Error adding " + groupname + " to " + unitName + "groups: " + err.Error())
					fmt.Fprintf(w,"{ \"ferry_error\": \"Error executing DB insert.\" }")		
				}
//				DBtx.Rollback()
				return
			} else {
				// error is nil, so it's a success. Commit the transaction and return success.
				DBtx.Commit(cKey)
				w.WriteHeader(http.StatusOK)
				log.WithFields(QueryFields(r, startTime)).Print("Successfully added " + groupname + " to affiliation_unit_groups.")
				fmt.Fprintf(w,"{ \"status\": \"success.\" }")
			}
			return	
		}
	} //end first switch
}

func removeGroupFromUnit(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query()
//	groupname := q.Get("groupname")
//	unitname := q.Get("unitname")
	NotDoneYet(w, r, startTime)
}
func setPrimaryStatusGroup(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	groupname := q.Get("groupname")
	unitName := q.Get("unitname")
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

	authorized,authout := authorize(r,AuthorizedDNs)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w,"{ \"ferry_error\": \"" + authout + "not authorized.\" }")
		return
	}
	
	cKey, err := DBtx.Start(DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Print("Error starting DB transaction: " + err.Error())
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error starting database transaction.\" }")
		return
	}
	
	setstr := fmt.Sprintf(`do $$ declare grpid int; declare idunit int; begin select groupid into grpid from groups where name='%s'; 
select unitid into idunit from affiliation_units where name ='%s'; 
if grpid is null then raise 'Group does not exist.' ; elseif
idunit is null then raise 'Unit does not exist.' ; else
update affiliation_unit_group set is_primary=1, last_updated=NOW() where groupid=grpid and unitid=idunit; end if ; end $$;`, groupname, unitName)
	stmt, err := DBtx.tx.Prepare(setstr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Print("Error preparing DB command: " + err.Error())
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error preparing database command.\" }")
		return
	}
	//run said statement and check errors
	_, err = stmt.Exec()
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
		return
	} else {
		// error is nil, so it's a success. Commit the transaction and return success.
		DBtx.Commit(cKey)
		w.WriteHeader(http.StatusOK)
		log.WithFields(QueryFields(r, startTime)).Print("Successfully added " + groupname + " to affiliation_unit_groups.")
		fmt.Fprintf(w,"{ \"status\": \"success.\" }")
	}
	return
}

func removePrimaryStatusfromGroup(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query()
//	groupname := q.Get("groupname")
//	collabunit := q.Get("collaboration_unit")
	NotDoneYet(w, r, startTime)
}
func getGroupMembers(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query() 
	groupname := q.Get("groupname")
	//	//should be a bool
	
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

	err := DBptr.QueryRow(`select groupid from groups where name=$1`,groupname).Scan(&grpid)
	switch {
	case err == sql.ErrNoRows:
		log.WithFields(QueryFields(r, startTime)).Print("Group " + groupname + " does not exist.")
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"ferry_error\": \"Group " + groupname + " does not exist.\" }")
		return	
		
	case err != nil:
		log.WithFields(QueryFields(r, startTime)).Print("Group ID query error: " + err.Error())
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error in DB query.\" }")
		return
		
	default:
		rows, err := DBptr.Query(`select users.uname, users.uid, user_group.is_leader from user_group join users on users.uid=user_group.uid where user_group.groupid=$1`,grpid)
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
			log.WithFields(QueryFields(r, startTime)).Error("Group " + groupname + " has no members")
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

func IsUserLeaderOf(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	uName := q.Get("username")
	groupname := q.Get("groupname")
	
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
	var groupId, uId int
	grouperr := DBptr.QueryRow(`select groupid from groups where name=$1`,groupname).Scan(&groupId)
	switch {
	case grouperr == sql.ErrNoRows:
		log.WithFields(QueryFields(r, startTime)).Print("Group " + groupname + " does not exist.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"Group " + groupname + " does not exist.\" }")
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
			case checkerr == sql.ErrNoRows:
				log.WithFields(QueryFields(r, startTime)).Print("User " + uName + " not a member of "+ groupname)
				w.WriteHeader(http.StatusNotFound)
				fmt.Fprintf(w,"{ \"ferry_error\": \"User is not a member of this group.\" }")
				return
			case checkerr != nil:
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

	//requires authorization
	authorized,authout := authorize(r,AuthorizedDNs)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w,"{ \"ferry_error\": \"" + authout + "not authorized.\" }")
		return
	}
	
	cKey, err := DBtx.Start(DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Print("Error starting DB transaction: " + err.Error())
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error starting database transaction.\" }")
		return
	}
	
	var groupId, uId int
	grouperr := DBptr.QueryRow(`select groupid from groups where name=$1`,groupname).Scan(&groupId)
	switch {
	case grouperr == sql.ErrNoRows:
		log.WithFields(QueryFields(r, startTime)).Print("Group " + groupname + " does not exist.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"Group " + groupname + " does not exist.\" }")
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
			setstr := fmt.Sprintf(`do $$ begin if exists (select uid,groupid from user_group where groupid=%d and uid=%d) then update user_group set is_leader=true, last_updated=NOW() where groupid=%d and uid=%d; else raise 'User is not a member of this group.'; end if ; end $$;`, groupId, uId, groupId, uId)
			stmt, err := DBtx.tx.Prepare(setstr)
			if err != nil {
				log.WithFields(QueryFields(r, startTime)).Print("Error preparing DB command: " + err.Error())
				w.WriteHeader(http.StatusNotFound)
				fmt.Fprintf(w,"{ \"ferry_error\": \"Error preparing database command.\" }")
				return
			}
			//run said statement and check errors
			_, err = stmt.Exec()
			if err != nil {
				if strings.Contains(err.Error(),`User is not a member of this group`) {
					log.WithFields(QueryFields(r, startTime)).Print("User " + uName + " is not a member of " + groupname + ".")
					fmt.Fprintf(w,"{ \"ferry_error\": \"User not a member of group.\" }")
				} else {
					w.WriteHeader(http.StatusNotFound)
					log.WithFields(QueryFields(r, startTime)).Print("Error setting " + uName + " leader of " + groupname + ": " + err.Error())
					fmt.Fprintf(w,"{ \"ferry_error\": \"Error executing DB update.\" }")		
				}
				return
			} else {
				// error is nil, so it's a success. Commit the transaction and return success.
				DBtx.Commit(cKey)
				w.WriteHeader(http.StatusOK)
				log.WithFields(QueryFields(r, startTime)).Print("Successfully set " + uName + " as leader of " + groupname + ".")
				fmt.Fprintf(w,"{ \"status\": \"success.\" }")
			}
			return
		}
	}
}

func removeGroupLeader(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query()
//	uname := q.Get("username")
//	groupname := q.Get("groupname")
	NotDoneYet(w, r, startTime)
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
	expOnly := false

	if group == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No group name specified in http query.")
		inputErr = append(inputErr, jsonerror{"No group name specified."})
	}
	if q.Get("experimentsonly") != "" {
		var err error
		if expOnly, err = strconv.ParseBool(q.Get("experimentsonly")); err != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Invalid experimentsonly specified in http query.")
			inputErr = append(inputErr, jsonerror{"Invalid experimentsonly specified."})
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

	rows, err := DBptr.Query(`select name, type, url, alternative_name, group_exists from (
								select 1 as key, au.*, vu.url from
									affiliation_unit_group as ag left join
									groups as g on ag.groupid = g.groupid left join
									affiliation_units as au on ag.unitid = au.unitid left join
									voms_url as vu on au.unitid = vu.unitid
								where g.name = $1 and ((url is not null = $2) or not $2)
							) as t right join (
								select 1 as key, $1 in (select name from groups) as group_exists
							) as c on t.key = c.key;`, group, expOnly)
	if err != nil {	
		defer log.WithFields(QueryFields(r, startTime)).Error(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error in DB query.\" }")
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
		w.WriteHeader(http.StatusNotFound)
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

func getGroupBatchPriorities(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	groupname := strings.TrimSpace(q.Get("groupname"))
	rName := strings.TrimSpace(q.Get("resourcename"))
//	expName := strings.TrimSpace(q.Get("experimentname"))
	if groupname == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No groupname specified in http query.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No groupname specified.\" }")
		return
	}
	if rName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No resource name specified in http query.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No resourcename specified.\" }")
		return
	}	
	
	rows, err := DBptr.Query(`select groups.name, cb.value, cb.valid_until from compute_batch as cb join compute_resources as cr on cb.compid=cr.compid join groups on groups.groupid=cb.groupid where cb.type='priority' and cr.name=$1 `,rName)
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
		Grpname string `json:"groupname"`
		Value float64 `json:"priority"`
		Validtime string `json:"valid_until,omitempty"`
	}
	var tmpout jsonout
	var Out []jsonout
	for rows.Next() {
		rows.Scan(&tmpName,&tmpVal,&tmpTime)
		tmpout.Grpname = tmpName
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

func getGroupCondorQuotas(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query()
//	groupname := q.Get("groupname")
//	resource := q.Get("resourcename")
//	exptname := q.Get("experimentname")
	NotDoneYet(w, r, startTime)
}
func setGroupBatchPriority(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query()
//	groupname := q.Get("groupname")
//	resource  := q.Get("resourcename")
//	// should be an int
//	prio := q.Get("priority")
	NotDoneYet(w, r, startTime)
}
func setGroupCondorQuota(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	group := q.Get("groupname")
	comp  := q.Get("resourcename")
	quota := q.Get("quota")
	until := q.Get("validuntil")

	authorized,authout := authorize(r,AuthorizedDNs)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w,"{ \"ferry_error\": \"" + authout + "not authorized.\" }")
		return
	}

	if group == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No groupname specified in http query.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No groupname specified.\" }")
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
	}

	gName := strings.Split(group, ".")[0]

	var name, qType string
	if strings.Contains(quota, ".") {
		name = "GROUP_QUOTA_DYNAMIC_group_" + group
		qType = "dynamic"
	} else {
		name = "GROUP_QUOTA_group_" + group
		qType = "static"
	}

	cKey, err := DBtx.Start(DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
	}

	_, err = DBtx.Exec(fmt.Sprintf(`do $$
									declare 
									    v_groupid int;
										v_compid int;
											
										c_gname constant text := '%s';
										c_compres constant text := '%s';
										c_qname constant text := '%s';
										c_qvalue constant numeric := %s;
										c_qtype constant text := '%s';
										c_valid constant date := '%s';
									begin
										select groupid into v_groupid from groups where name = c_gname;
										select compid into v_compid from compute_resources where name = c_compres;

										if v_compid is null then raise 'null value in column "compid"'; end if;
										
										if (v_compid, c_qname) not in (select compid, name from compute_batch) then
										    insert into compute_batch (compid, name, value, type, groupid, valid_until, last_updated)
															   values (v_compid, c_qname, c_qvalue, c_qtype, v_groupid, c_valid, NOW());
										else
											update compute_batch set value = c_qvalue, valid_until = c_valid, last_updated = NOW()
											where compid = v_compid and name = c_qname;
										end if;
									end $$;`, gName, comp, name, quota, qType, until))

	if err == nil {
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		fmt.Fprintf(w,"{ \"status\": \"success\" }")
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
	groupname := q.Get("groupname")
	resource := q.Get("resourcename")
	exptname := q.Get("experimentname")
	if groupname == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No group name specified in http query.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No group name specified.\" }")
		return
	}
	if resource == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No storage resource specified in http query.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No storage resource specified.\" }")
		return
	}
	if exptname == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No experiment specified in http query.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No experiment name specified.\" }")
		return
	}

	rows, err := DBptr.Query(`select sq.value, sq.unit, sq.valid_until from storage_quota sq INNER JOIN affiliation_units on affiliation_units.unitid = sq.unitid INNER JOIN storage_resources on storage_resources.storageid = sq.storageid INNER JOIN groups on groups.groupid = sq.groupid where affiliation_units.name = $3 AND storage_resources.type = $2 and groups.name = $1`, groupname, resource, exptname)
	if err != nil {	
		defer log.WithFields(QueryFields(r, startTime)).Print("Error in DB query: " + err.Error())
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error in DB query.\" }")
		
		return
	}
		
	defer rows.Close()	
	idx := 0
	output := ""
	type jsonout struct {
		Value string `json:"value"`
		Unit string `json:"unit"`
		ValidUntil string `json:"valid_until"`
	}
	var Out jsonout
		for rows.Next() {
		if idx != 0 {
			output += ","
		}
	var tmpValue,tmpUnit,tmpValid sql.NullString
		rows.Scan(&tmpValue,&tmpUnit,&tmpValid)
		if tmpValue.Valid {
			Out.Value, Out.Unit, Out.ValidUntil = tmpValue.String, tmpUnit.String, tmpValid.String
			outline, jsonerr := json.Marshal(Out)
			if jsonerr != nil {
				log.WithFields(QueryFields(r, startTime)).Error(jsonerr)
			}
			output += string(outline)
			idx ++
		}
		}
	if idx == 0 {
		w.WriteHeader(http.StatusNotFound)
		output += `"ferry_error": "Group has no quotas registered."`
		log.WithFields(QueryFields(r, startTime)).Error("Group has no quotas registered.")
	} else {
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
	}
	fmt.Fprintf(w,output)	
}

func setGroupStorageQuota(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

	authorized,authout := authorize(r,AuthorizedDNs)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w,"{ \"ferry_error\": \"" + authout + "not authorized.\" }")
		return
	}

	q := r.URL.Query()
	gName := q.Get("groupname")
	rName := q.Get("resourcename")
	groupquota := q.Get("quota")
	unitName := q.Get("unitname")
	validtime := q.Get("valid_until")
	unit := q.Get("unit")

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
	} else {
		validtime = "valid_until = " + validtime + ", "
	}
	if unit == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No quita unit specified in http query.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No unit specified.\" }")
		return
	}
	cKey, err := DBtx.Start(DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
	}


	_, err = DBtx.Exec(fmt.Sprintf(`do $$
							declare vSid int;
							declare vGid int;
                                                        declare vUnitid int; 
							begin
								select storageid into vSid from storage_resources where name = '%s';
								select gid into vGid from groups where name = '%s';
								select unitid into vUnitid from affiliation_units where name = '%s';

								if vSid is null then raise 'Resource does not exist.'; end if;
								if vGid is null then raise 'Group does not exist.'; end if;
								if vUnitid is null then raise 'Unit does not exist.'; end if;
										
								update storage_quota set value = '%s', unit = '%s', %s last_updated = NOW()
								where storageid = vSid and groupid = vGid and unitid = vUnitid;
							end $$;`, rName, gName, unitName, groupquota, unit, validtime))
	if err == nil {
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		fmt.Fprintf(w,"{ \"status\": \"success\" }")
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
//	q := r.URL.Query()
//	uname := q.Get("username")
//	groupname := q.Get("groupname")
//	resource := q.Get("resourcename")    
	NotDoneYet(w, r, startTime)
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
	authorized,authout := authorize(r,AuthorizedDNs)
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
		w.WriteHeader(http.StatusNotFound)
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
		cKey, err := DBtx.Start(DBptr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Error starting DB transaction: " + err.Error())
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w,"{ \"ferry_error\": \"Error starting database transaction.\" }")
			return
		}
		
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
	
	rows, err := DBptr.Query(`select name, groupid from groups`)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		log.WithFields(QueryFields(r, startTime)).Error("Error in DB query: " + err.Error())
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error in DB query.\" }")
		return
	}
	defer rows.Close()
	
	type jsonout struct {
		Groupname string `json:"name"`
		Grpid int `json:"groupid"`
		
	} 
	var tmpout jsonout
	var Out []jsonout
	
	for rows.Next() {
		rows.Scan(&tmpout.Groupname,&tmpout.Grpid)
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
