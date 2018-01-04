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
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Print("No groupname specified in http query.")
		fmt.Fprintf(w,"{ \"error\": \"No groupname specified.\" }")
		return
	}
	if gType == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Print("No grouptype specified in http query.")
		fmt.Fprintf(w,"{ \"error\": \"No grouptype specified.\" }")
		return
	}
	if q.Get("gid") != "" {
		gid.Scan(q.Get("gid"))
	}

	authorized,authout := authorize(r,AuthorizedDNs)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w,"{ \"error\": \"" + authout + "not authorized.\" }")
		return
	}

	cKey, err := DBtx.Start(DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Fatal(err)
	}

	_, err = DBtx.Exec("insert into groups (gid, name, type, last_updated) values ($1, $2, $3, NOW())", gid, gName, gType)
	if err == nil {
		DBtx.Commit(cKey)
		fmt.Fprintf(w,"{ \"status\": \"success\" }")
	} else {
		if strings.Contains(err.Error(), `invalid input value for enum groups_group_type`) {
			fmt.Fprintf(w,"{ \"error\": \"Invalid grouptype specified in http query.\" }")
		} else if strings.Contains(err.Error(), `duplicate key value violates unique constraint "idx_groups_gid"`) {
			fmt.Fprintf(w,"{ \"error\": \"GID already exists.\" }")
		} else if strings.Contains(err.Error(), `duplicate key value violates unique constraint "idx_groups_group_name"`) {
			fmt.Fprintf(w,"{ \"error\": \"Group already exists.\" }")
		} else {
			log.WithFields(QueryFields(r, startTime)).Print(err.Error())
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
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w,"{ \"error\": \"Invalid value for is_primary (must be 0 or 1).\" }")
			return
		}
	}
	if groupname == "" {	
		log.WithFields(QueryFields(r, startTime)).Print("No groupname specified.")
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w,"{ \"error\": \"No groupname specified\" }")
		return
	}
	if unitName == "" {	
		log.WithFields(QueryFields(r, startTime)).Print("No unitname specified.")
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w,"{ \"error\": \"No unitname specified\" }")
		return
	}

	authorized,authout := authorize(r,AuthorizedDNs)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w,"{ \"error\": \"" + authout + "not authorized.\" }")
		return
	}
	//make sure that the requested group and unit exist; bail out if they don't
	var unitId,groupId int
	checkerr := DBptr.QueryRow(`select unitid from affiliation_units where name=$1`,unitName).Scan(&unitId)
	log.WithFields(QueryFields(r, startTime)).Print("unitID = " + strconv.Itoa(unitId))
	switch {
	case checkerr == sql.ErrNoRows:
		log.WithFields(QueryFields(r, startTime)).Print("Affiliation unit " + unitName + " does not exist.")
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"error\": \"Affiliation unit " + unitName + " does not exist.\" }")
		return
	case checkerr != nil:
		log.WithFields(QueryFields(r, startTime)).Print("Affiliation unit query error: " + checkerr.Error())
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w,"{ \"error\": \"Error in DB query.\" }")
		return
	default:
		grouperr := DBptr.QueryRow(`select groupid from groups where name=$1`,groupname).Scan(&groupId)
		log.WithFields(QueryFields(r, startTime)).Print(" group ID = " + strconv.Itoa(groupId))
		switch {
		case grouperr == sql.ErrNoRows:
			log.WithFields(QueryFields(r, startTime)).Print("Group " + groupname + " does not exist.")
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w,"{ \"error\": \"Group " + groupname + " does not exist.\" }")
			return
		case grouperr != nil:
			log.WithFields(QueryFields(r, startTime)).Print("Group query error: " + checkerr.Error())
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w,"{ \"error\": \"Error in DB query.\" }")
			return
		default:
			// OK, both group and unit exist. Let's get down to business. Check if it's already in affiliation_unit_groups
			
			// start a transaction
			cKey, err := DBtx.Start(DBptr)
			if err != nil {
				log.WithFields(QueryFields(r, startTime)).Print("Error starting DB transaction: " + err.Error())
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprintf(w,"{ \"error\": \"Error starting database transaction.\" }")
				return
			}
			
			addstr := fmt.Sprintf(`do $$ begin if exists (select groupid, unitid from affiliation_unit_group where groupid=%d and unitid=%d) then raise 'Group and unit combination already in DB.'; else 
insert into affiliation_unit_group (groupid, unitid, is_primary, last_updated) values (%d, %d, %d, NOW()); end if ; end $$;`, groupId, unitId, groupId,unitId,isPrimary)
			stmt, err := DBtx.tx.Prepare(addstr)
			if err != nil {
				log.WithFields(QueryFields(r, startTime)).Print("Error preparing DB command: " + err.Error())
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprintf(w,"{ \"error\": \"Error preparing database command.\" }")
//				DBtx.Rollback()
				return
			}
			//run said statement and check errors
			_, err = stmt.Exec()
			if err != nil {
				if strings.Contains(err.Error(),`Group and unit combination already in DB`) {
					w.WriteHeader(http.StatusBadRequest)
					log.WithFields(QueryFields(r, startTime)).Print("Error adding " + groupname + " to " + unitName + "groups: " + err.Error())
					fmt.Fprintf(w,"{ \"error\": \"Group already belongs to unit.\" }")
				} else {
					w.WriteHeader(http.StatusInternalServerError)
					log.WithFields(QueryFields(r, startTime)).Print("Error adding " + groupname + " to " + unitName + "groups: " + err.Error())
					fmt.Fprintf(w,"{ \"error\": \"Error executing DB insert.\" }")		
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
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w,"{ \"error\": \"No groupname specified\" }")
		return
	}
	if unitName == "" {	
		log.WithFields(QueryFields(r, startTime)).Print("No unitname specified.")
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w,"{ \"error\": \"No unitname specified\" }")
		return
	}

	authorized,authout := authorize(r,AuthorizedDNs)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w,"{ \"error\": \"" + authout + "not authorized.\" }")
		return
	}
	
	cKey, err := DBtx.Start(DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Print("Error starting DB transaction: " + err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w,"{ \"error\": \"Error starting database transaction.\" }")
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
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w,"{ \"error\": \"Error preparing database command.\" }")
		return
	}
	//run said statement and check errors
	_, err = stmt.Exec()
	if err != nil {
		if strings.Contains(err.Error(),`Group does not exist`) {
			w.WriteHeader(http.StatusBadRequest)
			log.WithFields(QueryFields(r, startTime)).Print("Error adding " + groupname + " to " + unitName + "groups: " + err.Error())
			fmt.Fprintf(w,"{ \"error\": \"Group does not exist.\" }")
		} else if strings.Contains(err.Error(),`Unit does not exist`) {
			w.WriteHeader(http.StatusBadRequest)
			log.WithFields(QueryFields(r, startTime)).Print("Error adding " + groupname + " to " + unitName + "groups: " + err.Error())
			fmt.Fprintf(w,"{ \"error\": \"Unit does not exist.\" }")
		} else {
			w.WriteHeader(http.StatusInternalServerError)
			log.WithFields(QueryFields(r, startTime)).Print("Error adding " + groupname + " to " + unitName + "groups: " + err.Error())
			fmt.Fprintf(w,"{ \"error\": \"Error executing DB insert.\" }")		
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
//	q := r.URL.Query() 
//	groupname := q.Get("groupname")
//	//should be a bool
//	getLeaders := q.Get("return_leaders")
	NotDoneYet(w, r, startTime)
}
func IsUserLeaderOf(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query()
//	uname := q.Get("username")
//	groupname := q.Get("groupname")
	NotDoneYet(w, r, startTime)
}
func setGroupLeader(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query()
//	uname := q.Get("username")
//	groupname := q.Get("groupname")
	NotDoneYet(w, r, startTime)
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
//	q := r.URL.Query()
//	groupname := q.Get("groupname")
////should be a bool
//	expt_only := q.Get("experimentsonly")
	NotDoneYet(w, r, startTime)
}
func getGroupBatchPriorities(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	groupname := q.Get("groupname")
//	resource := q.Get("resourcename")
	if groupname == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Print("No groupname specified in http query.")
		fmt.Fprintf(w,"{ \"error\": \"No groupname specified.\" }")
		return
	}
	
	NotDoneYet(w, r, startTime)
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
		fmt.Fprintf(w,"{ \"error\": \"" + authout + "not authorized.\" }")
		return
	}

	if group == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Print("No groupname specified in http query.")
		fmt.Fprintf(w,"{ \"error\": \"No groupname specified.\" }")
		return
	}
	if comp == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Print("No resourcename specified in http query.")
		fmt.Fprintf(w,"{ \"error\": \"No resourcename specified.\" }")
		return
	}
	if quota == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Print("No quota specified in http query.")
		fmt.Fprintf(w,"{ \"error\": \"No quota specified.\" }")
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
		log.WithFields(QueryFields(r, startTime)).Fatal(err)
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
		fmt.Fprintf(w,"{ \"status\": \"success\" }")
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		if strings.Contains(err.Error(), `duplicate key value violates unique constraint`) {
			fmt.Fprintf(w,"{ \"error\": \"This quota already exists\" }")
		} else if strings.Contains(err.Error(), `null value in column "compid"`) {
			fmt.Fprintf(w,"{ \"error\": \"Resource does not exist.\" }")
		} else if strings.Contains(err.Error(), `invalid input syntax for type date`) ||
				  strings.Contains(err.Error(), `date/time field value out of range`) {
			fmt.Fprintf(w,"{ \"error\": \"Invalid expiration date.\" }")
		} else {
			log.WithFields(QueryFields(r, startTime)).Print(err.Error())
			fmt.Fprintf(w,"{ \"error\": \"Something went wrong.\" }")
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
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Print("No group name specified in http query.")
		fmt.Fprintf(w,"{ \"error\": \"No group name specified.\" }")
		return
	}
	if resource == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Print("No storage resource specified in http query.")
		fmt.Fprintf(w,"{ \"error\": \"No storage resource specified.\" }")
		return
	}
	if exptname == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Print("No experiment specified in http query.")
		fmt.Fprintf(w,"{ \"error\": \"No experiment name specified.\" }")
		return
	}

	rows, err := DBptr.Query(`select sq.value, sq.unit, sq.valid_until from storage_quota sq INNER JOIN affiliation_units on affiliation_units.unitid = sq.unitid INNER JOIN storage_resources on storage_resources.storageid = sq.storageid INNER JOIN groups on groups.groupid = sq.groupid where affiliation_units.name = $3 AND storage_resources.type = $2 and groups.name = $1`, groupname, resource, exptname)
	if err != nil {	
		defer log.WithFields(QueryFields(r, startTime)).Fatal(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"error\": \"Error in DB query.\" }")
		
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
				log.WithFields(QueryFields(r, startTime)).Fatal(jsonerr)
			}
			output += string(outline)
			idx ++
		}
		}
	if idx == 0 {
		w.WriteHeader(http.StatusNotFound)
		output += `"error": "Group has no quotas registered."`
	}
	fmt.Fprintf(w,output)	
}

func setGroupStorageQuota(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

	authorized,authout := authorize(r,AuthorizedDNs)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w,"{ \"error\": \"" + authout + "not authorized.\" }")
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
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Print("No group name specified in http query.")
		fmt.Fprintf(w,"{ \"error\": \"No group name specified.\" }")
		return
	}
	if rName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Print("No storage resource specified in http query.")
		fmt.Fprintf(w,"{ \"error\": \"No storage resource specified.\" }")
		return
	}
	if unitName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Print("No experiment specified in http query.")
		fmt.Fprintf(w,"{ \"error\": \"No experiment name specified.\" }")
		return
	}	
	if groupquota == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Print("No quota value specified in http query.")
		fmt.Fprintf(w,"{ \"error\": \"No quota specified.\" }")
		return
	}
	if validtime == "" {
		log.WithFields(QueryFields(r, startTime)).Print("No expire time given; assuming it is indefinite.")
	} else {
		validtime = "valid_until = " + validtime + ", "
	}
	if unit == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.WithFields(QueryFields(r, startTime)).Print("No quita unit specified in http query.")
		fmt.Fprintf(w,"{ \"error\": \"No unit specified.\" }")
		return
	}
	cKey, err := DBtx.Start(DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Fatal(err)
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
		fmt.Fprintf(w,"{ \"status\": \"success\" }")
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		if strings.Contains(err.Error(), `Group does not exist.`) {
			fmt.Fprintf(w,"{ \"error\": \"Group does not exist.\" }")
		} else if strings.Contains(err.Error(), `Resource does not exist.`) {
			fmt.Fprintf(w,"{ \"error\": \"Resource does not exist.\" }")
		} else {
			log.WithFields(QueryFields(r, startTime)).Print(err.Error())
			fmt.Fprintf(w,"{ \"error\": \"Something went wrong.\" }")
		}
	}

	DBtx.Commit(cKey)
}

func setUserAccessToResource(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query()
//	uname := q.Get("username")
//	groupname := q.Get("groupname")
//	resource := q.Get("resourcename")
//	shell := q.Get("shell")
//	homepath := q.Get("home_path")
	NotDoneYet(w, r, startTime)
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
