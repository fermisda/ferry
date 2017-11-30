package main
import (
	"strings"
	"database/sql"
	"fmt"
	"log"
	_ "github.com/lib/pq"
	"net/http"
	"encoding/json"
)

func createGroup(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	gName := q.Get("groupname")
	gType := q.Get("grouptype")
	var gid sql.NullString

	if gName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No groupname specified in http query.")
		fmt.Fprintf(w,"{ \"error\": \"No groupname specified.\" }")
		return
	}
	if gType == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No grouptype specified in http query.")
		fmt.Fprintf(w,"{ \"error\": \"No grouptype specified.\" }")
		return
	}
	if q.Get("gid") != "" {
		gid.Scan(q.Get("gid"))
	}

	cKey, err := DBtx.Start(DBptr)
	if err != nil {
		log.Fatal(err)
	}

	_, err = DBtx.Exec("insert into groups (gid, name, type, last_updated) values ($1, $2, $3, NOW())", gid, gName, gType)
	if err == nil {
		fmt.Fprintf(w,"{ \"status\": \"success\" }")
	} else {
		if strings.Contains(err.Error(), `invalid input value for enum groups_group_type`) {
			fmt.Fprintf(w,"{ \"error\": \"Invalid grouptype specified in http query.\" }")
		} else if strings.Contains(err.Error(), `duplicate key value violates unique constraint "idx_groups_gid"`) {
			fmt.Fprintf(w,"{ \"error\": \"GID already exists.\" }")
		} else if strings.Contains(err.Error(), `duplicate key value violates unique constraint "idx_groups_group_name"`) {
			fmt.Fprintf(w,"{ \"error\": \"Group already exists.\" }")
		} else {
			log.Print(err.Error())
		}
	}

	DBtx.Commit(cKey)
}

func deleteGroupt(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query()
//	groupname := q.Get("groupname")
	NotDoneYet(w)
}
func deleteGroup(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query()
// should be an int
//	gid := q.Get("gid")
	NotDoneYet(w) 
}
func addGroupToUnit(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query()
//	groupname := q.Get("groupname")
//	collabunit := q.Get("collaboration_unit")
//	// should be a bool
//	isPrimary := q.Get("is_primary")
	NotDoneYet(w)
}
func removeGroupFromUnit(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query()
//	groupname := q.Get("groupname")
//	unitname := q.Get("unitname")
	NotDoneYet(w)
}
func setPrimaryStatusGroup(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query()
//	groupname := q.Get("groupname")
//	collabunit := q.Get("collaboration_unit")
	NotDoneYet(w)
}

func removePrimaryStatusfromGroup(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query()
//	groupname := q.Get("groupname")
//	collabunit := q.Get("collaboration_unit")
	NotDoneYet(w)
}
func getGroupMembers(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query() 
//	groupname := q.Get("groupname")
//	//should be a bool
//	getLeaders := q.Get("return_leaders")
	NotDoneYet(w)
}
func IsUserLeaderOf(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query()
//	uname := q.Get("username")
//	groupname := q.Get("groupname")
	NotDoneYet(w)
}
func setGroupLeader(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query()
//	uname := q.Get("username")
//	groupname := q.Get("groupname")
	NotDoneYet(w)
}
func removeGroupLeader(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query()
//	uname := q.Get("username")
//	groupname := q.Get("groupname")
	NotDoneYet(w)
}
func getGroupUnits(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query()
//	groupname := q.Get("groupname")
////should be a bool
//	expt_only := q.Get("experimentsonly")
	NotDoneYet(w)
}
func getGroupBatchPriorities(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query()
//	groupname := q.Get("groupname")
//	resource := q.Get("resourcename")
//	exptname := q.Get("experimentname")
	NotDoneYet(w)
}
func getGroupCondorQuotas(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query()
//	groupname := q.Get("groupname")
//	resource := q.Get("resourcename")
//	exptname := q.Get("experimentname")
	NotDoneYet(w)
}
func setGroupBatchPriority(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query()
//	groupname := q.Get("groupname")
//	resource  := q.Get("resourcename")
//	// should be an int
//	prio := q.Get("priority")
	NotDoneYet(w)
}
func setGroupCondorQuota(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	group := q.Get("groupname")
	comp  := q.Get("resourcename")
	quota := q.Get("quota")
	until := q.Get("validuntil")

	if group == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No groupname specified in http query.")
		fmt.Fprintf(w,"{ \"error\": \"No groupname specified.\" }")
		return
	}
	if comp == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No resourcename specified in http query.")
		fmt.Fprintf(w,"{ \"error\": \"No resourcename specified.\" }")
		return
	}
	if quota == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No quota specified in http query.")
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
		log.Fatal(err)
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
		if strings.Contains(err.Error(), `duplicate key value violates unique constraint`) {
			fmt.Fprintf(w,"{ \"error\": \"This quota already exists\" }")
		} else if strings.Contains(err.Error(), `null value in column "compid"`) {
			fmt.Fprintf(w,"{ \"error\": \"Resource does not exist.\" }")
		} else if strings.Contains(err.Error(), `invalid input syntax for type date`) ||
				  strings.Contains(err.Error(), `date/time field value out of range`) {
			fmt.Fprintf(w,"{ \"error\": \"Invalid expiration date.\" }")
		} else {
			log.Print(err.Error())
			fmt.Fprintf(w,"{ \"error\": \"Something went wrong.\" }")
		}
	}

	DBtx.Commit(cKey)
}
func getGroupStorageQuotas(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	groupname := q.Get("groupname")
	resource := q.Get("resourcename")
	exptname := q.Get("experimentname")
	if groupname == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No group name specified in http query.")
		fmt.Fprintf(w,"{ \"error\": \"No group name specified.\" }")
		return
	}
	if resource == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No storage resource specified in http query.")
		fmt.Fprintf(w,"{ \"error\": \"No storage resource specified.\" }")
		return
	}
	if exptname == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No experiment specified in http query.")
		fmt.Fprintf(w,"{ \"error\": \"No experiment name specified.\" }")
		return
	}

	rows, err := DBptr.Query(`select sq.value, sq.unit, sq.valid_until from storage_quota sq INNER JOIN affiliation_units on affiliation_units.unitid = sq.unitid INNER JOIN storage_resources on storage_resources.storageid = sq.storageid INNER JOIN groups on groups.groupid = sq.groupid where affiliation_units.name = $3 AND storage_resources.type = $2 and groups.name = $1`, groupname, resource, exptname)
	if err != nil {	
		defer log.Fatal(err)
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
				log.Fatal(jsonerr)
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
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query()
//	groupname := q.Get("groupname")
//	resource := q.Get("resourcename")
////should be an int
//	groupquota := q.Get("quota")
//	unitname := q.Get("unit")
//	expire := q.Get("valid_until")
	NotDoneYet(w)
}
func setUserAccessToResource(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query()
//	uname := q.Get("username")
//	groupname := q.Get("groupname")
//	resource := q.Get("resourcename")
//	shell := q.Get("shell")
//	homepath := q.Get("home_path")
	NotDoneYet(w)
}
func removeUserAccessFromResource(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query()
//	uname := q.Get("username")
//	groupname := q.Get("groupname")
//	resource := q.Get("resourcename")    
	NotDoneYet(w)
}
