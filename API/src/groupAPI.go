package main
import (
	"math"
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

// IncludeGroupAPIs includes all APIs described in this file in an APICollection
func IncludeGroupAPIs(c *APICollection) {
	createGroup := BaseAPI {
		InputModel {
			Parameter{GroupName, true},
			Parameter{GroupType, true},
			Parameter{GID, false},
		},
		createGroup,
	}
	c.Add("createGroup", &createGroup)

	addGroupToUnit := BaseAPI {
		InputModel {
			Parameter{GroupName, true},
			Parameter{GroupType, true},
			Parameter{UnitName, true},
			Parameter{Primary, false},
		},
		addGroupToUnit,
	}
	c.Add("addGroupToUnit", &addGroupToUnit)

	setPrimaryStatusGroup := BaseAPI {
		InputModel {
			Parameter{GroupName, true},
			Parameter{UnitName, true},
		},
		setPrimaryStatusGroup,
	}
	c.Add("setPrimaryStatusGroup", &setPrimaryStatusGroup)

	getGroupMembers := BaseAPI {
		InputModel {
			Parameter{GroupName, true},
			Parameter{GroupType, false},
			Parameter{Leader, false},
		},
		getGroupMembers,
	}
	c.Add("getGroupMembers", &getGroupMembers)

	isUserMemberOfGroup := BaseAPI {
		InputModel {
			Parameter{UserName, true},
			Parameter{GroupName, true},
			Parameter{GroupType, false},
		},
		isUserMemberOfGroup,
	}
	c.Add("isUserMemberOfGroup", &isUserMemberOfGroup)

	isUserLeaderOfGroup := BaseAPI {
		InputModel {
			Parameter{UserName, true},
			Parameter{GroupName, true},
			Parameter{GroupType, false},
		},
		isUserLeaderOfGroup,
	}
	c.Add("isUserLeaderOfGroup", &isUserLeaderOfGroup)

	setGroupLeader := BaseAPI {
		InputModel {
			Parameter{UserName, true},
			Parameter{GroupName, true},
			Parameter{GroupType, true},
		},
		setGroupLeader,
	}
	c.Add("setGroupLeader", &setGroupLeader)

	removeGroupLeader := BaseAPI {
		InputModel {
			Parameter{UserName, true},
			Parameter{GroupName, true},
			Parameter{GroupType, true},
		},
		removeGroupLeader,
	}
	c.Add("removeGroupLeader", &removeGroupLeader)

	getGroupUnits := BaseAPI {
		InputModel {
			Parameter{GroupName, true},
			Parameter{GroupType, false},
			Parameter{Experiment, false},
			Parameter{LastUpdated, false},
		},
		getGroupUnits,
	}
	c.Add("getGroupUnits", &getGroupUnits)

	getAllGroups := BaseAPI {
		InputModel {
			Parameter{LastUpdated, false},
		},
		getAllGroups,
	}
	c.Add("getAllGroups", &getAllGroups)

	getAllGroupsMembers := BaseAPI {
		InputModel {
			Parameter{LastUpdated, false},
		},
		getAllGroupsMembers,
	}
	c.Add("getAllGroupsMembers", &getAllGroupsMembers)

	getGroupAccessToResource := BaseAPI {
		InputModel {
			Parameter{UnitName, true},
			Parameter{ResourceName, true},
			Parameter{LastUpdated, false},
		},
		getGroupAccessToResource,
	}
	c.Add("getGroupAccessToResource", &getGroupAccessToResource)

	getBatchPriorities := BaseAPI {
		InputModel {
			Parameter{UnitName, false},
			Parameter{ResourceName, false},
			Parameter{LastUpdated, false},
		},
		getBatchPriorities,
	}
	c.Add("getBatchPriorities", &getBatchPriorities)

	getCondorQuotas := BaseAPI {
		InputModel {
			Parameter{UnitName, false},
			Parameter{ResourceName, false},
		},
		getCondorQuotas,
	}
	c.Add("getCondorQuotas", &getCondorQuotas)

	setCondorQuota := BaseAPI {
		InputModel {
			Parameter{CondorGroup, true},
			Parameter{ResourceName, true},
			Parameter{Quota, true},
			Parameter{ExpirationDate, false},
			Parameter{Surplus, false},
		},
		setCondorQuota,
	}
	c.Add("setCondorQuota", &setCondorQuota)
}

func createGroup(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError
	
	var validType bool

	err := c.DBtx.QueryRow(`select $1 = any (enum_range(null::groups_group_type)::text[])`, i[GroupType]).Scan(&validType)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !validType {
		apiErr = append(apiErr, DefaultAPIError(ErrorInvalidData, GroupType))
		return nil, apiErr
	}

	_, err = c.DBtx.Exec("insert into groups (gid, name, type, last_updated) values ($1, $2, $3, NOW())",
						 i[GID], i[GroupName], i[GroupType])
	if err != nil {
		if strings.Contains(err.Error(), `duplicate key value violates unique constraint "idx_groups_gid"`) {
			apiErr = append(apiErr, DefaultAPIError(ErrorDuplicateData, GID))
		} else if strings.Contains(err.Error(), `duplicate key value violates unique constraint`) {
			apiErr = append(apiErr, DefaultAPIError(ErrorDuplicateData, GroupName))
		} else {
			log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		}
		return nil, apiErr
	}

	return nil, nil
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
func addGroupToUnit(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	groupid	:= NewNullAttribute(GroupID)
	unitid	:= NewNullAttribute(UnitID)
	primary	:= i[Primary].Default(false)
	
	var validType bool

	err := c.DBtx.QueryRow(`select $1 = any (enum_range(null::groups_group_type)::text[])`, i[GroupType]).Scan(&validType)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !validType {
		apiErr = append(apiErr, DefaultAPIError(ErrorInvalidData, GroupType))
		return nil, apiErr
	}

	err = c.DBtx.QueryRow(`select (select groupid from groups where name = $1 and type = $2),
								  (select unitid from affiliation_units where name = $3)`,
						  i[GroupName], i[GroupType], i[UnitName]).Scan(&groupid, &unitid)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !unitid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
	}
	if !groupid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, GroupName))
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`insert into affiliation_unit_group (groupid, unitid, is_primary, last_updated) values ($1, $2, $3, NOW())
						  on conflict (groupid, unitid) do nothing`,
						 groupid, unitid, primary)
	if err != nil && !strings.Contains(err.Error(), "pk_affiliation_unit_group") {
		if strings.Contains(err.Error(), `unq_affiliation_unit_group_unitid_is_primary`) {
			apiErr = append(apiErr, APIError{errors.New("affiliation unit already has a primary group"), ErrorAPIRequirement})
		} else {
			log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		}
		return nil, apiErr
	}

	return nil, nil
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

func setPrimaryStatusGroup(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	groupid	:= NewNullAttribute(GroupID)
	unitid	:= NewNullAttribute(UnitID)

	var groupInUnit bool

	err := c.DBtx.QueryRow(`select (select groupid from groups where name = $1 and type = 'UnixGroup'),
								   (select unitid from affiliation_units where name = $2)`,
						  i[GroupName], i[UnitName]).Scan(&groupid, &unitid)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !unitid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
	}
	if !groupid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, GroupName))
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	err = c.DBtx.QueryRow(`select ($1, $2) in (select groupid, unitid from affiliation_unit_group)`,
						  groupid, unitid).Scan(&groupInUnit)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`update affiliation_unit_group set is_primary = false, last_updated = NOW()
						  where is_primary = true and unitid = $1`, unitid)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if groupInUnit {
		_, err = c.DBtx.Exec(`update affiliation_unit_group set is_primary = true, last_updated = NOW()
							  where groupid = $1 and unitid = $2`, groupid, unitid)
	} else {
		_, err = c.DBtx.Exec(`insert into affiliation_unit_group (groupid, unitid, is_primary, last_updated)
							  values ($1, $2, true, NOW())`, groupid, unitid)
	}
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, nil
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
func getGroupMembers(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	grouptype		:= i[GroupType].Default("UnixGroup")
	groupLeaders	:= i[Leader].Default(false)
	groupid			:= NewNullAttribute(GroupID)
	
	var validType bool

	err := c.DBtx.QueryRow(`select $1 = any (enum_range(null::groups_group_type)::text[])`, grouptype).Scan(&validType)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !validType {
		apiErr = append(apiErr, DefaultAPIError(ErrorInvalidData, GroupType))
		return nil, apiErr
	}

	err = c.DBtx.QueryRow(`select (select groupid from groups where name = $1 and type = $2)`, i[GroupName], grouptype).Scan(&groupid)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !groupid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, GroupName))
		return nil, apiErr
	}

	rows, err := DBptr.Query(`select users.uname, users.uid, user_group.is_leader from
								user_group join
								users using(uid)
							  where
								groupid = $1 and
								(user_group.last_updated>=$2 or $2 is null)`,
							 groupid, i[LastUpdated])
	if err != nil {	
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	type jsonuser map[Attribute]interface{}
	out := make([]jsonuser, 0)
	
	for rows.Next() {
		row := NewMapNullAttribute(UserName, UID, Leader)
		rows.Scan(row[UserName], row[UID], row[Leader])

		if row[UID].Valid {
			entry := make(jsonuser)
			entry[UserName] = row[UserName].Data
			entry[UID] = row[UID].Data
			if groupLeaders.Data.(bool) {
				entry[Leader] = row[Leader].Data
			}
			out = append(out, entry)
		}
	}
	
	return out, nil
}

func isUserMemberOfGroup(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	uid			:= NewNullAttribute(UID)
	groupid		:= NewNullAttribute(GroupID)
	grouptype	:= i[GroupType].Default("UnixGroup")
	
	var validType bool

	err := c.DBtx.QueryRow(`select $1 = any (enum_range(null::groups_group_type)::text[])`, grouptype).Scan(&validType)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !validType {
		apiErr = append(apiErr, DefaultAPIError(ErrorInvalidData, GroupType))
		return nil, apiErr
	}

	err = c.DBtx.QueryRow(`select (select uid from users where uname = $1),
								  (select groupid from groups where name = $2 and type = $3)`,
						  i[UserName], i[GroupName], grouptype).Scan(&uid, &groupid)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !uid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, GroupName))
	}
	if !groupid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, GroupName))
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	var out bool
	err = c.DBtx.QueryRow(`select ($1, $2) in
							(select uid, groupid from user_group join groups using(groupid) where type = $3)`,
						  uid, groupid, grouptype).Scan(&out)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return out, nil
}

func isUserLeaderOfGroup(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	uid			:= NewNullAttribute(UID)
	groupid		:= NewNullAttribute(GroupID)
	grouptype	:= i[GroupType].Default("UnixGroup")
	
	var validType bool

	err := c.DBtx.QueryRow(`select $1 = any (enum_range(null::groups_group_type)::text[])`, grouptype).Scan(&validType)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !validType {
		apiErr = append(apiErr, DefaultAPIError(ErrorInvalidData, GroupType))
		return nil, apiErr
	}

	err = c.DBtx.QueryRow(`select (select uid from users where uname = $1),
								  (select groupid from groups where name = $2 and type = $3)`,
						  i[UserName], i[GroupName], grouptype).Scan(&uid, &groupid)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !uid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, GroupName))
	}
	if !groupid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, GroupName))
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	leader := NewNullAttribute(Leader)
	leader.Scan(false)
	err = c.DBtx.QueryRow(`select is_leader from user_group where uid = $1 and groupid = $2`, uid ,groupid).Scan(&leader)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return leader.Data, nil				
}

func setGroupLeader(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	uid			:= NewNullAttribute(UID)
	groupid		:= NewNullAttribute(GroupID)
	
	var validType bool

	err := c.DBtx.QueryRow(`select $1 = any (enum_range(null::groups_group_type)::text[])`, i[GroupType]).Scan(&validType)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !validType {
		apiErr = append(apiErr, DefaultAPIError(ErrorInvalidData, GroupType))
		return nil, apiErr
	}

	err = c.DBtx.QueryRow(`select (select uid from users where uname = $1),
								  (select groupid from groups where name = $2 and type = $3)`,
						  i[UserName], i[GroupName], i[GroupType]).Scan(&uid, &groupid)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !uid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, GroupName))
	}
	if !groupid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, GroupName))
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`insert into user_group (uid, groupid, is_leader) values($1, $2, true)
						  on conflict (uid, groupid) do update set is_leader = true`,
						 uid, groupid)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	
	return nil, nil
}

func removeGroupLeader(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	uid			:= NewNullAttribute(UID)
	groupid		:= NewNullAttribute(GroupID)
	
	var validType bool

	err := c.DBtx.QueryRow(`select $1 = any (enum_range(null::groups_group_type)::text[])`, i[GroupType]).Scan(&validType)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !validType {
		apiErr = append(apiErr, DefaultAPIError(ErrorInvalidData, GroupType))
		return nil, apiErr
	}

	err = c.DBtx.QueryRow(`select (select uid from users where uname = $1),
								  (select groupid from groups where name = $2 and type = $3)`,
						  i[UserName], i[GroupName], i[GroupType]).Scan(&uid, &groupid)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !uid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, GroupName))
	}
	if !groupid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, GroupName))
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`update user_group set is_leader = false, last_updated = NOW() where uid = $1 and groupid = $2`,
						 uid, groupid)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, nil
}

func getGroupUnits(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	groupid		:= NewNullAttribute(GroupID)
	groupType	:= i[GroupType].Default("UnixGroup")
	experiment	:= i[Experiment].Default(false)
	
	var validType bool

	err := c.DBtx.QueryRow(`select $1 = any (enum_range(null::groups_group_type)::text[])`, groupType).Scan(&validType)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !validType {
		apiErr = append(apiErr, DefaultAPIError(ErrorInvalidData, GroupType))
		return nil, apiErr
	}

	err = c.DBtx.QueryRow(`select groupid from groups where name = $1 and type = $2`,
						  i[GroupName], groupType).Scan(&groupid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !groupid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, GroupName))
		return nil, apiErr
	}

	rows, err := DBptr.Query(`select name, type, url, alternative_name from
									affiliation_unit_group as ag join
									affiliation_units using(unitid) left join
									voms_url as vu using(unitid)
								where groupid = $1 and ((url is not null = $2) or not $2)
								and (vu.last_updated>=$3 or ag.last_updated>=$3 or $3 is null)`,
								groupid, experiment, i[LastUpdated])
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	type jsonentry map[Attribute]interface{}
	out := make([]jsonentry, 0)

	for rows.Next() {
		row := NewMapNullAttribute(UnitName, UnitType, VOMSURL, AlternativeName)
		rows.Scan(row[UnitName], row[UnitType], row[VOMSURL], row[AlternativeName])

		if row[UnitName].Valid {
			out = append(out, jsonentry{
				UnitName:			row[UnitName].Data,
				UnitType:			row[UnitType].Data,
				VOMSURL:			row[VOMSURL].Data,
				AlternativeName:	row[AlternativeName].Data,
			})
		}
	}

	return out, nil
}

func getBatchPriorities(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	unitid := NewNullAttribute(UnitID)
	compid := NewNullAttribute(ResourceID)

	err := c.DBtx.QueryRow(`select (select unitid from affiliation_units where name = $1),
								   (select compid from compute_resources where name = $2)`,
						   i[UnitName], i[ResourceName]).Scan(&unitid, &compid)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if i[UnitName].Valid && !unitid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
	}
	if i[ResourceName].Valid && !compid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, ResourceName))
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	rows, err := DBptr.Query(`select name, value, valid_until from compute_batch
							  where type = 'priority'
							  and (compid = $1 or $1 is null)
							  and (unitid = $2 or $2 is null)
							  and (last_updated >= $3 or $3 is null)`, compid, unitid, i[LastUpdated])
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	type jsonpriority = map[Attribute]interface{}
	out := make([]jsonpriority, 0)

	for rows.Next() {
		row := NewMapNullAttribute(CondorGroup, Value, ExpirationDate)
		rows.Scan(row[CondorGroup], row[Value], row[ExpirationDate])
		
		priority := make(jsonpriority)
		priority[CondorGroup] = row[CondorGroup].Data
		priority[Value] = row[Value].Data
		if row[ExpirationDate].Valid {
			priority[ExpirationDate] = row[ExpirationDate].Data
		}
		
		out = append(out, priority)
	}
	
	return out, nil
}

func getCondorQuotas(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	unitid := NewNullAttribute(UnitID)
	compid := NewNullAttribute(ResourceID)

	err := c.DBtx.QueryRow(`select (select unitid from affiliation_units where name = $1),
								   (select compid from compute_resources where name = $2)`,
						   i[UnitName], i[ResourceName]).Scan(&unitid, &compid)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if i[UnitName].Valid && !unitid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
	}
	if i[ResourceName].Valid && !compid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, ResourceName))
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	rows, err := c.DBtx.Query(`select cr.name, au.name, cb.name, value, cb.type, surplus, cb.valid_until
								from compute_batch as cb
								left join affiliation_units as au using(unitid)
								join compute_resources as cr using(compid)
							   where cb.type in ('static', 'dynamic')
								and (cb.unitid = $1 or $1 is null)
								and (cb.compid = $2 or $2 is null)
								and (valid_until is null or valid_until >= NOW())
							   order by cb.name, valid_until desc`, unitid, compid)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	type jsonquota map[Attribute]interface{}
	out := make(map[string][]jsonquota)

	prevGroup := NewNullAttribute(CondorGroup)
	for rows.Next() {
		row := NewMapNullAttribute(ResourceName, UnitName, CondorGroup, Value, ResourceType, Surplus, ExpirationDate)
		rows.Scan(row[ResourceName], row[UnitName], row[CondorGroup], row[Value], row[ResourceType], row[Surplus], row[ExpirationDate])
		if row[CondorGroup].Valid {
			if *row[CondorGroup] != prevGroup {
				out[row[ResourceName].Data.(string)] = append(out[row[ResourceName].Data.(string)], jsonquota{
					CondorGroup: row[CondorGroup].Data,
					Value: row[Value].Data,
					ResourceType: row[ResourceType].Data,
					UnitName: row[UnitName].Data,
					Surplus: row[Surplus].Data,
					ExpirationDate: row[ExpirationDate].Data,
				})
			} else {
				out[row[ResourceName].Data.(string)][len(out[row[ResourceName].Data.(string)]) - 1] = jsonquota{
					CondorGroup: row[CondorGroup].Data,
					Value: row[Value].Data,
					ResourceType: row[ResourceType].Data,
					UnitName: row[UnitName].Data,
					Surplus: row[Surplus].Data,
					ExpirationDate: row[ExpirationDate].Data,
				}
			}
		}
		prevGroup = *row[CondorGroup]
	}

	return out, nil
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
func setCondorQuota(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	var quota string
	var quotaType string
	var baseQuota bool

	unitid := NewNullAttribute(UnitID)
	compid := NewNullAttribute(ResourceID)

	unitName := strings.Split(i[CondorGroup].Data.(string), ".")[0]
	condorGroup := i[CondorGroup].Data.(string)

	err := c.DBtx.QueryRow(`select (select unitid from affiliation_units where name = $1),
								   (select compid from compute_resources where name = $2),
								   (select $1 in (select name from compute_batch))`,
						   unitName, i[ResourceName]).Scan(&unitid, &compid, &baseQuota)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if strings.Contains(condorGroup, ".") {
		quotaType = "dynamic"
		fQuota, _ := i[Quota].Data.(float64)
		if fQuota < 0 || fQuota > 1 {
			apiErr = append(apiErr, APIError{errors.New("subgroup quota must be a float between 0 and 1"), ErrorAPIRequirement})
		}
		quota = strconv.FormatFloat(fQuota, 'f', 2, 64)
	} else {
		quotaType = "static"
		m := math.Mod(i[Quota].Data.(float64), 1)
		iQuota := int64(i[Quota].Data.(float64))
		if m != 0 {
			apiErr = append(apiErr, APIError{errors.New("top-level quota must be an integer"), ErrorAPIRequirement})
		}
		quota = strconv.FormatInt(iQuota, 10)
	}
	if quotaType == "dynamic" {
		if !unitid.Valid {
			apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
		}
		if !baseQuota {
			apiErr = append(apiErr, APIError{errors.New("base level quota does not exist"), ErrorAPIRequirement})
		}
	}
	if !compid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, ResourceName))
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`insert into compute_batch (compid, name, value, type, unitid, surplus, valid_until, last_updated)
						  values ($1, $2, $3, $4, $5, coalesce($6, true), $7, NOW())
						  on conflict (compid, name) where (valid_until is null) = ($7 is null) do
						  update set value = $3, valid_until = $7, surplus = coalesce($6, compute_batch.surplus), last_updated = NOW()`,
						 compid, condorGroup, quota, quotaType, unitid, i[Surplus], i[ExpirationDate])
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, nil
}

func removeCondorQuota(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	group := strings.TrimSpace(q.Get("condorgroup"))
	comp  := strings.TrimSpace(q.Get("resourcename"))

	authorized,authout := authorize(r)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w,"{ \"ferry_error\": \"" + authout + "not authorized.\" }")
		return
	}

	type jsonerror struct {
		Error []string `json:"ferry_error"`
	}

	var inputErr jsonerror

	if group == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No condorgroup specified in http query.")
		inputErr.Error = append(inputErr.Error, "No condorgroup specified.")
	}
	if comp == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No resourcename specified in http query.")
		inputErr.Error = append(inputErr.Error, "No resourcename specified.")
	}

	if len(inputErr.Error) > 0 {
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
	defer DBtx.Rollback(cKey)

	res, err := DBtx.Exec(`DELETE FROM compute_batch
						   WHERE compid = (SELECT compid FROM compute_resources WHERE name = $1)
						   AND name = $2;`, comp, group)

	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err.Error())
		fmt.Fprintf(w,"{ \"ferry_error\": \"Something went wrong.\" }")
		return
	} else {
		n, err := res.RowsAffected()
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error(err.Error())
			fmt.Fprintf(w,"{ \"ferry_error\": \"Error counting rows affected.\" }")
			return
		}
		if n == 0 {
			log.WithFields(QueryFields(r, startTime)).Error("Quota does not exist.")
			fmt.Fprintf(w,"{ \"ferry_error\": \"Quota does not exist.\" }")
			return
		}
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		fmt.Fprintf(w,"{ \"ferry_status\": \"success\" }")
	}

	DBtx.Commit(cKey)
}

func getGroupStorageQuota(w http.ResponseWriter, r *http.Request) {
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
								where affiliation_units.name = $4 AND storage_resources.name = $3 and (groups.name, groups.type) = ($1, $2)
								and (valid_until is null or valid_until >= NOW()) and (sq.last_updated>=$5 or $5 is null)
								order by valid_until desc
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
	var Out jsonentry
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
			Out = jsonentry{tmpValue.String, tmpUnit.String, tmpValid.String}
		}
		}
	
	var output interface{}
	if Out.Value == "" {
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
					 $3 in (select name from compute_resources),
					 (select is_primary from compute_access_group as cg
										join users as u on cg.uid = u.uid
										join groups as g on cg.groupid = g.groupid
										join compute_resources as cr on cg.compid = cr.compid
										where u.uname = $1 and g.name = $2 and cr.name = $3),
					 (select count(*) from compute_access_group as cg
										join users as u on cg.uid = u.uid
										join compute_resources as cr on cg.compid = cr.compid
										where u.uname = $1 and cr.name = $3)`

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
	var userExists, groupExists, resourceExists, isPrimary bool
	var groupCount int
	if rows.Next() {
		rows.Scan(&userExists, &groupExists, &resourceExists, &isPrimary, &groupCount)
	}
	rows.Close()

	if isPrimary && groupCount > 1 {
		log.Error("Trying to remove a primary group.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"Trying to remove a primary group. Set another group as primary or remove other groups prior to this one.\" }")
		return
	}

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
			output = jsonstatus{"success", ""}
		}
		DBtx.Commit(cKey)
	}

	jsonoutput, err := json.Marshal(output)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err.Error())
	}
	if cKey != 0 {
		fmt.Fprintf(w, string(jsonoutput))
	}
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

func getAllGroups(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	rows, err := DBptr.Query(`select name, type, gid from groups where groups.last_updated>=$1 or $1 is null`, i[LastUpdated])
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	type jsonentry map[Attribute]interface{}
	out := make([]jsonentry, 0)
	
	for rows.Next() {
		row := NewMapNullAttribute(GroupName, GroupType, GID)
		rows.Scan(row[GroupName], row[GroupType], row[GID])
		out = append(out, jsonentry{
			GroupName: 	row[GroupName].Data,
			GroupType: 	row[GroupType].Data,
			GID: 		row[GID].Data,
		})
	}

	return out, nil
}

func getAllGroupsMembers(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	rows, err := DBptr.Query(`select name, type, gid, uname, uid
							  from user_group as ug
							  join users using(uid)
							  right join groups as g using(groupid)
							  where ug.last_updated >= $1 or g.last_updated >= $1 or $1 is null
							  order by name, type;`, i[LastUpdated])
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	type jsonentry map[Attribute]interface{}

	const Members Attribute = "members"
	group := jsonentry {
		GroupName: 	"",
		GroupType: 	"",
		GID:		0,
		Members:	make([]jsonentry, 0),
	}
	out := make([]jsonentry, 0)
	
	for rows.Next() {
		row := NewMapNullAttribute(GroupName, GroupType, GID, UserName, UID)
		rows.Scan(row[GroupName], row[GroupType], row[GID], row[UserName], row[UID])
		if row[GroupName].Data != group[GroupName] {
			if group[GroupName] != "" {
				out = append(out, group)
			}
			group = jsonentry {
				GroupName: 	row[GroupName].Data,
				GroupType: 	row[GroupType].Data,
				GID:		row[GID].Data,
				Members:	make([]jsonentry, 0),
			}
			if row[UserName].Data != "" {
				group[Members] = append(group[Members].([]jsonentry), jsonentry {
					UserName:	row[UserName].Data,
					UID:		row[UID].Data,
				})
			}
		} else {
			group[Members] = append(group[Members].([]jsonentry), jsonentry {
				UserName:	row[UserName].Data,
				UID:		row[UID].Data,
			})
		}
	}
	out = append(out, group)

	return out, nil
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
	
	queryerr := tx.tx.QueryRow(`select	(select storageid from storage_resources where name = $1),
										(select groupid from groups where name = $2 and type = 'UnixGroup'),
										(select unitid from affiliation_units where name = $3)`,
								rName, gName, unitname).Scan(&vSid, &vGid, &vUnitid)
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
		queryerr = tx.tx.QueryRow(`select valid_until from storage_quota where storageid = $1 and unitid = $2 and groupid = $3 and valid_until is not null`,vSid, vUnitid, vGid).Scan(&vValid)
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
			
			_, reterr = tx.Exec(`update storage_quota set value = $1, unit = $2, valid_until = $6, last_updated = NOW()
				   where storageid = $3 and groupid = $4 and unitid = $5 and valid_until is not null`, groupquota, quotaunit, vSid, vGid, vUnitid, vValid)
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

func getGroupAccessToResource(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	unitid		:= NewNullAttribute(UnitID)
	resourceid	:= NewNullAttribute(ResourceID)

	err := c.DBtx.QueryRow(`select (select unitid from affiliation_units where name = $1),
								   (select compid from compute_resources where name = $2)`,
						  i[UnitName], i[ResourceName]).Scan(&unitid, &resourceid)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !unitid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
	}
	if !resourceid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, ResourceName))
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	out := make([]interface{}, 0)
	rows, err := DBptr.Query(`select name from groups where groupid in (
								select distinct groupid from compute_access as ca
								join compute_access_group using(compid, uid)
								join compute_resources using(compid)
								where compid=$1 and unitid=$2
								and (ca.last_updated>=$3 or $3 is null)
							  ) order by groups.name`, resourceid, unitid, i[LastUpdated])
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	for rows.Next() {
		row := NewNullAttribute(GroupName)
		rows.Scan(&row)
		out = append(out, row.Data)
	}
	
	return out, nil
}
