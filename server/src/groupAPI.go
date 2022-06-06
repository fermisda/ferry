package main

import (
	"database/sql"
	"fmt"
	"math"
	"strconv"
	"strings"

	_ "github.com/lib/pq"
	log "github.com/sirupsen/logrus"

	//	"io/ioutil"
	"errors"
)

// IncludeGroupAPIs includes all APIs described in this file in an APICollection
func IncludeGroupAPIs(c *APICollection) {
	createGroup := BaseAPI{
		InputModel{
			Parameter{GroupName, true},
			Parameter{GroupType, false},
			Parameter{GID, false},
		},
		createGroup,
		RoleWrite,
	}
	c.Add("createGroup", &createGroup)

	addGroupToUnit := BaseAPI{
		InputModel{
			Parameter{GroupName, true},
			Parameter{GroupType, true},
			Parameter{UnitName, true},
			Parameter{Primary, false},
			Parameter{Required, false},
		},
		addGroupToUnit,
		RoleWrite,
	}
	c.Add("addGroupToUnit", &addGroupToUnit)

	setGroupRequired := BaseAPI{
		InputModel{
			Parameter{GroupName, true},
			Parameter{GroupType, true},
			Parameter{UnitName, true},
			Parameter{Required, true},
		},
		setGroupRequired,
		RoleWrite,
	}
	c.Add("setGroupRequired", &setGroupRequired)

	removeGroupFromUnit := BaseAPI{
		InputModel{
			Parameter{GroupName, true},
			Parameter{GroupType, true},
			Parameter{UnitName, true},
		},
		removeGroupFromUnit,
		RoleWrite,
	}
	c.Add("removeGroupFromUnit", &removeGroupFromUnit)

	setPrimaryStatusGroup := BaseAPI{
		InputModel{
			Parameter{GroupName, true},
			Parameter{UnitName, true},
		},
		setPrimaryStatusGroup,
		RoleWrite,
	}
	c.Add("setPrimaryStatusGroup", &setPrimaryStatusGroup)

	getGroupMembers := BaseAPI{
		InputModel{
			Parameter{GroupName, true},
			Parameter{GroupType, false},
			Parameter{Leader, false},
		},
		getGroupMembers,
		RoleRead,
	}
	c.Add("getGroupMembers", &getGroupMembers)

	isUserMemberOfGroup := BaseAPI{
		InputModel{
			Parameter{UserName, true},
			Parameter{GroupName, true},
			Parameter{GroupType, false},
		},
		isUserMemberOfGroup,
		RoleRead,
	}
	c.Add("isUserMemberOfGroup", &isUserMemberOfGroup)

	isUserLeaderOfGroup := BaseAPI{
		InputModel{
			Parameter{UserName, true},
			Parameter{GroupName, true},
			Parameter{GroupType, false},
		},
		isUserLeaderOfGroup,
		RoleRead,
	}
	c.Add("isUserLeaderOfGroup", &isUserLeaderOfGroup)

	setGroupLeader := BaseAPI{
		InputModel{
			Parameter{UserName, true},
			Parameter{GroupName, true},
			Parameter{GroupType, true},
		},
		setGroupLeader,
		RoleWrite,
	}
	c.Add("setGroupLeader", &setGroupLeader)

	removeGroupLeader := BaseAPI{
		InputModel{
			Parameter{UserName, true},
			Parameter{GroupName, true},
			Parameter{GroupType, true},
		},
		removeGroupLeader,
		RoleWrite,
	}
	c.Add("removeGroupLeader", &removeGroupLeader)

	getGroupUnits := BaseAPI{
		InputModel{
			Parameter{GroupName, true},
			Parameter{GroupType, false},
			Parameter{Experiment, false},
			Parameter{LastUpdated, false},
		},
		getGroupUnits,
		RoleRead,
	}
	c.Add("getGroupUnits", &getGroupUnits)

	getAllGroups := BaseAPI{
		InputModel{
			Parameter{GroupType, false},
			Parameter{LastUpdated, false},
		},
		getAllGroups,
		RoleRead,
	}
	c.Add("getAllGroups", &getAllGroups)

	getAllGroupsMembers := BaseAPI{
		InputModel{
			Parameter{LastUpdated, false},
		},
		getAllGroupsMembers,
		RoleRead,
	}
	c.Add("getAllGroupsMembers", &getAllGroupsMembers)

	getGroupAccessToResource := BaseAPI{
		InputModel{
			Parameter{UnitName, true},
			Parameter{ResourceName, true},
			Parameter{LastUpdated, false},
		},
		getGroupAccessToResource,
		RoleRead,
	}
	c.Add("getGroupAccessToResource", &getGroupAccessToResource)

	getBatchPriorities := BaseAPI{
		InputModel{
			Parameter{UnitName, false},
			Parameter{ResourceName, false},
			Parameter{LastUpdated, false},
		},
		getBatchPriorities,
		RoleRead,
	}
	c.Add("getBatchPriorities", &getBatchPriorities)

	getCondorQuotas := BaseAPI{
		InputModel{
			Parameter{UnitName, false},
			Parameter{ResourceName, false},
		},
		getCondorQuotas,
		RoleRead,
	}
	c.Add("getCondorQuotas", &getCondorQuotas)

	setCondorQuota := BaseAPI{
		InputModel{
			Parameter{CondorGroup, true},
			Parameter{ResourceName, true},
			Parameter{Quota, true},
			Parameter{ExpirationDate, false},
			Parameter{Surplus, false},
		},
		setCondorQuota,
		RoleWrite,
	}
	c.Add("setCondorQuota", &setCondorQuota)

	removeCondorQuota := BaseAPI{
		InputModel{
			Parameter{CondorGroup, true},
			Parameter{ResourceName, true},
		},
		removeCondorQuota,
		RoleWrite,
	}
	c.Add("removeCondorQuota", &removeCondorQuota)

	getGroupStorageQuota := BaseAPI{
		InputModel{
			Parameter{GroupName, true},
			Parameter{ResourceName, true},
			Parameter{UnitName, true},
			Parameter{QuotaUnit, false},
			Parameter{LastUpdated, false},
		},
		getGroupStorageQuota,
		RoleRead,
	}
	c.Add("getGroupStorageQuota", &getGroupStorageQuota)

	removeUserAccessFromResource := BaseAPI{
		InputModel{
			Parameter{UserName, true},
			Parameter{ResourceName, true},
			Parameter{GroupName, false},
		},
		removeUserAccessFromResource,
		RoleRead,
	}
	c.Add("removeUserAccessFromResource", &removeUserAccessFromResource)
}

func createGroup(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	var validType bool

	if strings.Contains(i[GroupName].Data.(string), " ") {
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "Spaces are not allowed in a group's name."))
		return nil, apiErr
	}

	err := c.DBtx.QueryRow(`select $1 = any (enum_range(null::groups_group_type)::text[])`, i[GroupType]).Scan(&validType)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !validType {
		apiErr = append(apiErr, DefaultAPIError(ErrorInvalidData, GroupType))
		return nil, apiErr
	}

	if i[GroupType].Data == "UnixGroup" && !i[GID].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "GID is required for UnixGroup"))
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
			log.WithFields(QueryFields(c)).Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		}
		return nil, apiErr
	}

	return nil, nil
}

func addGroupToUnit(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	groupid := NewNullAttribute(GroupID)
	unitid := NewNullAttribute(UnitID)
	primary := i[Primary].Default(false)
	required := i[Required].Default(false)

	var validType bool

	err := c.DBtx.QueryRow(`select $1 = any (enum_range(null::groups_group_type)::text[])`, i[GroupType]).Scan(&validType)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
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
		log.WithFields(QueryFields(c)).Error(err)
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

	_, err = c.DBtx.Exec(`insert into affiliation_unit_group (groupid, unitid, is_primary, is_required, last_updated) values ($1, $2, $3, $4, NOW())
						  on conflict (groupid, unitid) do nothing`,
		groupid, unitid, primary, required)
	if err != nil && !strings.Contains(err.Error(), "pk_affiliation_unit_group") {
		if strings.Contains(err.Error(), `unq_affiliation_unit_group_unitid_is_primary`) {
			apiErr = append(apiErr, APIError{errors.New("affiliation unit already has a primary group"), ErrorAPIRequirement})
		} else {
			log.WithFields(QueryFields(c)).Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		}
		return nil, apiErr
	}

	return nil, nil
}

func setGroupRequired(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	groupid := NewNullAttribute(GroupID)
	unitid := NewNullAttribute(UnitID)

	var validType bool

	err := c.DBtx.QueryRow(`select $1 = any (enum_range(null::groups_group_type)::text[])`, i[GroupType]).Scan(&validType)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
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
		log.WithFields(QueryFields(c)).Error(err)
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

	result, err := c.DBtx.Exec(`update affiliation_unit_group set is_required = $1, last_updated = NOW()
						  where unitid = $2 and groupid = $3`, i[Required], unitid, groupid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	numUpdated, err := result.RowsAffected()
	if numUpdated == 0 {
		_, err = c.DBtx.Exec("insert into affiliation_unit_group (unitid, groupid, is_required, last_updated) values ($1, $2, $3, NOW())", unitid, groupid, i[Required])
	}
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, nil
}

func removeGroupFromUnit(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	groupid := NewNullAttribute(GroupID)
	unitid := NewNullAttribute(UnitID)
	primary := NewNullAttribute(Primary)

	var validType bool

	err := c.DBtx.QueryRow(`select $1 = any (enum_range(null::groups_group_type)::text[])`, i[GroupType]).Scan(&validType)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
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
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	err = c.DBtx.QueryRow(`select is_primary from affiliation_unit_group where groupid = $1 and unitid = $2`,
		groupid, unitid).Scan(&primary)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !unitid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
	}
	if !groupid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, GroupName))
	}
	if primary.Valid && primary.Data.(bool) {
		apiErr = append(apiErr, APIError{errors.New("this is the primary group for the unit"), ErrorAPIRequirement})
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`delete from affiliation_unit_group where groupid = $1 and unitid = $2`, groupid, unitid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, nil
}

func setPrimaryStatusGroup(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	groupid := NewNullAttribute(GroupID)
	unitid := NewNullAttribute(UnitID)

	var groupInUnit bool

	err := c.DBtx.QueryRow(`select (select groupid from groups where name = $1 and type = 'UnixGroup'),
								   (select unitid from affiliation_units where name = $2)`,
		i[GroupName], i[UnitName]).Scan(&groupid, &unitid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
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
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`update affiliation_unit_group set is_primary = false, last_updated = NOW()
						  where is_primary = true and unitid = $1`, unitid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
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
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, nil
}

func getGroupMembers(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	grouptype := i[GroupType].Default("UnixGroup")
	groupLeaders := i[Leader].Default(false)
	groupid := NewNullAttribute(GroupID)

	var validType bool

	err := c.DBtx.QueryRow(`select $1 = any (enum_range(null::groups_group_type)::text[])`, grouptype).Scan(&validType)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !validType {
		apiErr = append(apiErr, DefaultAPIError(ErrorInvalidData, GroupType))
		return nil, apiErr
	}

	err = c.DBtx.QueryRow(`select (select groupid from groups where name = $1 and type = $2)`, i[GroupName], grouptype).Scan(&groupid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !groupid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, GroupName))
		return nil, apiErr
	}

	rows, err := c.DBtx.Query(`select users.uname, users.uid, user_group.is_leader from
								user_group join
								users using(uid)
							   where
								groupid = $1 and
								(user_group.last_updated>=$2 or $2 is null)`,
		groupid, i[LastUpdated])
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
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

	uid := NewNullAttribute(UID)
	groupid := NewNullAttribute(GroupID)
	grouptype := i[GroupType].Default("UnixGroup")

	var validType bool

	err := c.DBtx.QueryRow(`select $1 = any (enum_range(null::groups_group_type)::text[])`, grouptype).Scan(&validType)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
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
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !uid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
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
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return out, nil
}

func isUserLeaderOfGroup(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	uid := NewNullAttribute(UID)
	groupid := NewNullAttribute(GroupID)
	grouptype := i[GroupType].Default("UnixGroup")

	var validType bool

	err := c.DBtx.QueryRow(`select $1 = any (enum_range(null::groups_group_type)::text[])`, grouptype).Scan(&validType)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
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
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !uid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
	}
	if !groupid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, GroupName))
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	leader := NewNullAttribute(Leader)
	leader.Scan(false)
	err = c.DBtx.QueryRow(`select is_leader from user_group where uid = $1 and groupid = $2`, uid, groupid).Scan(&leader)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return leader.Data, nil
}

func setGroupLeader(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	uid := NewNullAttribute(UID)
	groupid := NewNullAttribute(GroupID)

	var validType bool

	err := c.DBtx.QueryRow(`select $1 = any (enum_range(null::groups_group_type)::text[])`, i[GroupType]).Scan(&validType)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
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
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !uid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
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
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, nil
}

func removeGroupLeader(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	uid := NewNullAttribute(UID)
	groupid := NewNullAttribute(GroupID)

	var validType bool

	err := c.DBtx.QueryRow(`select $1 = any (enum_range(null::groups_group_type)::text[])`, i[GroupType]).Scan(&validType)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
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
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !uid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
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
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, nil
}

func getGroupUnits(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	groupid := NewNullAttribute(GroupID)
	groupType := i[GroupType].Default("UnixGroup")
	experiment := i[Experiment].Default(false)

	var validType bool

	err := c.DBtx.QueryRow(`select $1 = any (enum_range(null::groups_group_type)::text[])`, groupType).Scan(&validType)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
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
		log.WithFields(QueryFields(c)).Error(err)
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
		log.WithFields(QueryFields(c)).Error(err)
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
				UnitName:        row[UnitName].Data,
				UnitType:        row[UnitType].Data,
				VOMSURL:         row[VOMSURL].Data,
				AlternativeName: row[AlternativeName].Data,
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
		log.WithFields(QueryFields(c)).Error(err)
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
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	type jsonpriority map[Attribute]interface{}
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
		log.WithFields(QueryFields(c)).Error(err)
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
		log.WithFields(QueryFields(c)).Error(err)
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
					CondorGroup:    row[CondorGroup].Data,
					Value:          row[Value].Data,
					ResourceType:   row[ResourceType].Data,
					UnitName:       row[UnitName].Data,
					Surplus:        row[Surplus].Data,
					ExpirationDate: row[ExpirationDate].Coalesce(""),
				})
			} else {
				out[row[ResourceName].Data.(string)][len(out[row[ResourceName].Data.(string)])-1] = jsonquota{
					CondorGroup:    row[CondorGroup].Data,
					Value:          row[Value].Data,
					ResourceType:   row[ResourceType].Data,
					UnitName:       row[UnitName].Data,
					Surplus:        row[Surplus].Data,
					ExpirationDate: row[ExpirationDate].Coalesce(""),
				}
			}
		}
		prevGroup = *row[CondorGroup]
	}

	return out, nil
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
		log.WithFields(QueryFields(c)).Error(err)
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
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !i[ExpirationDate].Valid {
		_, err = c.DBtx.Exec(`delete from compute_batch where compid = $1 and name = $2 and valid_until is not null`, compid, condorGroup)
		if err != nil {
			log.WithFields(QueryFields(c)).Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
			return nil, apiErr
		}
	}

	return nil, nil
}

func removeCondorQuota(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	compid := NewNullAttribute(ResourceID)

	err := c.DBtx.QueryRow(`select compid from compute_resources where name = $1`, i[ResourceName]).Scan(&compid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !compid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, ResourceName))
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`delete from compute_batch where compid = $1 and name = $2`, compid, i[CondorGroup])
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, nil
}

func getGroupStorageQuota(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	groupid := NewNullAttribute(GroupID)
	storageid := NewNullAttribute(ResourceID)
	unitid := NewNullAttribute(UnitID)

	err := c.DBtx.QueryRow(`select (select groupid from groups where name = $1 and type = 'UnixGroup'),
								   (select storageid from storage_resources where name = $2),
								   (select unitid from affiliation_units where name = $3)`,
		i[GroupName], i[ResourceName], i[UnitName]).Scan(&groupid, &storageid, &unitid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !groupid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, GroupName))
	}
	if !storageid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, ResourceName))
	}
	if !unitid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
	}
	if i[QuotaUnit].Valid {
		ok := checkUnits(i[QuotaUnit].Data.(string))
		if !ok {
			apiErr = append(apiErr, APIError{errors.New("invalid quotaunit specified in http query"), ErrorAPIRequirement})
		}
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	rows, err := DBptr.Query(`select value, unit, valid_until from storage_quota
							  where groupid = $1 and storageid = $2 and unitid = $3
							  and (valid_until is null or valid_until >= NOW()) and (last_updated>=$4 or $4 is null)
							  order by valid_until desc`,
		groupid, storageid, unitid, i[LastUpdated])
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	type jsonentry map[Attribute]interface{}
	out := make(jsonentry)

	for rows.Next() {
		row := NewMapNullAttribute(Quota, QuotaUnit, ExpirationDate)
		rows.Scan(row[Quota], row[QuotaUnit], row[ExpirationDate])

		if row[Quota].Valid {
			if i[QuotaUnit].Valid && i[QuotaUnit].Data.(string) != row[QuotaUnit].Data.(string) {
				newQuota, err := convertValue(row[Quota].Data, row[QuotaUnit].Data.(string), i[QuotaUnit].Data.(string))
				if err == nil {
					row[Quota].Scan(newQuota)
					row[QuotaUnit].Scan(strings.ToUpper(i[QuotaUnit].Data.(string)))
				}
			}
			out = jsonentry{
				Quota:          row[Quota].Data,
				QuotaUnit:      row[QuotaUnit].Data,
				ExpirationDate: row[ExpirationDate].Data,
			}
		}
	}

	return out, nil
}

func removeUserAccessFromResource(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	uid := NewNullAttribute(UID)
	groupid := NewNullAttribute(GroupID)
	compid := NewNullAttribute(ResourceID)

	var isPrimary sql.NullBool
	var groupCount int64

	err := c.DBtx.QueryRow(`select (select uid from users where uname = $1),
								   (select groupid from groups where name = $2 and type = 'UnixGroup'),
								   (select compid from compute_resources where name = $3),
								   (select is_primary from compute_access_group
									join users using(uid)
									join groups g using(groupid)
									join compute_resources cr using(compid)
									where uname = $1 and g.name = $2 and cr.name = $3),
								   (select count(*) from compute_access_group as cg
									join users as u on cg.uid = u.uid
									join compute_resources as cr on cg.compid = cr.compid
									where u.uname = $1 and cr.name = $3)`,
		i[UserName], i[GroupName], i[ResourceName]).Scan(&uid, &groupid, &compid, &isPrimary, &groupCount)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !uid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
	}
	if !groupid.Valid && i[GroupName].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, GroupName))
	}
	if !compid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, ResourceName))
	}
	if isPrimary.Bool && groupCount > 1 {
		apiErr = append(apiErr, APIError{errors.New("trying to remove a primary group"), ErrorAPIRequirement})
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	res, err := c.DBtx.Exec(`delete from compute_access_group where uid = $1 and (groupid = $2 or $2 is null) and compid = $3`, uid, groupid, compid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	var nRows int64
	if err == nil {
		nRows, _ = res.RowsAffected()
	}

	if nRows == groupCount {
		_, err := c.DBtx.Exec(`delete from compute_access where uid = $1 and compid = $2`, uid, compid)
		if err != nil {
			log.WithFields(QueryFields(c)).Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
			return nil, apiErr
		}
	}

	return nil, nil
}

func getAllGroups(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	var rows *sql.Rows
	var err error
	var validType bool

	groupType := i[GroupType].Default("*")

	err = c.DBtx.QueryRow(`select $1 = any (enum_range(null::groups_group_type)::text[])`, groupType).Scan(&validType)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !validType && groupType.Data.(string) != "*" {
		apiErr = append(apiErr, DefaultAPIError(ErrorInvalidData, GroupType))
		return nil, apiErr
	}

	if validType {
		rows, err = DBptr.Query(`select name, type, gid from groups
		where (groups.last_updated>=$1 or $1 is null) and type = $2`, i[LastUpdated], groupType)
	} else {
		rows, err = DBptr.Query(`select name, type, gid from groups where groups.last_updated>=$1 or $1 is null`, i[LastUpdated])
	}
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	type jsonentry map[Attribute]interface{}
	out := make([]jsonentry, 0)

	for rows.Next() {
		row := NewMapNullAttribute(GroupName, GroupType, GID)
		rows.Scan(row[GroupName], row[GroupType], row[GID])
		if row[GroupName].Data == "celeritas" {
			log.Info(fmt.Sprintf("GID: %s    name: %s     type: %s", row[GID].Data, row[GroupName].Data, row[GroupType].Data))
		}
		out = append(out, jsonentry{
			GroupName: row[GroupName].Data,
			GroupType: row[GroupType].Data,
			GID:       row[GID].Data,
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
							  order by name, type`, i[LastUpdated])
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	type jsonentry map[Attribute]interface{}

	const Members Attribute = "members"
	group := jsonentry{
		GroupName: "",
		GroupType: "",
		GID:       nil,
		Members:   make([]jsonentry, 0),
	}
	out := make([]jsonentry, 0)

	for rows.Next() {
		row := NewMapNullAttribute(GroupName, GroupType, GID, UserName, UID)
		rows.Scan(row[GroupName], row[GroupType], row[GID], row[UserName], row[UID])
		if row[GroupName].Data != group[GroupName] {
			if group[GroupName] != "" {
				out = append(out, group)
			}
			group = jsonentry{
				GroupName: row[GroupName].Data,
				GroupType: row[GroupType].Data,
				Members:   make([]jsonentry, 0),
			}
			if row[GroupType].Data.(string) == "UnixGroup" {
				group[GID] = row[GID].Data
			} else {
				group[GID] = nil
			}
			if row[UserName].Data != "" {
				group[Members] = append(group[Members].([]jsonentry), jsonentry{
					UserName: row[UserName].Data,
					UID:      row[UID].Data,
				})
			}
		} else {
			group[Members] = append(group[Members].([]jsonentry), jsonentry{
				UserName: row[UserName].Data,
				UID:      row[UID].Data,
			})
		}
	}
	out = append(out, group)

	return out, nil
}

func getGroupAccessToResource(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	unitid := NewNullAttribute(UnitID)
	resourceid := NewNullAttribute(ResourceID)

	err := c.DBtx.QueryRow(`select (select unitid from affiliation_units where name = $1),
								   (select compid from compute_resources where name = $2)`,
		i[UnitName], i[ResourceName]).Scan(&unitid, &resourceid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
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
		log.WithFields(QueryFields(c)).Error(err)
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
