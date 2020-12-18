package main

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	_ "github.com/lib/pq"
	log "github.com/sirupsen/logrus"
)

// IncludeUserAPIs includes all APIs described in this file in an APICollection
func IncludeUserAPIs(c *APICollection) {
	getUserInfo := BaseAPI{
		InputModel{
			Parameter{UserName, true},
		},
		getUserInfo,
		RoleRead,
	}
	c.Add("getUserInfo", &getUserInfo)

	setUserInfo := BaseAPI{
		InputModel{
			Parameter{UserName, true},
			Parameter{FullName, false},
			Parameter{Status, false},
			Parameter{GroupAccount, false},
			Parameter{ExpirationDate, false},
		},
		setUserInfo,
		RoleWrite,
	}
	c.Add("setUserInfo", &setUserInfo)

	createUser := BaseAPI{
		InputModel{
			Parameter{UID, true},
			Parameter{UserName, true},
			Parameter{FullName, true},
			Parameter{Status, true},
			Parameter{GroupName, true},
			Parameter{ExpirationDate, false},
		},
		createUser,
		RoleWrite,
	}
	c.Add("createUser", &createUser)

	getSuperUserList := BaseAPI{
		InputModel{
			Parameter{UnitName, true},
		},
		getSuperUserList,
		RoleRead,
	}
	c.Add("getSuperUserList", &getSuperUserList)

	deleteUser := BaseAPI{
		InputModel{
			Parameter{UserName, true},
		},
		deleteUser,
		RoleWrite,
	}
	c.Add("deleteUser", &deleteUser)

	dropUser := BaseAPI{
		InputModel{
			Parameter{UID, true},
		},
		dropUser,
		RoleWrite,
	}
	c.Add("dropUser", &dropUser)

	addCertificateDNToUser := BaseAPI{
		InputModel{
			Parameter{UserName, true},
			Parameter{UnitName, true},
			Parameter{DN, true},
		},
		addCertificateDNToUser,
		RoleWrite,
	}
	c.Add("addCertificateDNToUser", &addCertificateDNToUser)

	getUserExternalAffiliationAttributes := BaseAPI{
		InputModel{
			Parameter{UserName, false},
			Parameter{LastUpdated, false},
		},
		getUserExternalAffiliationAttributes,
		RoleRead,
	}
	c.Add("getUserExternalAffiliationAttributes", &getUserExternalAffiliationAttributes)

	setUserExternalAffiliationAttribute := BaseAPI{
		InputModel{
			Parameter{UserName, true},
			Parameter{UserAttribute, true},
			Parameter{Value, true},
		},
		setUserExternalAffiliationAttribute,
		RoleWrite,
	}
	c.Add("setUserExternalAffiliationAttribute", &setUserExternalAffiliationAttribute)

	getUserStorageQuota := BaseAPI{
		InputModel{
			Parameter{UserName, true},
			Parameter{UnitName, true},
			Parameter{ResourceName, true},
		},
		getUserStorageQuota,
		RoleRead,
	}
	c.Add("getUserStorageQuota", &getUserStorageQuota)

	getStorageQuotas := BaseAPI{
		InputModel{
			Parameter{UserName, false},
			Parameter{GroupName, false},
			Parameter{ResourceName, false},
			Parameter{LastUpdated, false},
		},
		getStorageQuotas,
		RoleRead,
	}
	c.Add("getStorageQuotas", &getStorageQuotas)

	getUserAccessToComputeResources := BaseAPI{
		InputModel{
			Parameter{UserName, true},
			Parameter{LastUpdated, false},
		},
		getUserAccessToComputeResources,
		RoleRead,
	}
	c.Add("getUserAccessToComputeResources", &getUserAccessToComputeResources)

	setUserAccessToComputeResource := BaseAPI{
		InputModel{
			Parameter{UserName, true},
			Parameter{GroupName, true},
			Parameter{ResourceName, true},
			Parameter{Shell, false},
			Parameter{HomeDir, false},
			Parameter{Primary, false},
		},
		setUserAccessToComputeResource,
		RoleWrite,
	}
	c.Add("setUserAccessToComputeResource", &setUserAccessToComputeResource)

	setUserExperimentFQAN := BaseAPI{
		InputModel{
			Parameter{UserName, true},
			Parameter{UnitName, true},
			Parameter{FQAN, false},
			Parameter{Role, false},
		},
		setUserExperimentFQAN,
		RoleWrite,
	}
	c.Add("setUserExperimentFQAN", &setUserExperimentFQAN)

	getUserFQANs := BaseAPI{
		InputModel{
			Parameter{UserName, true},
			Parameter{UnitName, false},
			Parameter{LastUpdated, false},
		},
		getUserFQANs,
		RoleRead,
	}
	c.Add("getUserFQANs", &getUserFQANs)

	getUserCertificateDNs := BaseAPI{
		InputModel{
			Parameter{UserName, false},
			Parameter{UnitName, false},
		},
		getUserCertificateDNs,
		RoleRead,
	}
	c.Add("getUserCertificateDNs", &getUserCertificateDNs)

	getAllUsersCertificateDNs := BaseAPI{
		InputModel{
			Parameter{UnitName, false},
			Parameter{Status, false},
			Parameter{LastUpdated, false},
		},
		getAllUsersCertificateDNs,
		RoleRead,
	}
	c.Add("getAllUsersCertificateDNs", &getAllUsersCertificateDNs)

	getUserGroups := BaseAPI{
		InputModel{
			Parameter{UserName, true},
			Parameter{LastUpdated, false},
		},
		getUserGroups,
		RoleRead,
	}
	c.Add("getUserGroups", &getUserGroups)

	addUserToGroup := BaseAPI{
		InputModel{
			Parameter{UserName, true},
			Parameter{GroupName, true},
			Parameter{GroupType, true},
			Parameter{Leader, false},
		},
		addUserToGroup,
		RoleWrite,
	}
	c.Add("addUserToGroup", &addUserToGroup)

	setUserShellAndHomeDir := BaseAPI{
		InputModel{
			Parameter{UserName, true},
			Parameter{ResourceName, true},
			Parameter{Shell, true},
			Parameter{HomeDir, true},
		},
		setUserShellAndHomeDir,
		RoleWrite,
	}
	c.Add("setUserShellAndHomeDir", &setUserShellAndHomeDir)

	getUserShellAndHomeDir := BaseAPI{
		InputModel{
			Parameter{UserName, true},
			Parameter{ResourceName, true},
			Parameter{LastUpdated, false},
		},
		getUserShellAndHomeDir,
		RoleRead,
	}
	c.Add("getUserShellAndHomeDir", &getUserShellAndHomeDir)

	setUserShell := BaseAPI{
		InputModel{
			Parameter{UserName, true},
			Parameter{UnitName, true},
			Parameter{Shell, true},
		},
		setUserShell,
		RoleWrite,
	}
	c.Add("setUserShell", &setUserShell)

	removeUserFromGroup := BaseAPI{
		InputModel{
			Parameter{UserName, true},
			Parameter{GroupName, true},
			Parameter{GroupType, true},
		},
		removeUserFromGroup,
		RoleWrite,
	}
	c.Add("removeUserFromGroup", &removeUserFromGroup)

	getUserUname := BaseAPI{
		InputModel{
			Parameter{UID, true},
		},
		getUserUname,
		RoleRead,
	}
	c.Add("getUserUname", &getUserUname)

	getUserUID := BaseAPI{
		InputModel{
			Parameter{UserName, true},
		},
		getUserUID,
		RoleRead,
	}
	c.Add("getUserUID", &getUserUID)

	getAllUsers := BaseAPI{
		InputModel{
			Parameter{Status, false},
			Parameter{LastUpdated, false},
		},
		getAllUsers,
		RoleRead,
	}
	c.Add("getAllUsers", &getAllUsers)

	getAllUsersFQANs := BaseAPI{
		InputModel{
			Parameter{Suspend, false},
			Parameter{LastUpdated, false},
		},
		getAllUsersFQANs,
		RoleRead,
	}
	c.Add("getAllUsersFQANs", &getAllUsersFQANs)

	getMemberAffiliations := BaseAPI{
		InputModel{
			Parameter{UserName, true},
			Parameter{Experiment, false},
			Parameter{LastUpdated, false},
		},
		getMemberAffiliations,
		RoleRead,
	}
	c.Add("getMemberAffiliations", &getMemberAffiliations)

	setUserGridAccess := BaseAPI{
		InputModel{
			Parameter{UserName, true},
			Parameter{UnitName, true},
			Parameter{Suspend, true},
		},
		setUserGridAccess,
		RoleWrite,
	}
	c.Add("setUserGridAccess", &setUserGridAccess)

	removeUserCertificateDN := BaseAPI{
		InputModel{
			Parameter{UserName, true},
			Parameter{DN, true},
		},
		removeUserCertificateDN,
		RoleWrite,
	}
	c.Add("removeUserCertificateDN", &removeUserCertificateDN)

	removeUserExternalAffiliationAttribute := BaseAPI{
		InputModel{
			Parameter{UserName, true},
			Parameter{UserAttribute, true},
		},
		removeUserExternalAffiliationAttribute,
		RoleWrite,
	}
	c.Add("removeUserExternalAffiliationAttribute", &removeUserExternalAffiliationAttribute)

	getUserGroupsForComputeResource := BaseAPI{
		InputModel{
			Parameter{ResourceType, false},
			Parameter{UnitName, false},
		},
		getUserGroupsForComputeResource,
		RoleRead,
	}
	c.Add("getUserGroupsForComputeResource", &getUserGroupsForComputeResource)

	removeUserFromComputeResource := BaseAPI{
		InputModel{
			Parameter{UserName, true},
			Parameter{ResourceType, true},
			Parameter{ResourceName, true},
		},
		removeUserFromComputeResource,
		RoleWrite,
	}
	c.Add("removeUserFromComputeResource", &removeUserFromComputeResource)

}

func getUserCertificateDNs(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	uid := NewNullAttribute(UID)
	unitid := NewNullAttribute(UnitID)
	queryerr := c.DBtx.QueryRow(`select (select uid from users where uname=$1),
									    (select unitid from affiliation_units where name=$2)`,
		i[UserName], i[UnitName]).Scan(&uid, &unitid)
	if queryerr != nil {
		log.WithFields(QueryFields(c)).Error(queryerr)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !uid.Valid && i[UserName].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
	}
	if !unitid.Valid && i[UnitName].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	rows, queryerr := c.DBtx.Query(`select distinct uname, dn from
									user_certificates as uc
									join users as u using(uid)
									join affiliation_unit_user_certificate ac using(dnid)
									where uid = coalesce($1, uid)
									and (unitid in (select distinct unitid from grid_fqan where fqan like '%' || $2 || '%') or $2 is null)
									order by uname`,
		uid, i[UnitName])
	if queryerr != nil {
		log.WithFields(QueryFields(c)).Error(queryerr)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	const Certificates Attribute = "certificates"

	type jsonentry map[Attribute]interface{}
	type jsoncerts []interface{}

	entry := jsonentry{
		UserName:     "",
		Certificates: make(jsoncerts, 0),
	}
	var out jsoncerts

	row := NewMapNullAttribute(UserName, DN)
	for rows.Next() {
		rows.Scan(row[UserName], row[DN])
		if row[DN].Valid {
			if entry[UserName] == "" {
				entry[UserName] = row[UserName].Data
			}
			if row[UserName].Data != entry[UserName] {
				newEntry := make(jsonentry)
				newEntry[UserName] = entry[UserName]
				newEntry[Certificates] = entry[Certificates]
				out = append(out, newEntry)
				entry[UserName] = row[UserName].Data
				entry[Certificates] = make(jsoncerts, 0)
			}
			entry[Certificates] = append(entry[Certificates].(jsoncerts), row[DN].Data)
		}
	}
	out = append(out, entry)

	return out, nil
}

func getAllUsersCertificateDNs(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	unitid := NewNullAttribute(UnitID)

	queryerr := c.DBtx.QueryRow(`select unitid from affiliation_units where name=$1`,
		i[UnitName]).Scan(&unitid)
	if queryerr != nil && queryerr != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(queryerr)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !unitid.Valid && i[UnitName].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
		return nil, apiErr
	}

	activeOnly := i[Status].Default(false)

	rows, queryerr := c.DBtx.Query(`select uname, name, dn from
										affiliation_unit_user_certificate as ac
										join user_certificates using(dnid)
										join users using(uid)
										join affiliation_units using(unitid)
										where unitid = coalesce($1, unitid) and (status = $2 or not $2) and (ac.last_updated >= $3 or $3 is null)
									order by uname`, unitid, activeOnly, i[LastUpdated])
	if queryerr != nil {
		log.WithFields(QueryFields(c)).Error(queryerr)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	const Certificates Attribute = "certificates"

	type jsoncert map[Attribute]interface{}
	type jsonuser map[Attribute]interface{}
	var out []jsonuser

	prevUname := NewNullAttribute(UserName)
	for rows.Next() {
		row := NewMapNullAttribute(UserName, UnitName, DN)
		rows.Scan(row[UserName], row[UnitName], row[DN])
		if row[UserName].Valid {
			if prevUname != *row[UserName] {
				user := make(jsonuser)
				user[UserName] = row[UserName].Data
				user[Certificates] = make([]jsoncert, 0)
				out = append(out, user)
				prevUname = *row[UserName]
			}
			cert := make(jsoncert)
			cert[UnitName] = row[UnitName].Data
			cert[DN] = row[DN].Data
			out[len(out)-1][Certificates] = append(out[len(out)-1][Certificates].([]jsoncert), cert)
		}
	}

	return out, nil
}

func getUserFQANs(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	uid := NewNullAttribute(UID)
	unitid := NewNullAttribute(UnitID)

	err := c.DBtx.QueryRow(`select (select uid from users where uname = $1),
								   (select unitid from affiliation_units where name=$2)`,
		i[UserName], i[UnitName]).Scan(&uid, &unitid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !uid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
	}
	if !unitid.Valid && i[UnitName].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	rows, err := c.DBtx.Query(`select name, fqan from
								grid_access as ga
							  	join grid_fqan using(fqanid)
							  	left join affiliation_units using(unitid)
							   where
								uid = $1 and
								(unitid = $2 or $2 is null) and
							  	(ga.last_updated >= $3 or $3 is null)
							   order by name;`, uid, unitid, i[LastUpdated])
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	type jsonfqan map[Attribute]interface{}
	out := make([]jsonfqan, 0)

	for rows.Next() {
		row := NewMapNullAttribute(UnitName, FQAN)
		rows.Scan(row[UnitName], row[FQAN])
		if row[FQAN].Valid {
			out = append(out, jsonfqan{UnitName: row[UnitName].Data, FQAN: row[FQAN].Data})
		}
	}

	return out, nil
}

func getSuperUserList(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	unitID := NewNullAttribute(UnitID)
	err := c.DBtx.QueryRow(`select unitid from affiliation_units where name = $1`, i[UnitName]).Scan(&unitID)
	if err == sql.ErrNoRows {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
		return nil, apiErr
	} else if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	rows, err := c.DBtx.Query(`select distinct u.uname from
								users as u
								join grid_access as ga on u.uid=ga.uid
								join grid_fqan as gf on ga.fqanid = gf.fqanid
							   where ga.is_superuser=true and gf.unitid=$1
							   order by u.uname`, unitID)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	var out []interface{}

	for rows.Next() {
		row := NewNullAttribute(UserName)
		rows.Scan(&row)
		out = append(out, row.Data)
	}

	return out, nil
}

func getUserGroups(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	uid := NewNullAttribute(UID)
	err := c.DBtx.QueryRow(`select uid from users where uname = $1`, i[UserName]).Scan(&uid)
	if err == sql.ErrNoRows {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
		return nil, apiErr
	}
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	rows, err := c.DBtx.Query(`select gid, name, type from
									groups join
									user_group using(groupid)
							   where uid = $1 and (user_group.last_updated >= $2 or $2 is null)`,
		uid, i[LastUpdated])
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	type jsongroup map[Attribute]interface{}
	out := make([]jsongroup, 0)

	for rows.Next() {
		row := NewMapNullAttribute(GID, GroupName, GroupType)
		rows.Scan(row[GID], row[GroupName], row[GroupType])
		out = append(out, jsongroup{
			GID:       row[GID].Data,
			GroupName: row[GroupName].Data,
			GroupType: row[GroupType].Data,
		})
	}

	return out, nil
}

func getUserInfo(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	rows, err := c.DBtx.Query(`select full_name, uid, status, is_groupaccount, expiration_date from users where uname=$1`, i[UserName])
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	out := make(map[Attribute]interface{})
	row := NewMapNullAttribute(FullName, UID, Status, GroupAccount, ExpirationDate)

	for rows.Next() {
		rows.Scan(row[FullName], row[UID], row[Status], row[GroupAccount], row[ExpirationDate])
		for _, column := range row {
			out[column.Attribute] = column.Data
		}
	}
	if len(out) == 0 {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
		return nil, apiErr
	}

	return out, nil
}

func addUserToGroup(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	uid := NewNullAttribute(UID)
	groupid := NewNullAttribute(GroupID)
	leader := i[Leader].Default(false)

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

	_, err = c.DBtx.Exec(`insert into user_group (uid, groupid, is_leader, last_updated) values ($1, $2, $3, NOW())
						  on conflict (uid, groupid) do
						  update set is_leader = $3, last_updated = NOW() where $4`,
		uid, groupid, leader, i[Leader].Valid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, nil
}

func removeUserFromGroup(c APIContext, i Input) (interface{}, []APIError) {
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

	_, err = c.DBtx.Exec(`delete from user_group where uid = $1 and groupid = $2`, uid, groupid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		if strings.Contains(err.Error(), `fk_compute_access_group_user_group`) {
			apiErr = append(apiErr, APIError{errors.New("user belongs to this group in one or more compute resources"), ErrorAPIRequirement})
			return nil, apiErr
		}
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, nil
}

func setUserExperimentFQAN(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	fqan := NewNullAttribute(FQAN)
	if i[FQAN].Valid {
		fqan = i[FQAN]
	} else {
		if i[Role].Valid {
			fqan.Scan("%Role=" + strings.TrimSpace(i[Role].Data.(string)) + "%")
		} else if i[Role].AbsoluteNull {
			fqan.Scan("%Role=NULL%")
		} else {
			apiErr = append(apiErr, APIError{errors.New("no role or fqan specified"), ErrorAPIRequirement})
			return nil, apiErr
		}
	}

	uid := NewNullAttribute(UID)
	unitid := NewNullAttribute(UnitID)
	queryerr := c.DBtx.QueryRow(`select (select uid from users where uname=$1),
									    (select unitid from affiliation_units where name=$2)`,
		i[UserName], i[UnitName]).Scan(&uid, &unitid)
	if queryerr != nil {
		log.WithFields(QueryFields(c)).Error(queryerr)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !uid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
	}
	if !unitid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	var hasCert bool
	queryerr = c.DBtx.QueryRow(`select count(*) > 0 from affiliation_unit_user_certificate as ac
							  join user_certificates as uc on ac.dnid = uc.dnid
							  where uid = $1 and unitid = $2`, uid, unitid).Scan(&hasCert)
	if queryerr != nil {
		log.WithFields(QueryFields(c)).Error(queryerr)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !hasCert {
		apiErr = append(apiErr, APIError{errors.New("the user is not a member of the affiliation unit"), ErrorAPIRequirement})
		return nil, apiErr
	}

	rows, queryerr := c.DBtx.Query(`select fqanid from grid_fqan where unitid = $1 and fqan like $2`, unitid, fqan)
	if queryerr != nil {
		log.WithFields(QueryFields(c)).Error(queryerr)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	var fqanids []int
	for rows.Next() {
		var fqanid int
		rows.Scan(&fqanid)
		fqanids = append(fqanids, fqanid)
	}
	rows.Close()
	if len(fqanids) == 0 {
		apiErr = append(apiErr, APIError{errors.New("no FQANs found for this query"), ErrorAPIRequirement})
		return nil, apiErr
	}

	for _, fqanid := range fqanids {
		_, queryerr = c.DBtx.Exec(`insert into grid_access (uid, fqanid, is_superuser, is_banned, last_updated)
								   values($1, $2, false, false, NOW())
								   on conflict (uid, fqanid) do nothing`, uid, fqanid)
		if queryerr != nil {
			log.WithFields(QueryFields(c)).Error(queryerr)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
			return nil, apiErr
		}
	}

	return nil, nil
}

func setUserShellAndHomeDir(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	uid := NewNullAttribute(UID)
	resourceid := NewNullAttribute(ResourceID)

	err := c.DBtx.QueryRow(`select (select uid from users where uname = $1),
								   (select compid from compute_resources where name = $2)`,
		i[UserName], i[ResourceName]).Scan(&uid, &resourceid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !uid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
	}
	if !resourceid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, ResourceName))
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	var member bool
	err = c.DBtx.QueryRow(`select ($1, $2) in (select uid, compid from compute_access)`,
		uid, resourceid).Scan(&member)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !member {
		apiErr = append(apiErr, APIError{
			errors.New("user does not have access to compute resource"),
			ErrorAPIRequirement,
		})
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`update compute_access set shell = $1, home_dir = $2, last_updated = NOW()
						   where compid = $3 and uid = $4`, i[Shell], i[HomeDir], resourceid, uid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, nil
}

func setUserShell(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	uid := NewNullAttribute(UID)
	unitid := NewNullAttribute(UnitID)

	err := c.DBtx.QueryRow(`select (select uid from users where uname = $1),
								   (select unitid from affiliation_units where name = $2)`,
		i[UserName], i[UnitName]).Scan(&uid, &unitid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !uid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
	}
	if !unitid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	var member bool
	err = c.DBtx.QueryRow(`select ($1, $2) in (select uid, unitid from
						   compute_access join compute_resources using(compid)
						   where unitid is not null)`,
		uid, unitid).Scan(&member)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !member {
		apiErr = append(apiErr, APIError{
			errors.New("user does not have access to compute resource"),
			ErrorAPIRequirement,
		})
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`update compute_access set shell = $1, last_updated = NOW()
						  where uid = $2 and
						  compid in (select compid from compute_resources where unitid = $3)`, i[Shell], uid, unitid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, nil
}

func getUserShellAndHomeDir(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	uid := NewNullAttribute(UID)
	resourceid := NewNullAttribute(ResourceID)

	err := c.DBtx.QueryRow(`select (select uid from users where uname = $1),
								   (select compid from compute_resources where name = $2)`,
		i[UserName], i[ResourceName]).Scan(&uid, &resourceid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !uid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
	}
	if !resourceid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, ResourceName))
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	var member bool
	err = c.DBtx.QueryRow(`select ($1, $2) in (select uid, compid from compute_access)`,
		uid, resourceid).Scan(&member)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !member {
		apiErr = append(apiErr, APIError{
			errors.New("user does not have access to compute resource"),
			ErrorAPIRequirement,
		})
		return nil, apiErr
	}

	row := NewMapNullAttribute(Shell, HomeDir)
	err = c.DBtx.QueryRow(`select shell, home_dir from compute_access
						   where uid = $1 and compid = $2 and (last_updated >= $3 or $3 is null)`,
		uid, resourceid, i[LastUpdated]).Scan(row[Shell], row[HomeDir])
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	type jsonout map[Attribute]interface{}
	out := make(jsonout)

	if row[Shell].Valid {
		out = jsonout{
			Shell:   row[Shell].Data,
			HomeDir: row[HomeDir].Data,
		}
	}

	return out, nil
}

func getUserStorageQuota(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	uid := NewNullAttribute(UID)
	unitid := NewNullAttribute(UnitID)
	resourceid := NewNullAttribute(ResourceID)

	err := c.DBtx.QueryRow(`select (select uid from users where uname = $1),
								   (select unitid from affiliation_units where name = $2),
								   (select storageid from storage_resources where name = $3)`,
		i[UserName], i[UnitName], i[ResourceName]).Scan(&uid, &unitid, &resourceid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !uid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
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

	rows, err := DBptr.Query(`select path, value, unit, valid_until from storage_quota
							  where uid = $1 AND unitid = $2 and storageid = $3 and (valid_until is null or valid_until >= NOW())
							  order by valid_until desc`, uid, unitid, resourceid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	type jsonentry map[Attribute]interface{}
	out := make(jsonentry)

	for rows.Next() {
		row := NewMapNullAttribute(Path, Value, QuotaUnit, ExpirationDate)
		rows.Scan(row[Path], row[Value], row[QuotaUnit], row[ExpirationDate])
		if row[Value].Valid {
			out = jsonentry{
				Path:           row[Path].Data,
				Value:          row[Value].Data,
				QuotaUnit:      row[QuotaUnit].Data,
				ExpirationDate: row[ExpirationDate].Data,
			}
		}
	}

	return out, nil
}

func setUserExternalAffiliationAttribute(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	uid := NewNullAttribute(UID)
	err := c.DBtx.QueryRow(`select uid from users where uname = $1`, i[UserName]).Scan(&uid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	if !uid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
	}

	var validAttribute bool
	err = c.DBtx.QueryRow(`select $1 = any (enum_range(null::external_affiliation_attribute_attribute_type)::text[])`, i[UserAttribute]).Scan(&validAttribute)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	if !validAttribute {
		apiErr = append(apiErr, DefaultAPIError(ErrorInvalidData, UserAttribute))
	}

	if len(apiErr) > 0 {
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`insert into external_affiliation_attribute (uid, attribute, value) values ($1, $2, $3)
						on conflict (uid, attribute) do update set value = $3`, uid, i[UserAttribute], i[Value])
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, nil
}
func removeUserExternalAffiliationAttribute(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	var validAttribute bool
	uid := NewNullAttribute(UID)
	err := c.DBtx.QueryRow(`select (select uid from users where uname = $1),
								   (select $2 = any (enum_range(null::external_affiliation_attribute_attribute_type)::text[]))`,
		i[UserName], i[UserAttribute]).Scan(&uid, &validAttribute)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !uid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
	}
	if !validAttribute {
		apiErr = append(apiErr, DefaultAPIError(ErrorInvalidData, UserAttribute))
	}

	if len(apiErr) > 0 {
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`delete from external_affiliation_attribute where uid = $1 and attribute = $2`,
		uid, i[UserAttribute])
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, nil
}
func getUserExternalAffiliationAttributes(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	uid := NewNullAttribute(UID)

	err := c.DBtx.tx.QueryRow(`select uid from users where uname = $1`, i[UserName]).Scan(&uid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !uid.Valid && i[UserName].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
		return nil, apiErr
	}

	rows, err := DBptr.Query(`select uname, attribute, value from
								external_affiliation_attribute as a
								join users as u using(uid)
							  where u.uid = coalesce($1, uid) and (a.last_updated >= $2 or $2 is null)
							  order by uname`, uid, i[LastUpdated])
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	type jsonentry map[Attribute]interface{}
	out := make(map[string][]jsonentry)

	for rows.Next() {
		row := NewMapNullAttribute(UserName, UserAttribute, Value)
		err = rows.Scan(row[UserName], row[UserAttribute], row[Value])

		if row[UserAttribute].Valid {
			entry := make(jsonentry)
			entry[UserAttribute] = row[UserAttribute].Data
			entry[Value] = row[Value].Data
			out[row[UserName].Data.(string)] = append(out[row[UserName].Data.(string)], entry)
		}
	}

	return out, nil
}

func addCertificateDNToUser(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	// DN validation
	dn, err := ExtractValidDN(i[DN].Data.(string))
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err.Error())
		apiErr = append(apiErr, DefaultAPIError(ErrorInvalidData, DN))
		return nil, apiErr
	}

	uid := NewNullAttribute(UID)
	dnid := NewNullAttribute(DNID)
	unitid := NewNullAttribute(UnitID)

	err = c.DBtx.QueryRow(`select (select uid from users where uname = $1),
								  (select dnid from user_certificates where dn=$2),
								  (select unitid from affiliation_units where name=$3)`,
		i[UserName], dn, i[UnitName]).Scan(&uid, &dnid, &unitid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !uid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
	}
	if !unitid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	if !dnid.Valid {
		_, err := c.DBtx.Exec(`insert into user_certificates (dn, uid, last_updated) values ($1, $2, NOW()) returning dnid`, dn, uid)
		if err != nil {
			log.WithFields(QueryFields(c)).Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
			return nil, apiErr
		}
		err = c.DBtx.QueryRow(`select dnid from user_certificates where dn=$1`, dn).Scan(&dnid)
		if err != nil {
			log.WithFields(QueryFields(c)).Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
			return nil, apiErr
		}
	}

	_, err = c.DBtx.Exec(`insert into affiliation_unit_user_certificate (unitid, dnid, last_updated) values ($1, $2, NOW())
						  on conflict (unitid, dnid) do nothing`, unitid, dnid)
	if err != nil && !strings.Contains(err.Error(), `pk_affiliation_unit_user_certificate`) {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, nil
}

func removeUserCertificateDN(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	// DN validation
	dn, err := ExtractValidDN(i[DN].Data.(string))
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err.Error())
		apiErr = append(apiErr, DefaultAPIError(ErrorInvalidData, DN))
		return nil, apiErr
	}

	uid := NewNullAttribute(UID)
	dnid := NewNullAttribute(DNID)

	var countUnique int64

	err = c.DBtx.QueryRow(`select (select uid from users where uname = $1),
								  (select dnid from user_certificates where dn=$2)`,
		i[UserName], dn).Scan(&uid, &dnid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	err = c.DBtx.QueryRow(`select count(*) from
							(select uid, unitid, count(unitid)
							 from affiliation_unit_user_certificate as ac
							 join user_certificates as uc on ac.dnid = uc.dnid
						     where uid = $1 and unitid in
								(select unitid from affiliation_unit_user_certificate
								 join user_certificates using(dnid) where uid = $1 and dnid = $2)
							 group by unitid, uid order by uid, unitid, count) as c
						   where c.count = 1`,
		uid, dnid).Scan(&countUnique)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !uid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
	}
	if !dnid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, DN))
	}
	if countUnique > 0 {
		apiErr = append(apiErr, APIError{errors.New("this certificate is unique for the user in one or more affiliation units"), ErrorAPIRequirement})
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`delete from affiliation_unit_user_certificate where dnid = $1`, dnid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`delete from user_certificates where dnid = $1`, dnid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, nil
}

func setUserInfo(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	if !i[FullName].Valid && !i[Status].Valid && !i[GroupAccount].Valid &&
		!i[ExpirationDate].Valid && !i[ExpirationDate].AbsoluteNull {
		apiErr = append(apiErr, APIError{errors.New("not enough arguments"), ErrorAPIRequirement})
		return nil, apiErr
	}

	uid := NewNullAttribute(UID)
	expDate := NewNullAttribute(ExpirationDate)

	queryerr := c.DBtx.tx.QueryRow(`select uid, expiration_date from users where uname = $1`,
		i[UserName]).Scan(&uid, &expDate)
	if queryerr == sql.ErrNoRows {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
		return nil, apiErr
	} else if queryerr != nil {
		log.WithFields(QueryFields(c)).Error(queryerr)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	expDate = i[ExpirationDate].Default(expDate.Data)

	_, queryerr = c.DBtx.Exec(`update users set	full_name = coalesce($2, full_name),
												status = coalesce($3, status),
												is_groupaccount = coalesce($4, is_groupaccount),
												expiration_date = $5,
												last_updated = NOW()
							   where uid = $1`,
		uid, i[FullName], i[Status], i[GroupAccount], expDate)
	if queryerr != nil {
		log.WithFields(QueryFields(c)).Error(queryerr)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, nil
}

func createUser(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	groupid := NewNullAttribute(GroupID)
	expDate := i[ExpirationDate].Default("2038-01-01")

	err := c.DBtx.QueryRow(`select (select groupid from groups where name = $1 and type = 'UnixGroup')`, i[GroupName]).Scan(&groupid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !groupid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, GroupName))
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`insert into users (uname, uid, full_name, status, expiration_date, last_updated)
						  values ($1, $2, $3, $4, $5, NOW())`,
		i[UserName], i[UID], i[FullName], i[Status], expDate)
	if err != nil {
		if strings.Contains(err.Error(), "duplicate key value violates unique constraint \"pk_users\"") {
			apiErr = append(apiErr, DefaultAPIError(ErrorDuplicateData, UID))
		} else if strings.Contains(err.Error(), "duplicate key value violates unique constraint \"unq_users_uname\"") {
			apiErr = append(apiErr, DefaultAPIError(ErrorDuplicateData, UserName))
		} else {
			log.WithFields(QueryFields(c)).Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		}
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`insert into user_group (uid, groupid, is_leader, last_updated)
						  values ($1, $2, false, NOW())`,
		i[UID], groupid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, nil
}

func getMemberAffiliations(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	uid := NewNullAttribute(UID)
	experiment := i[Experiment].Default(false)

	err := c.DBtx.QueryRow(`select uid from users where uname = $1`, i[UserName]).Scan(&uid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !uid.Valid && i[UserName].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
		return nil, apiErr
	}

	rows, err := DBptr.Query(`select distinct name, alternative_name from affiliation_units
							  join affiliation_unit_user_certificate as ac using(unitid)
							  join user_certificates using(dnid)
							  where uid = $1 and (
								  (((unitid in (select unitid from voms_url)) = $2) or not $2)
								  and (ac.last_updated >= $3 or $3 is null)
							  )`, uid, experiment, i[LastUpdated])
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	type jsonentry map[Attribute]interface{}
	var out []jsonentry

	for rows.Next() {
		row := NewMapNullAttribute(UnitName, AlternativeName)
		rows.Scan(row[UnitName], row[AlternativeName])

		if row[UnitName].Valid {
			out = append(out, jsonentry{
				UnitName:        row[UnitName].Data,
				AlternativeName: row[AlternativeName].Data,
			})
		} else {
			apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
			return nil, apiErr
		}
	}
	return out, nil
}

func getUserUname(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	uname := NewNullAttribute(UserName)

	err := c.DBtx.QueryRow(`select uname from users where uid = $1`, i[UID]).Scan(&uname)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !uname.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
		return nil, apiErr
	}

	return uname.Data, nil
}

func getUserUID(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	uid := NewNullAttribute(UID)

	err := c.DBtx.QueryRow(`select uid from users where uname = $1`, i[UserName]).Scan(&uid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !uid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
		return nil, apiErr
	}

	return uid.Data, nil
}

func deleteUser(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	uid := NewNullAttribute(UID)

	err := c.DBtx.QueryRow(`select uid from users where uname = $1`, i[UserName]).Scan(&uid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !uid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`delete from users where uid = $1`, uid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		if strings.Contains(err.Error(), "violates foreign key constraint") {
			apiErr = append(apiErr, APIError{errors.New("all associations with this user must be removed before it can be deleted"), ErrorAPIRequirement})
		} else {
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		}
		return nil, apiErr
	}

	return nil, nil
}

func dropUser(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	uid := i[UID]

	// Process the user groups first
	colList, apiErr := getTableColumns(c, "user_group")
	if apiErr != nil {
		return nil, apiErr
	}
	columns := strings.Join(colList, ",")
	sql := fmt.Sprintf("with foo as (delete from user_group where uid=%d returning *, now() when_deleted) insert into user_group_deletions (%s, when_deleted) select * from foo",
		uid.Data, columns)
	_, err := c.DBtx.Exec(sql)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		if strings.Contains(err.Error(), "violates foreign key constraint") {
			apiErr = append(apiErr, APIError{errors.New("all associations with this user must be removed before it can be deleted"), ErrorAPIRequirement})
		} else {
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		}
		return nil, apiErr
	}
	// Now process the user record
	colList, apiErr = getTableColumns(c, "users")
	if apiErr != nil {
		return nil, apiErr
	}
	columns = strings.Join(colList, ",")
	sql = fmt.Sprintf(`with foo as (delete from users where uid=%d returning *, now() when_deleted) insert into user_deletions (%s, when_deleted) select * from foo`, uid.Data, columns)
	_, err = c.DBtx.Exec(sql)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		if strings.Contains(err.Error(), "violates foreign key constraint") {
			apiErr = append(apiErr, APIError{errors.New("all associations with this user must be removed before it can be deleted"), ErrorAPIRequirement})
		} else {
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		}
		return nil, apiErr
	}

	return nil, nil
}

func getUserAccessToComputeResources(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	uid := NewNullAttribute(UID)

	err := c.DBtx.tx.QueryRow(`select uid from users where uname = $1`, i[UserName]).Scan(&uid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !uid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
		return nil, apiErr
	}

	rows, err := c.DBtx.Query(`select name, type, shell, home_dir from
								compute_access as ca join
								compute_resources using(compid)
							   where uid = $1 and (ca.last_updated>=$2 or $2 is null)`,
		uid, i[LastUpdated])
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	type jsonentry map[Attribute]interface{}
	out := make([]jsonentry, 0)

	for rows.Next() {
		row := NewMapNullAttribute(ResourceName, ResourceType, Shell, HomeDir)
		rows.Scan(row[ResourceName], row[ResourceType], row[Shell], row[HomeDir])

		if row[ResourceName].Valid {
			entry := jsonentry{
				ResourceName: row[ResourceName].Data,
				ResourceType: row[ResourceType].Data,
				Shell:        row[Shell].Data,
				HomeDir:      row[HomeDir].Data,
			}
			out = append(out, entry)
		}
	}

	return out, nil
}

func getStorageQuotas(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	uid := NewNullAttribute(UID)
	groupid := NewNullAttribute(GroupID)
	resourceid := NewNullAttribute(ResourceID)

	err := c.DBtx.QueryRow(`select
								(select uid from users where uname = $1),
								(select groupid from groups where name = $2 and type = 'UnixGroup'),
								(select storageid from storage_resources where name = $3)`,
		i[UserName], i[GroupName], i[ResourceName]).Scan(&uid, &groupid, &resourceid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !uid.Valid && i[UserName].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
	}
	if !groupid.Valid && i[GroupName].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, GroupName))
	}
	if !resourceid.Valid && i[ResourceName].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, ResourceName))
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	rows, err := DBptr.Query(`select uname, g.name, sr.name, path, value, unit, valid_until from
								storage_quota as sq
								left join users as u on sq.uid = u.uid
								left join groups as g on sq.groupid = g.groupid
								join storage_resources as sr on sq.storageid = sr.storageid
							  where
							  		(u.uid = $1 or $1 is null)
								and (g.groupid = $2 or $2 is null)
								and (sr.storageid = coalesce($3, sr.storageid))
								and (valid_until is null or valid_until >= NOW())
								and (sq.last_updated >= $4 or $4 is null)
							  order by uname asc, g.name asc, sr.name asc, valid_until desc`,
		uid, groupid, resourceid, i[LastUpdated])
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	const Users Attribute = "users"
	const Groups Attribute = "groups"

	type jsonquota map[Attribute]interface{}
	type jsonstorage map[string]jsonquota
	type jsonowner map[string]jsonstorage

	out := make(map[Attribute]jsonowner)
	out[Users] = make(jsonowner)
	out[Groups] = make(jsonowner)

	for rows.Next() {
		row := NewMapNullAttribute(UserName, GroupName, ResourceName, Path, Quota, QuotaUnit, ExpirationDate)
		rows.Scan(row[UserName], row[GroupName], row[ResourceName], row[Path], row[Quota], row[QuotaUnit], row[ExpirationDate])

		var ownerName, ownerType Attribute
		if row[UserName].Valid {
			ownerName = UserName
			ownerType = Users
		}
		if row[GroupName].Valid {
			ownerName = GroupName
			ownerType = Groups
		}

		if ownerName != "" {
			if _, ok := out[ownerType][row[ownerName].Data.(string)]; !ok {
				out[ownerType][row[ownerName].Data.(string)] = make(jsonstorage)
			}
			out[ownerType][row[ownerName].Data.(string)][row[ResourceName].Data.(string)] = jsonquota{
				Path:           row[Path].Data,
				Quota:          row[Quota].Data,
				QuotaUnit:      row[QuotaUnit].Data,
				ExpirationDate: row[ExpirationDate].Data,
			}
		}
	}

	return out, nil
}

func setUserAccessToComputeResource(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	primary := i[Primary].Default(false)

	dShell := NewNullAttribute(Shell)
	dHome := NewNullAttribute(HomeDir)
	groupid := NewNullAttribute(GroupID)
	compid := NewNullAttribute(ResourceID)
	uid := NewNullAttribute(UID)

	err := c.DBtx.QueryRow(`select
								(select uid from users where uname = $1),
								(select groupid from groups where name = $2 and type = 'UnixGroup'),
								(select compid from compute_resources where name = $3)`,
		i[UserName], i[GroupName], i[ResourceName]).Scan(&uid, &groupid, &compid)
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
	if !compid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, ResourceName))
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	// Ensure user is member of group (user_group)
	_, err = c.DBtx.Exec(`insert into user_group (uid, groupid) values ($1, $2)
						  on conflict (uid, groupid) do nothing`, uid, groupid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	// Grant user access to the resource (compute_access)
	err = c.DBtx.QueryRow(`select default_shell, default_home_dir from compute_resources where compid = $1`,
		compid).Scan(&dShell, &dHome)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if dHome.Valid {
		homeStr := strings.TrimRight(dHome.Data.(string), "/")
		if homeStr == "/nashome" {
			homeStr = "/nashome/" + i[UserName].Data.(string)[0:1]
		}
		homeStr = homeStr + "/" + i[UserName].Data.(string)
		dHome.Scan(homeStr)
	}

	shell := i[Shell].Default(dShell.Data.(string))
	home := i[HomeDir].Default(dHome.Data.(string))

	if !shell.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorInvalidData, Shell))
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`insert into compute_access as ca (compid, uid, shell, home_dir) values ($1, $2, $3, $4)
						  on conflict (compid, uid) do update set shell = coalesce($5, ca.shell), home_dir = coalesce($6, ca.home_dir)`,
		compid, uid, shell, home, i[Shell], i[HomeDir])
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	// If the caller passed in primary as TRUE then we must set any prior true to false.  -- there can be only one!
	if primary.Data.(bool) {
		_, err = c.DBtx.Exec(`update compute_access_group set is_primary = false where uid = $1 and compid = $2 and is_primary`,
			uid, compid)
		if err != nil {
			log.WithFields(QueryFields(c)).Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
			return nil, apiErr
		}
	}
	_, err = c.DBtx.Exec(`insert into compute_access_group as cg (compid, uid, groupid, is_primary) values ($1, $2, $3, $4)
	 						on conflict (compid, uid, groupid) do update set is_primary = coalesce($5, cg.is_primary)`,
		compid, uid, groupid, primary, primary)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	// We have set up what was asked for BUT, we must insure that there is at least one record for this resource  with is_primary
	// set to TRUE.   If none exist, the user will not appear in the password file.  In that case, force the current record to be primary.  (is_primary=true)
	var priCount int
	err = c.DBtx.QueryRow(`select count(*) from compute_access_group where uid = $1 and compid = $2 and is_primary`,
		uid, compid).Scan(&priCount)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	if priCount == 0 {
		_, err = c.DBtx.Exec(`update compute_access_group set is_primary=true
								where uid = $1 and compid = $2`, uid, compid)
		if err != nil {
			log.WithFields(QueryFields(c)).Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
			return nil, apiErr
		}
		log.Warn("User choices caused no record to be primary for this resource.  Forcing the current record to be primary.")
	}
	return nil, nil
}

func getAllUsers(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	status := i[Status].Default(false)

	rows, err := DBptr.Query(`select uname, uid, full_name, status, expiration_date from users
							  where (status=$1 or not $1) and (last_updated>=$2 or $2 is null)
							  order by uname`, status, i[LastUpdated])
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	type jsonout map[Attribute]interface{}
	var out []jsonout

	for rows.Next() {
		row := NewMapNullAttribute(UserName, UID, FullName, Status, ExpirationDate)
		rows.Scan(row[UserName], row[UID], row[FullName], row[Status], row[ExpirationDate])

		var expirationDate interface{}
		if row[ExpirationDate].Valid {
			expirationDate = row[ExpirationDate].Data
		}

		out = append(out, jsonout{
			UserName:       row[UserName].Data,
			UID:            row[UID].Data,
			FullName:       row[FullName].Data,
			Status:         row[Status].Data,
			ExpirationDate: expirationDate,
		})
	}

	return out, nil
}

func getAllUsersFQANs(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	rows, err := DBptr.Query(`select uname, fqan, name, is_banned from grid_access as ga
							  join grid_fqan as gf using(fqanid)
							  join users as u using(uid)
							  join affiliation_units as au using(unitid)
							  where (ga.last_updated>=$2 or gf.last_updated>=$2 or
									  u.last_updated>=$2 or au.last_updated>=$2 or $2 is null)
									and (is_banned = $1 or $1 is null)  order by uname`,
		i[Suspend], i[LastUpdated])
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	type jsonfqan map[Attribute]interface{}
	out := make(map[string][]jsonfqan)

	for rows.Next() {
		row := NewMapNullAttribute(UserName, FQAN, UnitName, Suspend)
		rows.Scan(row[UserName], row[FQAN], row[UnitName], row[Suspend])
		out[row[UserName].Data.(string)] = append(out[row[UserName].Data.(string)], jsonfqan{
			FQAN:     row[FQAN].Data,
			UnitName: row[UnitName].Data,
			Suspend:  row[Suspend].Data,
		})
	}

	return out, nil
}

func setUserGridAccess(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	uid := NewNullAttribute(UID)
	unitid := NewNullAttribute(UnitID)
	queryerr := c.DBtx.QueryRow(`select (select uid from users where uname=$1),
									    (select unitid from affiliation_units where name=$2)`,
		i[UserName], i[UnitName]).Scan(&uid, &unitid)
	if queryerr != nil {
		log.WithFields(QueryFields(c)).Error(queryerr)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !uid.Valid && i[UserName].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
	}
	if !unitid.Valid && i[UnitName].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	_, err := c.DBtx.Exec(`update grid_access set is_banned = $1, last_updated = NOW()
						   where uid = $2 and fqanid in (select fqanid from grid_fqan where unitid = $3)`,
		i[Suspend], uid, unitid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, nil
}

func getUserGroupsForComputeResource(c APIContext, i Input) (interface{}, []APIError) {

	var apiErr []APIError

	rows, err := c.DBtx.Query(`select cr.type, cr.name, au.name, u.uname, g.name, cag.is_primary
							   from compute_resources cr
									left join affiliation_units au using(unitid)
									left join compute_access_group cag using(compid)
									join users u using (uid)
									join groups g using(groupid)
								where (cr.type=$1 or $1 is null)
									and (au.name = $2 or $2 is null)
								order by cr.type, cr.name, au.name, u.uname`, i[ResourceType], i[UnitName])
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	type resources map[Attribute]interface{}
	out := make([]resources, 0)
	type users map[Attribute]interface{}
	uout := make([]users, 0)
	rname := NewNullAttribute(ResourceName)

	for rows.Next() {
		row := NewMapNullAttribute(ResourceType, ResourceName, UnitName, UserName, GroupName, Primary)
		rows.Scan(row[ResourceType], row[ResourceName], row[UnitName], row[UserName], row[GroupName], row[Primary])

		user := make(users)
		user[UserName] = row[UserName].Data
		user[GroupName] = row[GroupName].Data
		user[Primary] = row[Primary].Data
		uout = append(uout, user)

		if rname.Data != row[ResourceName].Data {
			/* A resource has only one type and belongs to only one resource, so put this out togather. */
			entry := make(resources)
			entry[ResourceName] = row[ResourceName].Data
			entry[ResourceType] = row[ResourceType].Data
			entry[UnitName] = row[UnitName].Data
			entry["users"] = uout
			out = append(out, entry)
			rname.Data = row[ResourceName].Data
			uout = make([]users, 0)
		}
	}

	return out, nil
}

func removeUserFromComputeResource(c APIContext, i Input) (interface{}, []APIError) {

	return nil, nil
}
