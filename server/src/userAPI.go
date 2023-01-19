package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"strings"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	log "github.com/sirupsen/logrus"
)

// IncludeUserAPIs includes all APIs described in this file in an APICollection
func IncludeUserAPIs(c *APICollection) {
	banUser := BaseAPI{
		InputModel{
			Parameter{UserName, true},
			Parameter{Banned, true},
		},
		banUser,
		RoleWrite,
	}
	c.Add("banUser", &banUser)

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
			Parameter{ExternalAttribute, false},
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
			Parameter{Status, false},
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

// banUser       godoc
// @Summary      Stops a user from access via FERRY.
// @Description  Fully bans the user from ALL FERRY use!!  Upon execution, the user will be immediately removed
// @Description  from LDAP and their status will be set to inactive.  The account will be locked so that no method, except this one,
// @Description  can remove the ban.  Removing the ban allows the user's status to be changed.
// @Tags         Users
// @Accept       html
// @Produce      json
// @Param        username       query     string  true  "user to be banned"
// @Param        banned         query     boolean true  "true to ban the user"
// @Success      200  {object}  main.jsonOutput
// @Failure      400  {object}  main.jsonOutput
// @Failure      401  {object}  main.jsonOutput
// @Router /banned [put]
func banUser(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	uid := NewNullAttribute(UID)
	isBanned := NewNullAttribute(Banned)

	err := c.DBtx.QueryRow(`select uid, is_banned from users where uname=$1`,
		i[UserName]).Scan(&uid, &isBanned)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	} else if !uid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
		return nil, apiErr
	} else if isBanned.Data.(bool) == i[Banned].Data.(bool) {
		return nil, nil
	}

	if i[Banned].Data.(bool) {
		_, err = c.DBtx.Exec(`update users set is_banned = true, status=false where uid = $1`, uid.Data)
		if err != nil {
			log.WithFields(QueryFields(c)).Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
			return nil, apiErr
		}
		_, apiErr = removeUserFromLdap(c, i)
		if apiErr != nil {
			return nil, apiErr
		}
	} else {
		// Just because a ban is lifted, you don't set status back to true.  Let them call setUserInfo for that.
		_, err = c.DBtx.Exec(`update users set is_banned = false where uid = $1`, uid.Data)
		if err != nil {
			log.WithFields(QueryFields(c)).Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
			return nil, apiErr
		}
	}
	return nil, nil
}

// getUserCertificateDNs       godoc
// @Summary      Returns DNs registered for users.
// @Description  Returns all the certificate DNs registered for users. If the optional unitname variable
// @Description  is set, it only returns a list of certificate DNs registered with the specified experiment name.
// @Tags         Users
// @Accept       html
// @Produce      json
// @Param        unitname       query     string  false  "limit DNs to specified affiliation"
// @Param        username       query     string  false  "specific user to return DNs for"
// @Success      200  {object}  main.userCertificates
// @Failure      400  {object}  main.jsonOutput
// @Failure      401  {object}  main.jsonOutput
// @Router /getUserCertificateDNs [get]
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

// getAllUsersCertificateDNs       godoc
// @Summary      Returns DNs registered for all users.
// @Description  Returns all the certificate DNs registered for all users.
// @Tags         Users
// @Accept       html
// @Produce      json
// @Param        lastupdated    query     string  false  "return those updated since"  Format(date)
// @Param        status         query     string  false  "return DNs for inactive users, default active"
// @Param        username       query     string  false  "return DNs for specific affiliations"
// @Success      200  {object}  main.userCertificates
// @Failure      400  {object}  main.jsonOutput
// @Failure      401  {object}  main.jsonOutput
// @Router /getAllUsersCertificateDNs [get]
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

// getUserFQANs       godoc
// @Summary      Returns the FQANs a user is assigned.
// @Description  Given a username, returns all the FQANs a user is assigned to broken down by experiment names.
// @Tags         Users
// @Accept       html
// @Produce      json
// @Param        lastupdated    query     string  false  "limit results to records  updated since"  Format(date)
// @Param        unitname       query     string  false  "limit results to FQANs for a specific affiliation"
// @Param        username       query     string  false  "limit results to the user"
// @Success      200  {object}  main.userFQANS
// @Failure      400  {object}  main.jsonOutput
// @Failure      401  {object}  main.jsonOutput
// @Router /getUserFQANs [get]
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
							    is_suspended = false and
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

// getUserGroups godoc
// @Summary      Returns the gid and group names of all the groups the user is member of.
// @Description  Returns the gid and group names of all the groups the user is member of.
// @Tags         Users
// @Accept       html
// @Produce      json
// @Param        lastupdated    query     string  false  "limit results to records  updated since"  Format(date)
// @Param        username       query     string  false  "limit results to the user"
// @Success      200  {object}  main.userGroups
// @Failure      400  {object}  main.jsonOutput
// @Failure      401  {object}  main.jsonOutput
// @Router /getUserGroups [get]
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

// getUserInfo godoc
// @Summary      Return attributes for a user.
// @Description  For a specific user, returns the entity attributes.
// @Tags         Users
// @Accept       html
// @Produce      json
// @Param        username       query     string  true  "user for whom the attributes are to be returned"
// @Success      200  {object}  userAttributes
// @Failure      400  {object}  jsonOutput
// @Failure      401  {object}  jsonOutput
// @Router /getUserInfo [get]
func getUserInfo(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	var row userAttributes

	err := c.DBtx.QueryRow(`select full_name, uid, status, is_groupaccount, expiration_date, voPersonID, is_banned from users
							where uname=$1`, i[UserName]).Scan(&row.FullName, &row.UID, &row.Status, &row.GroupAccount,
		&row.ExpirationDate, &row.VoPersonID, &row.Banned)

	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	} else if err != nil {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
		return nil, apiErr
	}

	return row, nil
}

// addUserToGroup godoc
// @Summary      This is not the method you are looking for.
// @Description  This is probably NOT what you want to run.  This is mostly an internal usage API.  You most likely want
// @Description  setUserAccessToComputeResource which can be run multiple times to add the user to a specific group and cluster.
// @Tags         Users
// @Accept       html
// @Produce      json
// @Param        group          query     string  true  "the group to add the user too"
// @Param        grouptype      query     string  true  "the specific type of group"
// @Param        username       query     string  true  "user to add to the group"
// @Success      200  {object}  main.jsonOutput
// @Failure      400  {object}  main.jsonOutput
// @Failure      401  {object}  main.jsonOutput
// @Router /addUserToGroup [post]
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

// removeUserFromGroup godoc
// @Summary      Remove this group membership from the user.
// @Description  Remove this group membership from the user.
// @Tags         Users
// @Accept       html
// @Produce      json
// @Param        group          query     string  true  "the group to remove the user from"
// @Param        grouptype      query     string  true  "the specific type of group"
// @Param        username       query     string  true  "user to remove from membership in the group"
// @Success      200  {object}  main.jsonOutput
// @Failure      400  {object}  main.jsonOutput
// @Failure      401  {object}  main.jsonOutput
// @Router /removeUserFromGroup [put]
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

// setUserExperimentFQAN godoc
// @Summary      Assign a user to a specific experiment FQAN.
// @Description  Assign a user to a specific experiment FQAN.
// @Tags         Users
// @Accept       html
// @Produce      json
// @Param        fqan           query     string  true  "fqan to assign user too"
// @Param        unitname       query     string  true  "affiliation to limit assignment too"
// @Param        username       query     string  true  "user to be assigned to fqan/affiliation"
// @Success      200  {object}  main.jsonOutput
// @Failure      400  {object}  main.jsonOutput
// @Failure      401  {object}  main.jsonOutput
// @Router /setUserExperimentFQAN [post]
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
		_, queryerr = c.DBtx.Exec(`insert into grid_access (uid, fqanid, is_superuser, is_suspended, last_updated)
								   values($1, $2, false, false, NOW())
								   on conflict (uid, fqanid) do nothing`, uid, fqanid)
		if queryerr != nil {
			log.WithFields(QueryFields(c)).Error(queryerr)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
			return nil, apiErr
		}
	}

	if len(fqanids) > 0 {
		_, apiErr := addOrUpdateUserInLdap(c, i)
		if apiErr != nil {
			log.Warningf("LDAP %s - %s", i[UserName].Data.(string), apiErr[0].Error.Error())
		}
	}

	return nil, nil
}

// setUserShellAndHomeDir godoc
// @Summary      This method sets the user shell and home directory for a specific compute resource.
// @Description  Some users have preferences over the types of shell they use. This method sets the user shell and home directory
// @Description  for a specific compute resource. Users who have not specified any preferences, will get the default value.
// @Tags         Users
// @Accept       html
// @Produce      json
// @Param        homedir        query     string  true  "home directory to set as default"
// @Param        resourcename   query     string  true  "compute resource the default will apply to"
// @Param        shell          query     string  true  "shell to set as default"
// @Param        username       query     string  true  "user whose defaults are being set"
// @Success      200  {object}  main.jsonOutput
// @Failure      400  {object}  main.jsonOutput
// @Failure      401  {object}  main.jsonOutput
// @Router /setUserShellAndHomeDir [put]
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

// setUserShell godoc
// @Summary      This method sets the user shell for all resources in an affiliation unit.
// @Description  Some users have preferences over the types of shell they use. This method sets the user shell for all resources
// @Description  in an affiliation unit. Users who have not specified any preferences, will get the default value.
// @Tags         Users
// @Accept       html
// @Produce      json
// @Param        shell          query     string  true  "shell to set as default"
// @Param        unitname       query     string  true  "affiliation whose resources will have this shell as default"
// @Param        username       query     string  true  "user whose defaults are being set"
// @Success      200  {object}  main.jsonOutput
// @Failure      400  {object}  main.jsonOutput
// @Failure      401  {object}  main.jsonOutput
// @Router /setUserShell [put]
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

// getUserShellAndHomeDir godoc
// @Summary      Return the users prefrences for shell and home dir.
// @Description  Some users have preferences over the types of shell they use. This method returns those preferences.
// @Tags         Users
// @Accept       html
// @Produce      json
// @Param        fqan           query     string  true  "fqan to assign user too"
// @Param        unitname       query     string  true  "affiliation to limit assignment too"
// @Param        username       query     string  true  "user to be assigned to fqan/affiliation"
// @Success      200  {object}  main.userShellAndHomeDir
// @Failure      400  {object}  main.jsonOutput
// @Failure      401  {object}  main.jsonOutput
// @Router /getUserShellAndHomeDir [get]
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

// getUserStorageQuota godoc
// @Summary      Returns the user's storage quota.
// @Description  Returns the storage quota for a resource applied to a user, if any.
// @Tags         Users
// @Accept       html
// @Produce      json
// @Param        resourcename   query     string  true  "resource for which the quota is to be given"
// @Param        unitname       query     string  true  "affiliation the quota is for"
// @Param        username       query     string  true  "user whose quota is to be given"
// @Success      200  {object}  main.userStorageQuota
// @Failure      400  {object}  main.jsonOutput
// @Failure      401  {object}  main.jsonOutput
// @Router /getUserStorageQuota [get]
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

// setUserExternalAffiliationAttribute godoc
// @Summary      Sets an external affiliation attribute and value for a user.
// @Description  It sets an external affiliation attribute and value for a user. Normally, the  attribute name is most likely the cern_username
// @Description  and affiliation value is the actual uname at CERN.
// @Tags         Users
// @Accept       html
// @Produce      json
// @Param        attribute      query     string  true  "attribute, 'key' to be set"
// @Param        username       query     string  true  "user to be assigned the attribute"
// @Param        value          query     string  true  "value provided for the attribute"
// @Success      200  {object}  main.jsonOutput
// @Failure      400  {object}  main.jsonOutput
// @Failure      401  {object}  main.jsonOutput
// @Router /setUserExternalAffiliationAttribute [post]
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

// removeUserExternalAffiliationAttribute godoc
// @Summary      Removes an external affiliation attribute and value from a user.
// @Description  Removes an external affiliation attribute and value from a user.
// @Tags         Users
// @Accept       html
// @Produce      json
// @Param        attribute      query     string  true  "attribute to be removed"
// @Param        username       query     string  true  "user whose attribute is to be removed"
// @Success      200  {object}  main.jsonOutput
// @Failure      400  {object}  main.jsonOutput
// @Failure      401  {object}  main.jsonOutput
// @Router /removeUserExternalAffiliationAttribute [put]
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

// getUserExternalAffiliationAttributes godoc
// @Summary      Returns the external affiliation attributes assigned to a user.
// @Description  Returns the external affiliation attributes assigned to a user.
// @Tags         Users
// @Accept       html
// @Produce      json
// @Param        attribute      query     string  true  "attribute to be removed"
// @Param        username       query     string  true  "user whose attribute is to be removed"
// @Success      200  {object}  main.userExternalAttributes
// @Failure      400  {object}  main.jsonOutput
// @Failure      401  {object}  main.jsonOutput
// @Router /getUserExternalAffiliationAttributes [get]
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
							    and (a.attribute = $3 or $3 is null)
							  order by uname`, uid, i[LastUpdated], i[ExternalAttribute])
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

// addCertificateDNToUser godoc
// @Summary      Adds a DN certificate for the user.
// @Description  Adds a DN certificate for the user, if it does not already exist.
// @Tags         Users
// @Accept       html
// @Produce      json
// @Param        dn             query     string  true "dn to assign to user"
// @Param        unitname       query     string  true "affiliation to associatate with the dn"
// @Param        username       query     string  true "user to whom the dn belongs"
// @Success      200  {object}  main.jsonOutput
// @Failure      400  {object}  main.jsonOutput
// @Failure      401  {object}  main.jsonOutput
// @Router /addCertificateDNToUser [post]
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

// removeUserCertificateDN godoc
// @Summary      Removes a DN certificate from the user.
// @Description  Removes a DN certificate from, the user, if it exists.
// @Tags         Users
// @Accept       html
// @Produce      json
// @Param        dn             query     string  true "dn remove from the user"
// @Param        username       query     string  true "user to whom the dn belongs"
// @Success      200  {object}  main.jsonOutput
// @Failure      400  {object}  main.jsonOutput
// @Failure      401  {object}  main.jsonOutput
// @Router /removeUserCertificateDN [put]
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

// setUserInfo godoc
// @Summary      Updates a set of attributes belonging to a user.
// @Description  Updates a set of attributes belonging to a user.
// @Description  NOTE: A Banned user's status cannot be set to true -- see the API banUser.
// @Tags         Users
// @Accept       html
// @Produce      json
// @Param        expirationdate query     string  false  "date the user's account expires" Format(date)
// @Param        fullname       query     string  false  "proper name of the user"
// @Param        groupaccount   query     boolean false  "true is this is a group account - default is false"
// @Param        status         query     string  false  "false to deactivate the account - default is true"
// @Param        username       query     string  true   "user whose attributes are to be set"
// @Success      200  {object}  main.jsonOutput
// @Failure      400  {object}  main.jsonOutput
// @Failure      401  {object}  main.jsonOutput
// @Router /setUserInfo [put]
func setUserInfo(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	if !i[FullName].Valid && !i[Status].Valid && !i[GroupAccount].Valid &&
		!i[ExpirationDate].Valid && !i[ExpirationDate].AbsoluteNull {
		apiErr = append(apiErr, APIError{errors.New("not enough arguments"), ErrorAPIRequirement})
		return nil, apiErr
	}

	uid := NewNullAttribute(UID)
	expDate := NewNullAttribute(ExpirationDate)
	isBanned := NewNullAttribute(Banned)

	queryerr := c.DBtx.tx.QueryRow(`select uid, expiration_date, is_banned from users where uname = $1`,
		i[UserName]).Scan(&uid, &expDate, &isBanned)
	if queryerr == sql.ErrNoRows {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
		return nil, apiErr
	} else if queryerr != nil {
		log.WithFields(QueryFields(c)).Error(queryerr)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	} else if i[Status].Valid && i[Status].Data.(bool) && isBanned.Data.(bool) {
		// Never allow status to be set to true if the user has been banned!
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, "user is banned"))
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

	if i[FullName].Valid {
		input := Input{
			UserName: i[UserName],
			FullName: i[FullName],
		}
		// syncLdapWithFerry DOES NOT modify user attributes, so, this must succeed.
		// TODO sync... should be updated, we should not fail for this.
		_, apiErr2 := modifyUserLdapAttributes(c, input)
		if apiErr2 != nil {
			return nil, apiErr2
		}
	}
	if i[Status].Valid {
		// syncLdapWithFerry will add/remove the user if ldap fails. It runs nightly on batch.
		// So, no need to generate an error if ldap failed.
		if i[Status].Data.(bool) {
			_, _ = addOrUpdateUserInLdap(c, i)
		} else if apiErr == nil {
			_, _ = removeUserFromLdap(c, i)
		}
	}

	return nil, nil
}

// createUser godoc
// @Summary      Adds a new user to FERRY.
// @Description  Adds a new user to FERRY.  Note: FERRY's cronjob which talks to userDB and services, normally handles this.
// @Tags         Users
// @Accept       html
// @Produce      json
// @Param        expirationdate query     string  false  "date the user's account expires" Format(date)
// @Param        fullname       query     string  true   "proper name of the user"
// @Param        groupaccount   query     boolean true   "true is this is a group account - default is false"
// @Param        status         query     string  true   "false to deactivate the account - default is true"
// @Param        uid            query     int     true   "the uid for of this new user"
// @Param        username       query     string  true   "user's account name"
// @Success      200  {object}  main.jsonOutput
// @Failure      400  {object}  main.jsonOutput
// @Failure      401  {object}  main.jsonOutput
// @Router /createUser [post]
func createUser(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	groupid := NewNullAttribute(GroupID)
	expDate := i[ExpirationDate].Default("2038-01-01")

	if strings.Contains(i[UserName].Data.(string), " ") {
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "Spaces are not allowed in uname."))
		return nil, apiErr
	}

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

	newUUID := uuid.New().String()
	_, err = c.DBtx.Exec(`insert into users (uname, uid, full_name, status, expiration_date, vopersonid, last_updated)
						  values ($1, $2, $3, $4, $5, $6, NOW())`,
		i[UserName], i[UID], i[FullName], i[Status], expDate, newUUID)
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

// getMemberAffiliations godoc
// @Summary      Returns the affiliations the user is a member of.
// @Description  Returns the affiliations the user is a member of.
// @Tags         Users
// @Accept       html
// @Produce      json
// @Param        experiment     query     string  true  "limit results to a specific affiliation"
// @Param        username       query     string  true  "user to list whose membership will be listed"
// @Success      200  {object}  main.userMemberAffiliations
// @Failure      400  {object}  main.jsonOutput
// @Failure      401  {object}  main.jsonOutput
// @Router /getMemberAffiliations [get]
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

// getUserUname godoc
// @Summary      Returns the username associated with a UID
// @Description  Returns the username associated with a UID.
// @Tags         Basic Queries
// @Accept       html
// @Produce      json
// @Param        uid       query     int  true  "uid for which the username will be returned"
// @Success      200  {object}  main.jsonOutput
// @Failure      400  {object}  main.jsonOutput
// @Failure      401  {object}  main.jsonOutput
// @Router /getUserUname [get]
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

// getUserUID godoc
// @Summary      Returns the UID associated with a username.
// @Description  Returns the UID associated with a username.
// @Tags         Basic Queries
// @Accept       html
// @Produce      json
// @Param        username       query     string  true  "user whose UID will be returned"
// @Success      200  {object}  main.jsonOutput
// @Failure      400  {object}  main.jsonOutput
// @Failure      401  {object}  main.jsonOutput
// @Router /getUserUID [get]
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

// dropUser godoc
// @Summary      Deletes (actually it archives) user from the database.
// @Description  Intended for internal cronjob use only.  Moves some  user data to the archive table.  Purposely fails if user is in any table
// @Description  except user and user_group.   If you need to have someone removed from all tables, ask the FERRY adimistrator
// @Description  to run the archiveUser.py script.
// @Tags         Users
// @Accept       html
// @Produce      json
// @Param        username       query     string  true  "user to be deleted"
// @Success      200  {object}  main.jsonOutput
// @Failure      400  {object}  main.jsonOutput
// @Failure      401  {object}  main.jsonOutput
// @Router /dropUser [put]
func dropUser(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	uname := NewNullAttribute(UserName)

	err := c.DBtx.QueryRow(`select uname from users where uid = $1`, i[UID]).Scan(&uname)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	// Must use the table columns in the json output to match what is
	// in the user_archives table.
	const jUserGroup Attribute = "user_group"
	const jUsers Attribute = "users"

	const jUID Attribute = "uid"
	const jGroupID Attribute = "groupid"
	const jLeader Attribute = "is_leader"
	const jLastUpdated Attribute = "last_updated"

	const jUserName Attribute = "uname"
	const jStatus Attribute = "status"
	const jExpirationDate Attribute = "expiration_date"
	const jFullName Attribute = "full_name"
	const jGroupAccount Attribute = "is_groupaccount"
	const jSharedAccount Attribute = "is_sharedaccount"
	const jVoPersonID Attribute = "vopersonid"

	type jsonentry map[Attribute]interface{}
	type jsonlist []interface{}

	jtables := jsonentry{
		jUserGroup: make(jsonlist, 0),
		jUsers:     make(jsonlist, 0),
	}

	rowEntry := jsonentry{
		jUID:         TypeInt,
		jGroupID:     TypeInt,
		jLeader:      TypeBool,
		jLastUpdated: TypeDate,
	}

	rows, err := DBptr.Query(`select uid, groupid, is_leader, last_updated from user_group where uid = $1`, i[UID])
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	list := make(jsonlist, 0)
	row := NewMapNullAttribute(jUID, jGroupID, jLeader, jLastUpdated)
	for rows.Next() {
		rows.Scan(row[jUID], row[jGroupID], row[jLeader], row[jLastUpdated])
		rowEntry[jUID] = row[jUID].Data
		rowEntry[jGroupID] = row[jGroupID].Data
		rowEntry[jLeader] = row[jLeader].Data
		rowEntry[jLastUpdated] = row[jLastUpdated].Data
		list = append(list, rowEntry)
		rowEntry = jsonentry{
			jUID:         TypeInt,
			jGroupID:     TypeInt,
			jLeader:      TypeBool,
			jLastUpdated: TypeDate,
		}
	}
	jtables[jUserGroup] = list

	uid := NewNullAttribute(UID)
	status := NewNullAttribute(Status)
	expDate := NewNullAttribute(Experiment)
	lastUpdated := NewNullAttribute(LastUpdated)
	fullName := NewNullAttribute(FullName)
	isGroup := NewNullAttribute(GroupAccount)
	isShared := NewNullAttribute(SharedAccount)
	voPersonID := NewNullAttribute(VoPersonID)

	err = c.DBtx.tx.QueryRow(`select uid, uname, status, expiration_date, last_updated, full_name, is_groupaccount, is_sharedaccount, voPersonID
							  from users
							  where uid = $1`, i[UID]).Scan(&uid, &uname, &status, &expDate, &lastUpdated, &fullName, &isGroup, &isShared, &voPersonID)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	r := jsonentry{
		jUID:            TypeInt,
		jUserName:       TypeString,
		jStatus:         TypeBool,
		jExpirationDate: TypeDate,
		jLastUpdated:    TypeDate,
		jFullName:       TypeString,
		jGroupAccount:   TypeBool,
		jSharedAccount:  TypeBool,
		jVoPersonID:     TypeString,
	}
	r[jUID] = uid.Data
	r[jUserName] = uname.Data
	r[jStatus] = status.Data
	r[jExpirationDate] = expDate.Data
	r[jLastUpdated] = lastUpdated.Data
	r[jFullName] = fullName.Data
	r[jGroupAccount] = isGroup.Data
	r[jSharedAccount] = isShared.Data
	r[jVoPersonID] = voPersonID.Data

	jtables[jUsers] = r

	parsedOut, err := json.Marshal(jtables)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err.Error())
		apiErr = append(apiErr, DefaultAPIError(ErrorText, err))
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`delete from user_group where uid=$1`, uid.Data)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		if strings.Contains(err.Error(), "violates foreign key constraint") {
			apiErr = append(apiErr, APIError{errors.New("cannot drop as this user has associations which must be archived"), ErrorAPIRequirement})
		} else {
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		}
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`delete from users where uid=$1`, uid.Data)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		if strings.Contains(err.Error(), "violates foreign key constraint") {
			apiErr = append(apiErr, APIError{errors.New("cannot drop as this user has associations which must be archived"), ErrorAPIRequirement})
		} else {
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		}
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`insert into user_archives (uid, uname, user_data, date_deleted) values ($1, $2, $3, now())`,
		uid, uname, string(parsedOut))
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	input := Input{
		UserName: uname,
	}

	_, apiErr = getUserLdapInfo(c, i) // Returns an error if user is not in ldap
	if apiErr == nil {
		_, apiErr = removeUserFromLdap(c, input)
		if apiErr != nil {
			log.Warningf("LDAP %s - %s", i[UserName].Data.(string), apiErr[0].Error.Error())
		}
	}
	return nil, nil
}

// getUserAccessToComputeResources godoc
// @Summary      Return a list of all the compute and storage resources the user has access to.
// @Description  Return a list of all the compute and storage resources the user has access to.
// @Tags         Users
// @Accept       html
// @Produce      json
// @Param        lastupdated    query     string  false  "limit results to records  updated since"  Format(date)
// @Param        username       query     string  true  "user to be assigned to fqan/affiliation"
// @Success      200  {object}  main.userComputeResources
// @Failure      400  {object}  main.jsonOutput
// @Failure      401  {object}  main.jsonOutput
// @Router /getUserAccessToComputeResources [get]
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

	rows, err := c.DBtx.Query(`select cr.name, cr.type, ca.shell, ca.home_dir, g.name from
								compute_access as ca join
								compute_resources as cr using(compid) join
								compute_access_group as cag using(compid,uid) join
								groups as g using(groupid)
							   where ca.uid = $1 and (ca.last_updated>=$2 or $2 is null)`,
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
		row := NewMapNullAttribute(ResourceName, ResourceType, Shell, HomeDir, GroupName)
		rows.Scan(row[ResourceName], row[ResourceType], row[Shell], row[HomeDir], row[GroupName])

		if row[ResourceName].Valid {
			entry := jsonentry{
				ResourceName: row[ResourceName].Data,
				ResourceType: row[ResourceType].Data,
				Shell:        row[Shell].Data,
				HomeDir:      row[HomeDir].Data,
				GroupName:    row[GroupName].Data,
			}
			out = append(out, entry)
		}
	}

	return out, nil
}

// getStorageQuotas godoc
// @Summary      Returns the storage quota allocated for a user on a resource.
// @Description  Returns the storage quota allocated for a user on a resource.
// @Tags         Users
// @Accept       html
// @Produce      json
// @Param        groupname      query     string  false  "group to limit results to"
// @Param        lastupdated    query     string  false  "limit results to records  updated since"  Format(date)
// @Param        resourcename   query     string  false  "limit results to a specific resource"
// @Param        username       query     string  false  "limit results to a specific user"
// @Success      200  {object}  main.userStorageQuotas
// @Failure      400  {object}  main.jsonOutput
// @Failure      401  {object}  main.jsonOutput
// @Router /getStorageQuotas [get]
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

// setUserAccessToComputeResource godoc
// @Summary      Allows the user to have interactive access with a resource.
// @Description  Given a username and a resource, it allows the user to have interactive access to the resource.
// @Tags         Users
// @Accept       html
// @Produce      json
// @Param        groupname      query     string  true   "group through which access is provided"
// @Param        homedir        query     string  false  "home directory to use, if different from the default"
// @Param        primary        query     boolean false  "if true this group is primary for the resource -- the user's primary group records do NOT appear in the passwd file"
// @Param        resourcename   query     string  true   "compute resource to which the user is being given access"
// @Param        shell          query     string  false  "shell to use, if different from the default"
// @Param        username       query     string  true   "user being given access to the resource"
// @Success      200  {object}  main.jsonOutput
// @Failure      400  {object}  main.jsonOutput
// @Failure      401  {object}  main.jsonOutput
// @Router /setUserAccessToComputeResource [put]
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

// getAllUsers godoc
// @Summary      List all user accounts
// @Description  Returns all user accounts
// @Tags         Users
// @Accept       html
// @Produce      json
// @Param        status         query     boolean false  "return only those with the specified status"  Format(true/false)
// @Param        lastupdated    query     string  false  "return those updated since"  Format(date)
// @Success      200  {object}  allUsersAttributes
// @Failure      400  {object}  jsonOutput
// @Failure      401  {object}  jsonOutput
// @Router /getAllUsers [get]
func getAllUsers(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	status := i[Status].Default(false)

	rows, err := DBptr.Query(`select uname, uid, full_name, status, cast(expiration_date as text), voPersonID, is_banned from users
							  where (status=$1 or not $1) and (last_updated>=$2 or $2 is null)
							  order by uname`, status, i[LastUpdated])
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	var out []allUsersAttributes
	var row allUsersAttributes

	for rows.Next() {
		rows.Scan(&row.UserName, &row.UID, &row.FullName, &row.Status, &row.ExpirationDate, &row.VoPersonID, &row.Banned)
		out = append(out, row)
	}

	return out, nil
}

// getAllUsersFQANs godoc
// @Summary      Returns all FQANs for all users
// @Description  Returns all FQANs for all users.  By default includes suspended FQANS - marked as such.
// @Tags         Users
// @Accept       html
// @Produce      json
// @Param        lastupdated     query     string  false  "limit results to records  updated since"  Format(date)
// @Param        suspended       query     boolean  false  "limit to suspended or not suspended"
// @Success      200  {object}  main.userAllUserFQANs
// @Failure      400  {object}  main.jsonOutput
// @Failure      401  {object}  main.jsonOutput
// @Router /getAllUsersFQANs [get]
func getAllUsersFQANs(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	rows, err := DBptr.Query(`select uname, fqan, name, ga.is_suspended from grid_access as ga
							  join grid_fqan as gf using(fqanid)
							  join users as u using(uid)
							  join affiliation_units as au using(unitid)
							  where (ga.last_updated>=$2 or gf.last_updated>=$2 or
									  u.last_updated>=$2 or au.last_updated>=$2 or $2 is null)
									and (ga.is_suspended = $1 or $1 is null)  order by uname`,
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

// setUserGridAccess godoc
// @Summary      Restricts the user the privileges associated with an experiment's FQANs.
// @Description  Allows the application of the "Naughty Policy" by restricting the user's privileges associated with an
// @Description  experiment's FQANs until it is restored by this same method.
// @Tags         Users
// @Accept       html
// @Produce      json
// @Param        suspend        query     boolean true  "true to restrict the user from using the FQAN, false to remove the restriction"
// @Param        unitname       query     string  true  "affiliation to limit the user's FQAN access on"
// @Param        username       query     string  true  "user whose FQAN is to be limited"
// @Success      200  {object}  jsonOutput
// @Failure      400  {object}  jsonOutput
// @Failure      401  {object}  jsonOutput
// @Router /setUserGridAccess [put]
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

	_, err := c.DBtx.Exec(`update grid_access set is_suspended = $1, last_updated = NOW()
						   where uid = $2 and fqanid in (select fqanid from grid_fqan where unitid = $3)`,
		i[Suspend], uid, unitid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	_, apiErr = addOrUpdateUserInLdap(c, i)

	return nil, apiErr
}

// getUserGroupsForComputeResource godoc
// @Summary      Returns attributes of compute resources and associated users.
// @Description  Returns attributes of compute resources and associated users.
// @Tags         Compute and Storage Resources
// @Accept       html
// @Produce      json
// @Param        resourcetype   query     string  false  "type of the compute resource to restrict results to"
// @Param        status         query     boolean false  "status of the users to restrict results to, default all"
// @Param        unitname       query     string  false  "affiliation to limit results to"
// @Success      200  {object}  userGroupComputeResourcesMap
// @Failure      400  {object}  main.jsonOutput
// @Failure      401  {object}  main.jsonOutput
// @Router /getUserGroupsForComputeResource [get]
func getUserGroupsForComputeResource(c APIContext, i Input) (interface{}, []APIError) {

	var apiErr []APIError

	rows, err := c.DBtx.Query(`select cr.type, cr.name, au.name, u.uname, g.name, cag.is_primary, u.status
							   from compute_resources cr
									left join affiliation_units au using(unitid)
									left join compute_access_group cag using(compid)
									join users u using (uid)
									join groups g using(groupid)
								where (cr.type = $1 or $1 is null)
									and (au.name = $2 or $2 is null)
									and (u.status = $3 or $3 is null)
								order by au.name, cr.name, cr.type, u.uname`, i[ResourceType], i[UnitName], i[Status])
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	type jsonentry map[Attribute]interface{}

	const Users Attribute = "users"
	user := jsonentry{
		UserName:  "",
		GroupName: "",
		Primary:   "",
		Status:    "",
	}

	resource := jsonentry{
		ResourceName: "",
		ResourceType: "",
		UnitName:     "",
		Users:        make([]jsonentry, 0),
	}

	var dejaVu bool = false
	out := make([]jsonentry, 0)

	for rows.Next() {
		row := NewMapNullAttribute(ResourceType, ResourceName, UnitName, UserName, GroupName, Primary, Status)
		rows.Scan(row[ResourceType], row[ResourceName], row[UnitName], row[UserName], row[GroupName], row[Primary], row[Status])
		if !dejaVu {
			dejaVu = true
			resource[UnitName] = row[UnitName].Data
			resource[ResourceName] = row[ResourceName].Data
			resource[ResourceType] = row[ResourceType].Data
		}

		if row[ResourceName].Data != resource[ResourceName] {
			out = append(out, resource)
			resource = jsonentry{
				ResourceName: row[ResourceName].Data,
				ResourceType: row[ResourceType].Data,
				UnitName:     row[UnitName].Data,
				Users:        make([]jsonentry, 0),
			}
		}

		user = jsonentry{
			UserName:  row[UserName].Data,
			GroupName: row[GroupName].Data,
			Primary:   row[Primary].Data,
			Status:    row[Status].Data,
		}
		resource[Users] = append(resource[Users].([]jsonentry), user)
	}
	// Add the last entry
	if dejaVu {
		out = append(out, resource)
	}

	return out, nil
}

// removeUserFromComputeResource godoc
// @Summary      Removes the user from the specified compute resource.
// @Description  Removes the user from the specified compute resource.
// @Tags         Users
// @Accept       html
// @Produce      json
// @Param        resourcename   query     string  true  "compute resource to remove user from"
// @Param        resourcetype   query     string  type  "type of the compute resource to remove user from"
// @Param        username       query     string  true  "user to be disassociated from the resource"
// @Success      200  {object}  main.jsonOutput
// @Failure      400  {object}  main.jsonOutput
// @Failure      401  {object}  main.jsonOutput
// @Router /removeUserFromComputeResource [put]
func removeUserFromComputeResource(c APIContext, i Input) (interface{}, []APIError) {

	var apiErr []APIError
	var uid = NewNullAttribute(UID)
	var compid = NewNullAttribute(ResourceID)

	err := c.DBtx.QueryRow(`select
							(select uid from users where uname = $1),
							(select compid from compute_resources where name = $2 and type = $3)`,
		i[UserName], i[ResourceName], i[ResourceType]).Scan(&uid, &compid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	if !uid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
	}
	if !compid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, ResourceName))
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`delete from compute_access_group where compid=$1 and uid=$2`, compid, uid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	_, err = c.DBtx.Exec(`delete from compute_access where compid=$1 and uid=$2`, compid, uid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, nil
}
