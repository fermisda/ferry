package main

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	"github.com/go-ldap/ldap/v3"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

func IncludeLdapAPIs(c *APICollection) {
	getUserLdapInfo := BaseAPI{
		InputModel{
			Parameter{UserName, true},
		},
		getUserLdapInfo,
		RoleRead,
	}
	c.Add("getUserLdapInfo", &getUserLdapInfo)

	addOrUpdateUserInLdap := BaseAPI{
		InputModel{
			Parameter{UserName, true},
		},
		addOrUpdateUserInLdap,
		RoleWrite,
	}
	c.Add("addOrUpdateUserInLdap", &addOrUpdateUserInLdap)

	syncLdapWithFerry := BaseAPI{
		InputModel{},
		syncLdapWithFerry,
		RoleWrite,
	}
	c.Add("syncLdapWithFerry", &syncLdapWithFerry)

	removeUserFromLdap := BaseAPI{
		InputModel{
			Parameter{UserName, true},
		},
		removeUserFromLdap,
		RoleWrite,
	}
	c.Add("removeUserFromLdap", &removeUserFromLdap)

	getCapabilitySet := BaseAPI{
		InputModel{
			Parameter{UnitName, false},
			Parameter{SetName, false},
			Parameter{Role, false},
		},
		getCapabilitySet,
		RoleRead,
	}
	c.Add("getCapabilitySet", &getCapabilitySet)

	createCapabilitySet := BaseAPI{
		InputModel{
			Parameter{SetName, true},
			Parameter{Pattern, true},
			Parameter{TokenSubject, false},    // default = capabilitySetName@fnal.gov  in ldap: eduPersonPrincipalName
			Parameter{VaultStorageKey, false}, // default = capabilitySetName  in ldap: voPersonApplicationUID
		},
		createCapabilitySet,
		RoleWrite,
	}
	c.Add("createCapabilitySet", &createCapabilitySet)

	setCapabilitySetAttributes := BaseAPI{
		InputModel{
			Parameter{SetName, true},
			Parameter{TokenSubject, false},
			Parameter{VaultStorageKey, false},
		},
		setCapabilitySetAttributes,
		RoleWrite,
	}
	c.Add("setCapabilitySetAttributes", &setCapabilitySetAttributes)

	dropCapabilitySet := BaseAPI{
		InputModel{
			Parameter{SetName, true},
		},
		dropCapabilitySet,
		RoleWrite,
	}
	c.Add("dropCapabilitySet", &dropCapabilitySet)

	addScopeToCapabilitySet := BaseAPI{
		InputModel{
			Parameter{SetName, true},
			Parameter{Pattern, true},
		},
		addScopeToCapabilitySet,
		RoleWrite,
	}
	c.Add("addScopeToCapabilitySet", &addScopeToCapabilitySet)

	removeScopeFromCapabilitySet := BaseAPI{
		InputModel{
			Parameter{SetName, true},
			Parameter{Pattern, true},
		},
		removeScopeFromCapabilitySet,
		RoleWrite,
	}
	c.Add("removeScopeFromCapabilitySet", &removeScopeFromCapabilitySet)

	addCapabilitySetToFQAN := BaseAPI{
		InputModel{
			Parameter{SetName, true},
			Parameter{UnitName, true},
			Parameter{Role, true},
		},
		addCapabilitySetToFQAN,
		RoleWrite,
	}
	c.Add("addCapabilitySetToFQAN", &addCapabilitySetToFQAN)

	removeCapabilitySetFromFQAN := BaseAPI{
		InputModel{
			Parameter{SetName, true},
			Parameter{UnitName, true},
			Parameter{Role, true},
		},
		removeCapabilitySetFromFQAN,
		RoleWrite,
	}
	c.Add("removeCapabilitySetFromFQAN", &removeCapabilitySetFromFQAN)

	updateLdapForAffiliation := BaseAPI{
		InputModel{
			Parameter{UnitName, true},
		},
		updateLdapForAffiliation,
		RoleWrite,
	}
	c.Add("updateLdapForAffiliation", &updateLdapForAffiliation)

	updateLdapForCapabilitySet := BaseAPI{
		InputModel{
			Parameter{SetName, true},
		},
		updateLdapForCapabilitySet,
		RoleWrite,
	}
	c.Add("updateLdapForCapabilitySet", &updateLdapForCapabilitySet)

	modifyUserLdapAttributes := BaseAPI{
		InputModel{
			Parameter{UserName, true},
			Parameter{FullName, false},
		},
		modifyUserLdapAttributes,
		RoleWrite,
	}
	c.Add("modifyUserLdapAttributes", &modifyUserLdapAttributes)

}

func getUserLdapInfo(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError
	var lData LDAPData
	var vopid sql.NullString

	uid := NewNullAttribute(UID)

	err := DBptr.QueryRow(`select uid, vopersonid from users where uname=$1`, i[UserName]).Scan(&uid, &vopid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	} else if !uid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
		return nil, apiErr
	} else if len(vopid.String) == 0 {
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "user is not in LDAP"))
		return nil, apiErr
	}
	lData.voPersonID = vopid.String

	con, err := LDAPgetConnection(false)
	if err != nil {
		msg := fmt.Sprintf("LDAP, connection failed: %v", err)
		log.Error(msg)
		apiErr = append(apiErr, DefaultAPIError(ErrorText, msg))
		return nil, apiErr
	}

	lData, err = LDAPgetUserData(lData.voPersonID, con)
	if err != nil {
		con.Close()
		log.Error(fmt.Sprintf("From LDAPgetUserData: %s", err))
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "Unable to get user's LDAP data."))
		return nil, apiErr
	}
	con.Close()

	out := make(map[Attribute]interface{})
	out["dn"] = lData.dn
	out["objectClass"] = lData.objectClass
	out["voPersonID"] = lData.voPersonID
	out["voPersonExternalID"] = lData.voPersonExternalID
	out["uid"] = lData.uid
	out["sn"] = lData.sn
	out["cn"] = lData.cn
	out["givenName"] = lData.givenName
	out["mail"] = lData.mail
	out["eduPersonPrincipalName"] = lData.eduPersonPrincipalName
	out["eduPersonEntitlement"] = lData.eduPersonEntitlement
	out["isMemberOf"] = lData.isMemberOf

	return out, apiErr
}

// Adds a new user to ldap and updates ldap for an existing user's FQANs.
func addOrUpdateUserInLdap(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	con, err := LDAPgetConnection(false)
	if err != nil {
		msg := fmt.Sprintf("LDAP, connection failed: %v", err)
		log.Error(msg)
		apiErr = append(apiErr, DefaultAPIError(ErrorText, msg))
		return nil, apiErr
	}

	lData, apiErr := addUserToLdapBase(c, i, con)
	if apiErr == nil {
		vops := []string{lData.voPersonID}
		_, apiErr = updateLdapForUserSet(c, vops, con)
	}
	con.Close()

	return nil, apiErr
}

// Syncronize LDAP to FERRY where FERRY is the source of truth.
//   Removes all records in LDAP which have no corresponding record in FERRY (identified by users.voPersionID is null).
//   Adds all FERRY users to LDAP which are missing - have FQAN records with associated capability set, but are not in LDAP.
//   Verifies the capability sets and groups are correct for each user, per their FQANs, correcting those which are not correct.
// Due to the third step, this method may take quite a while.
func syncLdapWithFerry(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	type jsonentry map[Attribute]interface{}
	type jsonlist []interface{}

	const Removed Attribute = "removedFromLdap"
	const Added Attribute = "addedToLdap"
	const Updated Attribute = "updatedLdapData"

	entry := jsonentry{
		Removed: make(jsonlist, 0),
		Added:   make(jsonlist, 0),
		Updated: make(jsonlist, 0),
	}

	con, err := LDAPgetConnection(true)
	if err != nil {
		msg := fmt.Sprintf("LDAP, connection with paging failed: %v", err)
		log.Error(msg)
		apiErr = append(apiErr, DefaultAPIError(ErrorText, msg))
		return nil, apiErr
	}

	// First is to remove all LDAP users which FERRY does not have registered - except the required ones in the config file.

	ldapUsers, err := LDAPgetAllVoPersonIDs(con)
	if err != nil {
		con.Close()
		log.Errorf("LDAPgetAllUsers failed: %s", err)
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "Unable to get all users from ldap"))
		return nil, apiErr
	}
	con.Close()
	con, err = LDAPgetConnection(false)
	if err != nil {
		msg := fmt.Sprintf("LDAP, connection failed: %v", err)
		log.Error(msg)
		apiErr = append(apiErr, DefaultAPIError(ErrorText, msg))
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`CREATE TEMPORARY TABLE temp_vopersonids (voPersonID text not null);`)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	valueStrings := make([]string, 0, len(ldapUsers))
	for _, voPersonID := range ldapUsers {
		valueStrings = append(valueStrings, fmt.Sprintf("('%s')", voPersonID))
	}
	smt := fmt.Sprintf("INSERT INTO temp_vopersonids (voPersonID) VALUES %s", strings.Join(valueStrings, ","))
	_, err = c.DBtx.Exec(smt)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	rows, err := c.DBtx.Query(`select voPersonID from temp_voPersonids
	                           except
							   select cast(voPersonID as text) from users where voPersonID is not null`)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	requiredAccts := strings.Split(requiredAccounts, ",")
	for rows.Next() {
		var deleteVoPersonID string

		rows.Scan(&deleteVoPersonID)
		if stringInSlice(deleteVoPersonID, requiredAccts) {
			continue
		}
		llData, err := LDAPgetUserData(deleteVoPersonID, con)
		if err != nil {
			log.Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorText, fmt.Sprintf("Unable to get user's LDAP data. voPersonID: %s", deleteVoPersonID)))
			return entry, apiErr
		}
		err = LDAPremoveUser(deleteVoPersonID, con)
		if err != nil {
			log.Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorText, fmt.Sprintf("Unable to remove user from LDAP: %s - %s", llData.mail, deleteVoPersonID)))
			con.Close()
			return entry, apiErr
		}
		entry[Removed] = append(entry[Removed].(jsonlist), fmt.Sprintf("uname: %s voPersonID: %s.", llData.uid, deleteVoPersonID))
	}

	// Second, is to add in all the users that FERRY has registered as in LDAP but are missing.

	// Get Both those FERRY thinks are in LDAP and those that are not but should be
	rows, err = c.DBtx.Query(`select uname, voPersonID
							  from users as u
							  where u.status is true
								and u.is_groupaccount is false
								and u.is_sharedaccount is false
							  order by u.uid`)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return entry, apiErr
	}

	type userdata struct {
		uname      string
		voPersonID string
	}
	users := make([]userdata, 0)

	for rows.Next() {
		var u, v string
		rows.Scan(&u, &v)
		x := userdata{
			uname:      u,
			voPersonID: v,
		}
		users = append(users, x)
	}

	// Add all missing users to LDAP first.
	var voPersonIDs []string
	for _, u := range users {
		var reallyInLdap = false
		if u.voPersonID != "" {
			// DB says the user is in LDAP, but... Ensure the user really is in LDAP
			for _, voPersonID := range ldapUsers {
				if u.voPersonID == voPersonID {
					reallyInLdap = true
					break
				}
			}
		}
		if (u.voPersonID == "") || !reallyInLdap {
			n := NewNullAttribute(UserName).Default(u.uname)
			input := Input{UserName: n}
			lData, apiErr := addUserToLdapBase(c, input, con)
			if len(apiErr) > 0 {
				con.Close()
				log.Errorf("ldapAPI: addUsertoLdapBase: error on uname: %s", u.uname)
				return entry, apiErr
			}
			entry[Added] = append(entry[Added].(jsonlist), fmt.Sprintf("uname: %s voPersonID: %s", u.uname, lData.voPersonID))
			voPersonIDs = append(voPersonIDs, lData.voPersonID)
		} else {
			voPersonIDs = append(voPersonIDs, u.voPersonID)
		}
	}

	// Third, is to ensure the eduPersonEntitilements and groups for every user is correct and update those that are not correct.

	updated, apiErr := updateLdapForUserSet(c, voPersonIDs, con)
	for _, u := range updated {
		entry[Updated] = append(entry[Updated].(jsonlist), u)
	}
	if len(apiErr) > 0 {
		con.Close()
		log.Errorf("ldapAPI: updateLdapForUserSet: error on uname: %s", apiErr[0].Error)
		return entry, apiErr
	}

	con.Close()

	return entry, nil
}

// Constructs a wlcggroup from the fqan and unitname
func getWlcgGroup(fqan string, unitname string) string {
	parts := strings.SplitAfter(fqan, "/Role=")
	if len(parts) == 1 {
		return ""
	}
	parts = strings.Split(parts[1], "/")
	role := strings.ToLower(parts[0])
	if role == "null" {
		return ""
	} else if role == "analysis" {
		return "/" + unitname
	}
	return "/" + unitname + "/" + role
}

// Adds a user to LDAP but does NOT deal with eduPersonEntitilments or isMemberOf.  see updateLdapForUserSet for that.
// This method ensures a user, who listed in the DB, is also in LDAP.  It not the user is added to LDAP.
func addUserToLdapBase(c APIContext, i Input, con *ldap.Conn) (LDAPData, []APIError) {
	var apiErr []APIError
	var lData LDAPData

	emailSuffix := "fnal.gov"
	uname := NewNullAttribute(UserName)
	uid := NewNullAttribute(UID)
	lData.objectClass = []string{"person", "organizationalPerson", "inetOrgPerson", "eduMember", "eduPerson", "voPerson"}

	err := c.DBtx.QueryRow(`select uid, uname, full_name,
								   split_part(full_name, ' ', 2),
								   split_part(full_name, ' ', 1)  from users where uname=$1`,
		i[UserName]).Scan(&uid, &uname, &lData.givenName, &lData.sn, &lData.cn)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return lData, apiErr
	}

	err = c.DBtx.QueryRow(`select voPersonID from users
						   where uid = $1 and voPersonID is not null`, uid).Scan(&lData.voPersonID)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return lData, apiErr
	}
	// Ensure the user really is in LDAP (we don't have 2 phase commit - so we must test), if a record is in LDAP, we are done.  If not, then use the
	// voPersonID from the DB and add them.  If no voPersonID exists for the use, then create one.
	if len(lData.voPersonID) > 0 {
		llData, err := LDAPgetUserData(lData.voPersonID, con)
		if err != nil {
			log.Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorText, "Unable to get user's LDAP data."))
			return llData, apiErr
		}
		if llData.dn != "" {
			// User is in both DB and LDAP, we're outta here!
			return llData, nil
		}
	}
	// Create a voPersionID iff the DB did not find one for this user.
	if len(lData.voPersonID) == 0 {
		lData.voPersonID = uuid.New().String()
	}

	lData.dn = fmt.Sprintf("voPersonID=%s,%s", lData.voPersonID, ldapBaseDN)
	lData.mail = fmt.Sprintf("%s@%s", uname.Data, emailSuffix)
	lData.eduPersonPrincipalName = lData.voPersonID
	lData.uid = uname.Data.(string)
	lData.voPersonExternalID = lData.mail

	err = LDAPaddUser(lData, con)
	if err != nil {
		log.Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "Unable to store user in LDAP"))
		return lData, apiErr
	}

	_, err = c.DBtx.Exec(`update users set vopersonid=$1 where uid=$2`, lData.voPersonID, uid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return lData, apiErr
	}

	return lData, nil
}

func removeUserFromLdap(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	uid := NewNullAttribute(UID)

	err := c.DBtx.QueryRow(`select uid from users where uname=$1`, i[UserName]).Scan(&uid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !uid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
		return nil, apiErr
	}

	var voPersonID string

	err = c.DBtx.QueryRow(`select voPersonID from users where uid=$1 and voPersonId is not null`, uid).Scan(&voPersonID)
	if err != nil {
		if err == sql.ErrNoRows {
			// Ferry says user is not in LDAP
			return nil, nil
		}
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	con, err := LDAPgetConnection(false)
	if err != nil {
		msg := fmt.Sprintf("LDAP, connection failed: %v", err)
		log.Error(msg)
		apiErr = append(apiErr, DefaultAPIError(ErrorText, msg))
		return nil, apiErr
	}

	lerr := LDAPremoveUser(voPersonID, con)
	if lerr != nil && lerr.Error() != ldapErrNoSuchObject {
		log.Error(lerr)
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "Unable to remove user from LDAP"))
		con.Close()
		return nil, apiErr
	}
	con.Close()

	_, err = c.DBtx.Exec(`update users set voPersonID=null where uid = $1`, uid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, apiErr
}

func getCapabilitySet(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	unitid := NewNullAttribute(UnitID)
	setid := NewNullAttribute(SetID)
	role := NewNullAttribute(Role)

	var roleCnt int

	if i[Role].Valid {
		role.Default("%/role=" + i[Role].Data.(string) + "/%")
	}

	err := c.DBtx.QueryRow(`select (select unitid  from affiliation_units  where name=$1),
								   (select setid from capability_sets where name=$2),
								   (select count(fqan) from grid_fqan join affiliation_units using (unitid)
								     where name=$1 and (lower(fqan) like lower($3)))`,
		i[UnitName], i[SetName], role).Scan(&unitid, &setid, &roleCnt)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	if i[UnitName].Valid && !unitid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
	}
	if i[SetName].Valid && !setid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, SetName))
	}
	if i[Role].Valid && roleCnt == 0 {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, Role))
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	rows, err := c.DBtx.Query(`select cs.name, s.pattern, au.name, gf.fqan, u.uname, g.name
							   from capability_sets cs
	  						     join scopes s using (setid)
						   	     left join grid_fqan gf using (setid)
	                             left join affiliation_units au using (unitid)
								 left join groups g on mapped_group = g.groupid
								 left join users u on mapped_user = u.uid
	                           where (au.name = $1 or $1 is null)
	                             and (cs.name = $2 or $2 is null)
	                             and ( (lower(fqan) like lower($3)) or $3 is null)
	                           order by cs.name, au.name`, i[UnitName], i[SetName], role)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	type jsonentry map[Attribute]interface{}
	type jsonlist []interface{}

	const Patterns Attribute = "patterns"
	const Roles Attribute = "roles"
	const MappedUser Attribute = "mappeduser"
	const MappedGroup Attribute = "mappedgroup"

	entry := jsonentry{
		SetName:  "",
		Patterns: make(jsonlist, 0),
		Roles:    make(jsonlist, 0),
	}

	roleEntry := jsonentry{
		Role:        "",
		FQAN:        "",
		UnitName:    "",
		MappedUser:  "",
		MappedGroup: "",
	}

	out := make([]jsonentry, 0)

	row := NewMapNullAttribute(SetName, Pattern, UnitName, FQAN, UserName, GroupName)

	var list []string
	dejavu := NewNullAttribute(FQAN)
	dejavu2 := NewNullAttribute(GroupName)

	re := regexp.MustCompile(`Role=`)
	for rows.Next() {
		rows.Scan(row[SetName], row[Pattern], row[UnitName], row[FQAN], row[UserName], row[GroupName])

		if entry[SetName] == "" {
			entry[SetName] = row[SetName].Data
		}
		if entry[SetName] == row[SetName].Data {
			if !stringInSlice(row[Pattern].Data.(string), list) {
				entry[Patterns] = append(entry[Patterns].(jsonlist), row[Pattern].Data)
				list = append(list, row[Pattern].Data.(string))
			}
		} else {
			out = append(out, entry)
			entry = jsonentry{
				SetName:  row[SetName].Data,
				Patterns: make(jsonlist, 0),
				Roles:    make(jsonlist, 0),
			}
			list = nil
			dejavu = NewNullAttribute(FQAN)
			dejavu2 = NewNullAttribute(GroupName)
		}

		if (dejavu.Data != row[FQAN].Data) || (dejavu2.Data != row[GroupName].Data) {
			dejavu = NewNullAttribute(FQAN).Default(row[FQAN].Data)
			dejavu2 = NewNullAttribute(GroupName).Default(row[GroupName].Data)
			roleEntry = jsonentry{
				Role:        "",
				FQAN:        "",
				UnitName:    "",
				MappedUser:  "",
				MappedGroup: "",
			}
			if len(row[FQAN].Data.(string)) > 0 {
				a := re.FindStringIndex(row[FQAN].Data.(string))[1]
				b := row[FQAN].Data.(string)[a:]
				roleEntry[Role] = strings.Split(b, "/")[0]
				roleEntry[FQAN] = row[FQAN].Data
			}
			roleEntry[UnitName] = row[UnitName].Data
			if row[UserName].Valid {
				roleEntry[MappedUser] = row[UserName].Data
			}
			if row[GroupName].Valid {
				roleEntry[MappedGroup] = row[GroupName].Data
			}
			entry[Roles] = append(entry[Roles].(jsonlist), roleEntry)
		}
	}
	out = append(out, entry)

	return out, nil
}

func createCapabilitySet(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError
	var rData LDAPSetData

	setid := NewNullAttribute(SetID)

	err := c.DBtx.QueryRow(`select setid from capability_sets where name=$1`, i[SetName]).Scan(&setid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	if setid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "Capability set name already exists."))
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	// TokenSubject default = capabilitySetName@fnal.gov  in ldap: eduPersonPrincipalName
	// VaultStorageKey default = capabilitySetName  in ldap: voPersonApplicationUID
	// If either is set to "none" then nothing is entered for that attribute.

	if !i[TokenSubject].Valid {
		rData.eduPersonPrincipalName = append(rData.eduPersonPrincipalName, i[SetName].Data.(string)+"@fnal.gov")
	} else if i[TokenSubject].Data == "none" {
		rData.eduPersonPrincipalName = append(rData.eduPersonPrincipalName, "")
	} else {
		rData.eduPersonPrincipalName = append(rData.eduPersonPrincipalName, i[TokenSubject].Data.(string))
	}

	if !i[VaultStorageKey].Valid {
		rData.voPersonApplicationUID = append(rData.voPersonApplicationUID, i[SetName].Data.(string))
	} else if i[VaultStorageKey].Data == "none" {
		rData.voPersonApplicationUID = append(rData.voPersonApplicationUID, "")
	} else {
		rData.voPersonApplicationUID = append(rData.voPersonApplicationUID, i[VaultStorageKey].Data.(string))
	}

	rData.dn = fmt.Sprintf("uid=%s,%s", i[SetName].Data, ldapBaseSetDN)
	rData.objectClass = []string{"account", "eduPerson", "voPerson"}
	rData.voPersonExternalID = i[SetName].Data.(string) + "@fnal.gov"
	rData.uid = i[SetName].Data.(string)
	patterns := strings.Split(i[Pattern].Data.(string), ",")
	rData.eduPersonEntitlement = append(rData.eduPersonEntitlement, patterns...)

	con, err := LDAPgetConnection(false)
	if err != nil {
		msg := fmt.Sprintf("LDAP, connection failed: %v", err)
		log.Error(msg)
		apiErr = append(apiErr, DefaultAPIError(ErrorText, msg))
		return nil, apiErr
	}

	err = LDAPaddCapabilitySet(rData, con)
	if err != nil {
		con.Close()
		log.Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "Unable to store capability set in LDAP"))
		return nil, apiErr
	}
	con.Close()

	err = c.DBtx.QueryRow(`insert into capability_sets (name, token_subject, vault_storage_key)
							 values ($1, $2, $3) RETURNING setid`,
		i[SetName], rData.eduPersonPrincipalName[0], rData.voPersonApplicationUID[0]).Scan(&setid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	for _, pattern := range rData.eduPersonEntitlement {
		_, err = c.DBtx.Exec(`insert into scopes (setid, pattern) values ($1, $2)`, &setid, &pattern)
		if err != nil {
			log.WithFields(QueryFields(c)).Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
			return nil, apiErr
		}
	}

	return nil, apiErr
}

func setCapabilitySetAttributes(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError
	var rData LDAPSetData

	// This method does not allow the CS name to be modified.   The moment you allow that, you have to deal with all
	// the users whose LDAP records contain this set.   Better to create brand new set, replace the current one with it
	// and delete the original.

	setid := NewNullAttribute(SetID)
	tokenSubject := ""
	vaultStorageKey := ""

	if !i[TokenSubject].Valid && !i[VaultStorageKey].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "At least one attribute must be provided"))
		return nil, apiErr
	}

	err := c.DBtx.QueryRow(`select setid from capability_sets where name=$1`, i[SetName]).Scan(&setid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	if !setid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, SetName))
		return nil, apiErr
	}

	rData.dn = fmt.Sprintf("uid=%s,%s", i[SetName].Data, ldapBaseSetDN)

	if i[TokenSubject].Valid {
		rData.eduPersonPrincipalName = append(rData.eduPersonPrincipalName, i[TokenSubject].Data.(string))
		if i[TokenSubject].Data != "none" {
			tokenSubject = i[TokenSubject].Data.(string)
		}
	}

	if i[VaultStorageKey].Valid {
		rData.voPersonApplicationUID = append(rData.voPersonApplicationUID, i[VaultStorageKey].Data.(string))
		if i[VaultStorageKey].Data != "none" {
			vaultStorageKey = i[VaultStorageKey].Data.(string)
		}
	}

	con, err := LDAPgetConnection(false)
	if err != nil {
		msg := fmt.Sprintf("LDAP, connection failed: %v", err)
		log.Error(msg)
		apiErr = append(apiErr, DefaultAPIError(ErrorText, msg))
		return nil, apiErr
	}

	eData, err := LDAPgetCapabilitySetData(rData.dn, con)
	if err != nil {
		msg := fmt.Sprintf("LDAP, upable to get capability set data: %v", err)
		log.Error(msg)
		apiErr = append(apiErr, DefaultAPIError(ErrorText, msg))
		return nil, apiErr
	}

	err = LDAPmodifyCapabilitySetAttributes(rData, eData, con)
	if err != nil {
		con.Close()
		log.Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "Unable to modify capability set in LDAP"))
		return nil, apiErr
	}
	con.Close()

	_, err = c.DBtx.Exec(`update capability_sets set token_subject = $1, vault_storage_key = $2 where setid = $3`,
		tokenSubject, vaultStorageKey, setid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, apiErr
}

func dropCapabilitySet(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	setid := NewNullAttribute(SetID)
	var setidCnt int

	err := c.DBtx.QueryRow(`select (select setid from capability_sets where name=$1),
	                               (select count(setid) from grid_fqan join capability_sets using (setid) where name = $1)`, i[SetName]).Scan(&setid, &setidCnt)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	if !setid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, SetName))
		return nil, apiErr
	}
	if setidCnt > 0 {
		apiErr = append(apiErr, DefaultAPIError(ErrorText, fmt.Sprintf("Capability set is in use by %d fqan records.", setidCnt)))
		return nil, apiErr
	}
	con, err := LDAPgetConnection(false)
	if err != nil {
		msg := fmt.Sprintf("LDAP, connection failed: %v", err)
		log.Error(msg)
		apiErr = append(apiErr, DefaultAPIError(ErrorText, msg))
		return nil, apiErr
	}

	err = LDAPremoveCapabilitySet(i[SetName].Data.(string), con)
	if err != nil {
		con.Close()
		log.Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "Unable to remove capability set from LDAP"))
		return nil, apiErr
	}
	con.Close()

	_, err = c.DBtx.Exec(`delete from scopes where setid = $1`, setid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`delete from capability_sets where setid=$1`, setid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, apiErr
}

func addScopeToCapabilitySet(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	setid := NewNullAttribute(SetID)
	var patternCnt int

	err := c.DBtx.QueryRow(`select (select setid from capability_sets where name=$1),
	                               (select count(setid) from scopes join capability_sets using (setid)
								      where name=$1 and pattern=$2)`, i[SetName], i[Pattern]).Scan(&setid, &patternCnt)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	if !setid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, SetName))
		return nil, apiErr
	}
	if patternCnt > 0 {
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "pattern already exists"))
		return nil, apiErr
	}

	con, err := LDAPgetConnection(false)
	if err != nil {
		msg := fmt.Sprintf("LDAP, connection failed: %v", err)
		log.Error(msg)
		apiErr = append(apiErr, DefaultAPIError(ErrorText, msg))
		return nil, apiErr
	}

	patterns := strings.Split(i[Pattern].Data.(string), ",")

	err = LDAPaddScope(i[SetName].Data.(string), patterns, con)
	if err != nil {
		con.Close()
		log.Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "Unable to remove scope from LDAP"))
		return nil, apiErr
	}
	con.Close()

	for _, pattern := range patterns {
		_, err = c.DBtx.Exec(`insert into scopes (setid, pattern) values($1, $2)`, setid, pattern)
		if err != nil {
			log.WithFields(QueryFields(c)).Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
			return nil, apiErr
		}
	}

	return nil, apiErr
}

func removeScopeFromCapabilitySet(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	setid := NewNullAttribute(SetID)
	var patternCnt int

	err := c.DBtx.QueryRow(`select (select setid from capability_sets where name=$1),
	                               (select count(setid) from scopes join capability_sets using (setid)
								      where name=$1 and pattern=$2)`, i[SetName], i[Pattern]).Scan(&setid, &patternCnt)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	if !setid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, SetName))
		return nil, apiErr
	}
	if patternCnt == 0 {
		return nil, nil
	}

	con, err := LDAPgetConnection(false)
	if err != nil {
		msg := fmt.Sprintf("LDAP, connection failed: %v", err)
		log.Error(msg)
		apiErr = append(apiErr, DefaultAPIError(ErrorText, msg))
		return nil, apiErr
	}

	patterns := strings.Split(i[Pattern].Data.(string), ",")

	err = LDAPremoveScope(i[SetName].Data.(string), patterns, con)
	if err != nil {
		con.Close()
		log.Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "Unable to remove scope from LDAP"))
		return nil, apiErr
	}
	con.Close()

	for _, pattern := range patterns {
		_, err = c.DBtx.Exec(`delete from scopes where setid=$1 and pattern=$2`, setid, pattern)
		if err != nil {
			log.WithFields(QueryFields(c)).Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
			return nil, apiErr
		}
	}

	return nil, apiErr
}

func addCapabilitySetToFQAN(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	setid := NewNullAttribute(SetID)
	unitid := NewNullAttribute(UnitID)
	var roleCnt int
	var fqansWithSetId int

	role := "%/role=" + i[Role].Data.(string) + "/%"

	err := c.DBtx.QueryRow(`select (select setid from capability_sets where name=$1),
								   (select unitid from affiliation_units where name=$2),
								   (select count(fqan) from grid_fqan join affiliation_units using (unitid)
								    where name=$2 and (lower(fqan) like lower($3))),
								   (select count(setid) from grid_fqan join affiliation_units using (unitid)
								    where name=$2 and (lower(fqan) like lower($3))
									   and setid is not null)`,
		i[SetName], i[UnitName], role).Scan(&setid, &unitid, &roleCnt, &fqansWithSetId)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	if !unitid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
		return nil, apiErr
	}
	if !setid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, SetName))
		return nil, apiErr
	}
	if roleCnt == 0 {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, Role))
		return nil, apiErr
	}
	if fqansWithSetId != 0 {
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "FQAN already has a capability set, you must first remove it"))
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`update grid_fqan set setid = $1 where unitid=$2 and (lower(fqan) like lower($3))`,
		setid, unitid, role)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	// Update LDAP for all the users who have the FQAN for the newly added set.
	_, apiErr = updateLdapForCapabilitySet(c, i)

	return nil, apiErr
}

func removeCapabilitySetFromFQAN(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	setid := NewNullAttribute(SetID)
	unitid := NewNullAttribute(UnitID)
	var fullFQAN = NewNullAttribute(FQAN)

	role := "%/role=" + i[Role].Data.(string) + "/%"

	err := c.DBtx.QueryRow(`select (select setid from capability_sets where name=$1),
								   (select unitid from affiliation_units where name=$2),
								   (select fqan from grid_fqan join affiliation_units using (unitid)
								     where name=$2 and (lower(fqan) like lower($3)) limit 1)`,
		i[SetName], i[UnitName], role).Scan(&setid, &unitid, &fullFQAN)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	if !unitid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
		return nil, apiErr
	}
	if !setid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, SetName))
		return nil, apiErr
	}
	if !fullFQAN.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, Role))
		return nil, apiErr
	}

	// Before dropping the set from the DB we MUST remove the LDAP entry from those using it.  Hence this query and the
	// following block of code.  Do not touch the FQAN record until all user's have been updated.  Why?
	// So, this can be re-run if there are LDAP issues.
	rows, err := c.DBtx.Query(`select distinct u.vopersonid
							   from users u using
								 join grid_access ga using (uid)
								 join grid_fqan gf using (fqanid)
								 join capability_sets cs using (setid)
							   where u.vopersonid is not null
								 and gf.fqanid in (select fqanid from grid_fqan join affiliation_units using (unitid)
								                   where name=$1
												     and lower(fqan) like lower($2) )
							   order by e.value`, i[UnitName], role)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	if err == sql.ErrNoRows {
		return nil, nil
	}

	con, lErr := LDAPgetConnection(false)
	if lErr != nil {
		msg := fmt.Sprintf("LDAP, connection failed: %v", lErr)
		log.Error(msg)
		apiErr = append(apiErr, DefaultAPIError(ErrorText, msg))
		return nil, apiErr
	}

	var dn string
	var setsToDrop, setsToAdd, groupsToDrop, groupsToAdd []string
	var voPersonID string

	setsToDrop = append(setsToDrop, ldapCapabitySet+i[SetName].Data.(string))
	wgroup := getWlcgGroup(fullFQAN.Data.(string), i[UnitName].Data.(string))
	if wgroup != "" {
		groupsToDrop = append(groupsToDrop, wgroup)
	}

	for rows.Next() {
		rows.Scan(&voPersonID)

		dn = fmt.Sprintf("voPersonID=%s,%s", voPersonID, ldapBaseDN)
		log.Infof("dn: %s", dn)
		_, lErr = LDAPmodifyUserScoping(dn, setsToDrop, setsToAdd, groupsToDrop, groupsToAdd, con)
		if lErr != nil {
			con.Close()
			log.Error(lErr)
			apiErr = append(apiErr, DefaultAPIError(ErrorText, "Unable to modify eduPersonEntitlment"))
			log.Errorf("LDAPmodifyUserScoping failed on dn: %s", dn)
			return nil, apiErr
		}
	}
	con.Close()

	// Now the DB record can be updated!
	_, err = c.DBtx.Exec(`update grid_fqan set setid = null where unitid=$1 and (lower(fqan) like lower($2))`,
		unitid, role)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, apiErr
}

// Given a set of user's voPersonIDs, for each user update LDAP.
func updateLdapForUserSet(c APIContext, voPersonIDs []string, con *ldap.Conn) ([]string, []APIError) {
	var apiErr []APIError
	var updated []string
	var dn string

	// Make sure they really are in LDAP. We don't have 2 phase commit between ldap and ferry, so we must test this.
	// If they are not then add them.
	for _, voPersonID := range voPersonIDs {

		// get the capability sets for the user as FERRY has them
		// Then get the sets as LDAP has them,
		// compare the two and pass the differences to the modify method.
		rows, err := c.DBtx.Query(` select distinct cs.name, gf.fqan, au.name
									from users u
										join grid_access as ga using (uid)
										join grid_fqan as gf using(fqanid)
										join capability_sets as cs using(setid)
										join affiliation_units as au using(unitid)
									where u.vopersonid = $1
										and ga.is_banned = false
									order by cs.name`, voPersonID)
		if err != nil && err != sql.ErrNoRows {
			log.WithFields(QueryFields(c)).Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
			return updated, apiErr
		}
		var ferryCsets, ferryWgroups []string
		var setname, fqan, unitname string
		for rows.Next() {
			rows.Scan(&setname, &fqan, &unitname)
			if !stringInSlice(ldapCapabitySet+setname, ferryCsets) {
				ferryCsets = append(ferryCsets, ldapCapabitySet+setname)
			}
			wgroup := getWlcgGroup(fqan, unitname)
			if wgroup != "" && !stringInSlice(wgroup, ferryWgroups) {
				ferryWgroups = append(ferryWgroups, wgroup)
			}
		}

		lData, lErr := LDAPgetUserData(voPersonID, con)
		if lErr != nil {
			log.Error(lErr)
			apiErr = append(apiErr, DefaultAPIError(ErrorText, "Unable to get user's LDAP data."))
			return updated, apiErr
		}

		setsToDrop := arrayCompare(lData.eduPersonEntitlement, ferryCsets)
		setsToAdd := arrayCompare(ferryCsets, lData.eduPersonEntitlement)
		groupsToDrop := arrayCompare(lData.isMemberOf, ferryWgroups)
		groupsToAdd := arrayCompare(ferryWgroups, lData.isMemberOf)

		dn = fmt.Sprintf("voPersonID=%s,%s", voPersonID, ldapBaseDN)
		modified, lErr := LDAPmodifyUserScoping(dn, setsToDrop, setsToAdd, groupsToDrop, groupsToAdd, con)
		if lErr != nil {
			log.Errorf("From LDAPmodifyUserScoping - error on dn: %s  Error: %s", dn, lErr)
			apiErr = append(apiErr, DefaultAPIError(ErrorText, "Unable to modify user eduPersonEntitlments or isMemberOf"))
			return updated, apiErr
		}
		if modified {
			updated = append(updated, fmt.Sprintf("uname: %s voPersonId: %s", lData.uid, lData.voPersonID))
		}

	}

	return updated, apiErr
}

func updateLdapForAffiliation(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	unitid := NewNullAttribute(UnitID)

	err := c.DBtx.QueryRow(`select unitid from affiliation_units where name=$1`, i[UnitName]).Scan(&unitid)
	if err != nil {
		if err == sql.ErrNoRows {
			apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
		} else {
			log.WithFields(QueryFields(c)).Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		}
		return nil, apiErr
	}

	rows, err := c.DBtx.Query(`select distinct u.voPersonID
							   from affiliation_unit_group aug
							   	   join groups using (groupid)
							       join user_group ug using (groupid)
								   join users u using (uid)
							   where aug.unitid = $1
	                             and aug.is_primary = true
	                             and u.status = true
								 and u.voPersonID is not null
							   order by u.voPersonID`, unitid)
	if err != nil {
		if err == sql.ErrNoRows {
			apiErr = append(apiErr, DefaultAPIError(ErrorText, "No voPersonIDs found for affiliation."))
		} else {
			log.WithFields(QueryFields(c)).Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		}
		return nil, apiErr
	}
	defer rows.Close()

	var voPersonIDs []string
	var voPersonID string

	for rows.Next() {
		rows.Scan(&voPersonID)
		voPersonIDs = append(voPersonIDs, voPersonID)
	}

	con, err := LDAPgetConnection(false)
	if err != nil {
		msg := fmt.Sprintf("LDAP, connection failed: %v", err)
		log.Error(msg)
		apiErr = append(apiErr, DefaultAPIError(ErrorText, msg))
		return nil, apiErr
	}

	_, apiErr = updateLdapForUserSet(c, voPersonIDs, con)
	con.Close()

	return nil, apiErr
}

func updateLdapForCapabilitySet(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	setid := NewNullAttribute(SetID)

	err := c.DBtx.QueryRow(`select setid from capability_sets where name=$1`, i[SetName]).Scan(&setid)
	if err != nil {
		if err == sql.ErrNoRows {
			apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
		} else {
			log.WithFields(QueryFields(c)).Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		}
		return nil, apiErr
	}

	rows, err := c.DBtx.Query(`select distinct u.voPersonID
							   from users u using
								 join grid_access ga using (uid)
								 join grid_fqan gf using (fqanid)
								 join capability_sets cs using (setid)
							   where ga.is_banned = false
								 and u.status = true
								 and cs.setid = $1
							   order by u.voPersonID`, &setid)
	if err != nil {
		if err == sql.ErrNoRows {
			apiErr = append(apiErr, DefaultAPIError(ErrorText, "No voPersonIDs found for affiliation."))
		} else {
			log.WithFields(QueryFields(c)).Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		}
		return nil, apiErr
	}
	defer rows.Close()

	var voPersonIDs []string
	var voPersonID string

	for rows.Next() {
		rows.Scan(&voPersonID)
		voPersonIDs = append(voPersonIDs, voPersonID)
	}

	con, err := LDAPgetConnection(false)
	if err != nil {
		msg := fmt.Sprintf("LDAP, connection failed: %v", err)
		log.Error(msg)
		apiErr = append(apiErr, DefaultAPIError(ErrorText, msg))
		return nil, apiErr
	}

	_, apiErr = updateLdapForUserSet(c, voPersonIDs, con)
	con.Close()

	return nil, apiErr
}

// For modifying simple attributes, not groups, entitlements....
func modifyUserLdapAttributes(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError
	var voPersonID sql.NullString

	err := DBptr.QueryRow(`select voPersonID from users )
						   where uname = $1`, i[UserName]).Scan(&voPersonID)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	} else if err != nil {
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "The user does not exist in FERRY."))
		return nil, apiErr
	} else if len(voPersonID.String) == 0 {
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "The user in not in LDAP."))
		return nil, apiErr
	}

	m := map[string]string{}

	if i[FullName].Valid {
		m["givenName"] = i[FullName].Data.(string)
	} else {
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "an attribute to be changed must be provided"))
		return nil, apiErr
	}

	con, err := LDAPgetConnection(false)
	if err != nil {
		msg := fmt.Sprintf("LDAP, connection failed: %v", err)
		log.Error(msg)
		apiErr = append(apiErr, DefaultAPIError(ErrorText, msg))
		return nil, apiErr
	}
	dn := fmt.Sprintf("voPersonID=%s,%s", voPersonID.String, ldapBaseDN)
	err = LdapModifyAttributes(dn, m, con)
	if err != nil {
		con.Close()
		log.Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "Unable to modify provided attribute(s)"))
		log.Errorf("LdapModifyAttributes failed: %s", err)
		return nil, apiErr
	}
	con.Close()

	return nil, apiErr
}
