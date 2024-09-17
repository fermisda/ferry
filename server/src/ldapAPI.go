package main

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	"github.com/go-ldap/ldap/v3"
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

// getUserLdapInfo godoc
// @Summary      Returns the user's LDAP data, directly from LDAP, not FERRY's DB.
// @Description  Returns the user's LDAP data, directly from LDAP, not FERRY's DB.
// @Tags         LDAP
// @Accept       html
// @Produce      json
// @Param        username       query     string  true  "user to return LDAP data for"
// @Success      200  {object}  LDAPUserData
// @Failure      400  {object}  main.jsonOutput
// @Failure      401  {object}  main.jsonOutput
// @Router /getUserLdapInfo [get]
func getUserLdapInfo(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError
	var lData LDAPUserData
	var vopid sql.NullString

	uid := NewNullAttribute(UID)

	err := DBptr.QueryRow(`select uid, token_subject from users where uname=$1`, i[UserName]).Scan(&uid, &vopid)
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
	lData.TokenSubject = vopid.String

	con, err := LDAPgetConnection(false)
	if err != nil {
		msg := fmt.Sprintf("LDAP, connection failed: %v", err)
		log.Error(msg)
		apiErr = append(apiErr, DefaultAPIError(ErrorText, msg))
		return nil, apiErr
	}

	lData, err = LDAPgetUserData(lData.TokenSubject, con)
	if err != nil {
		con.Close()
		log.Error(fmt.Sprintf("From LDAPgetUserData: %s", err))
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "Unable to get user's LDAP data."))
		return nil, apiErr
	}
	con.Close()

	if len(lData.TokenSubject) == 0 {
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "user is not in LDAP (2)"))
		return nil, apiErr
	}

	return lData, apiErr
}

// addOrUpdateUserInLdap godoc
// @Summary      Adds a non-existant user to LDAP and updates LDAP for an existing user.
// @Description  Adds a user to LDAP iff that user does not already exist in LDAP. For both the new user and an existing user,
// @Description  updates LDAP so the eduPersonEntitlements and isMemberOf records match FERRY's active FQANs for the user.
// @Tags         LDAP
// @Accept       html
// @Produce      json
// @Param        username       query     string  true  "user whose LDAP data is to be updated"
// @Success      200  {object}  main.jsonOutput
// @Failure      400  {object}  main.jsonOutput
// @Failure      401  {object}  main.jsonOutput
// @Router /addOrUpdateUserInLdap [put]
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
		vops := []string{lData.TokenSubject}
		_, apiErr = updateLdapForUserSet(c, vops, con)
	}
	con.Close()

	return nil, apiErr
}

// syncLdapWithFerry godoc
// @Summary      Synchronize all USER LDAP data to FERRY with FERRY as the source of truth.
// @Description  Synchronize all USER LDAP data to FERRY with FERRY as the source of truth.
// @Description  Does NOT synchronize capability sets and scopes.
// @Description  1. Removes all records in LDAP which have no corresponding record in FERRY, or are not active users in FERRY.
// @Description  2. Adds all active FERRY users to LDAP which are missing from LDAP.
// @Description  3. Verifies the capability sets in LDAP are set properly for each user, per their FQANs, correcting LDAP as needed.
// @Tags         LDAP
// @Accept       html
// @Produce      json
// @Success      200  {object}  main.jsonOutput
// @Failure      400  {object}  main.jsonOutput
// @Failure      401  {object}  main.jsonOutput
// @Router /syncLdapWithFerry [put]
func syncLdapWithFerry(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	type jsonentry map[Attribute]interface{}
	type jsonlist []interface{}

	const Removed Attribute = "removedFromLdap"
	const Added Attribute = "addedToLdap"
	const Updated Attribute = "updatedLDAPUserData"

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

	_, err = c.DBtx.Exec(`CREATE TEMPORARY TABLE temp_token_subjects (token_subject text not null);`)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	valueStrings := make([]string, 0, len(ldapUsers))
	for _, voPersonID := range ldapUsers {
		valueStrings = append(valueStrings, fmt.Sprintf("('%s')", voPersonID))
	}
	smt := fmt.Sprintf("INSERT INTO temp_token_subjects (token_subject) VALUES %s", strings.Join(valueStrings, ","))
	_, err = c.DBtx.Exec(smt)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	rows, err := c.DBtx.Query(`select token_subject from temp_token_subjects
	                           except
							   select cast(token_subject as text) from users where token_subject is not null`)
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
			apiErr = append(apiErr, DefaultAPIError(ErrorText, fmt.Sprintf("Unable to remove user from LDAP: %s - %s", llData.Mail, deleteVoPersonID)))
			con.Close()
			return entry, apiErr
		}
		entry[Removed] = append(entry[Removed].(jsonlist), fmt.Sprintf("uname: %s voPersonID: %s.", llData.Uid, deleteVoPersonID))
	}

	// Second, is to add in all the users that FERRY has registered as in LDAP but are missing.

	// Get Both those FERRY thinks are in LDAP and those that are not but should be
	rows, err = c.DBtx.Query(`select uname, token_subject
							  from users as u
							  where u.status is true
								and u.is_groupaccount is false
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
			entry[Added] = append(entry[Added].(jsonlist), fmt.Sprintf("uname: %s voPersonID: %s", u.uname, lData.TokenSubject))
			voPersonIDs = append(voPersonIDs, lData.TokenSubject)
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

// removeUserFromLdap godoc
// @Summary      Removes a user from LDAP.
// @Description  Removes a user from LDAP, providing the user has a tokensubject stored in FERRY. If not, the LDAP record will
// @Description  need to be removed with direct LDAP commands or by running syncLdapWithFerry. -- Use getUserLdapInfo to verify
// @Description  the TokenSubject exists.  Be aware, syncLdapWithFerry (runs nightly in cron) will restore the LDAP records if
// @Description  the user is active.
// @Tags         LDAP
// @Accept       html
// @Produce      json
// @Param        username       query     string  true  "user to be assigned to fqan/affiliation"
// @Success      200  {object}  main.jsonOutput
// @Failure      400  {object}  main.jsonOutput
// @Failure      401  {object}  main.jsonOutput
// @Router /removeUserFromLdap [put]
func removeUserFromLdap(c APIContext, i Input) (interface{}, []APIError) {
	//********
	// NEVER remove the TokenSubject (vopersonid) FROM the DB.  If we need to restore the user, we want the original.
	//********
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

	err = c.DBtx.QueryRow(`select token_subject from users where uid=$1 and voPersonId is not null`, uid).Scan(&voPersonID)
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
	log.Infof("removeUserFromLdap - removed from ldap, uname: %s token_subject: %s", i[UserName].Data, voPersonID)

	return nil, apiErr
}

// getCapabilitySet godoc
// @Summary      Return capability set definitions with related roles and affiliations.
// @Description  Returns the definition of or more capability sets, as it is stored in FERRY, with the associated affiliations and FQANs.
// @Tags         LDAP
// @Accept       html
// @Produce      json
// @Param        setname  query     string  false  "capability set to return"
// @Param        role     query     string  false  "role for which all capability sets are to be returned"
// @Param        unitname query     string  false  "affiliation for which all related capability sets are to be returned"
// @Success      200  {object}  ldapCapabilitySet
// @Failure      400  {object}  jsonOutput
// @Router /getCapabilitySet [get]
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
			entry[Patterns] = append(entry[Patterns].(jsonlist), row[Pattern].Data)
			list = append(list, row[Pattern].Data.(string))
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

// createCapabilitySet godoc
// @Summary      Creates a capability set in FERRY's DB and LDAP.
// @Description  Creates a capability set in FERRY's DB and LDAP.  To associate it with an FQAN see addCapabilitySetToFQAN.
// @Description  Note: if either tokensubject or vaultstorageky is none, the other must also be none.
// @Tags         LDAP
// @Accept       html
// @Produce      json
// @Param        pattern          query     string  true  "comma seperated list of scopes to include in set example: compute.create"
// @Param        setname          query     string  true  "name of the capability set"
// @Param        tokensubject     query     string  false  "default = capabilitySetName@fnal.gov set tokensubject=none to make the JWT use requester's uuid for the subject"
// @Param        vaultstoragekey  query     string  false  "default = capabilitySetName set to “none” if no ldap vaultstoragekey should be set"
// @Success      200  {object}  main.jsonOutput
// @Failure      400  {object}  main.jsonOutput
// @Failure      401  {object}  main.jsonOutput
// @Router /createCapabilitySet [post]
func createCapabilitySet(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError
	var rData LDAPCapabilitySetData

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
	//    For TokenSubject this means LDAP will use the REQUESTER's  eduPersonPrincipalName

	if !i[TokenSubject].Valid {
		rData.eduPersonPrincipalName = append(rData.eduPersonPrincipalName, i[SetName].Data.(string)+"@fnal.gov")
	} else if i[TokenSubject].Data == "none" {
		rData.eduPersonPrincipalName = append(rData.eduPersonPrincipalName, "")
	} else {
		rData.eduPersonPrincipalName = append(rData.eduPersonPrincipalName, i[TokenSubject].Data.(string))
	}

	if i[TokenSubject].Data == "none" || i[VaultStorageKey].Data == "none" {
		if i[TokenSubject].Data != i[VaultStorageKey].Data {
			apiErr = append(apiErr, DefaultAPIError(ErrorText, "Both or neither TokenSubject and VaultStorageKey must be none, only one is set"))
			return nil, apiErr
		}
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
	for _, pattern := range strings.Split(i[Pattern].Data.(string), ",") {
		// Strip spaces.  Leading spaces will cause the LDAP lib to insert odd stuff.
		rData.eduPersonEntitlement = append(rData.eduPersonEntitlement, strings.TrimSpace(pattern))
	}

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

// setCapabilitySetAttributes godoc
// @Summary      Alters attributes of a capability set in both FERRY’s DB and LDAP.
// @Description  Alters attributes of a capability set in both FERRY’s DB and LDAP.  Due to the LDAP/FERRY associations, you cannot
// @Description  change the name of a CS.  Create a new one and delete the existing one.
// @Tags         LDAP
// @Accept       html
// @Produce      json
// @Param        setname         query     string  true  "name of the set to change attributes of"
// @Param        tokensubject    query     string  true  "tokensubject to change the tokensubject too"
// @Param        vaultstoragekey query     string  true  "vaultstroagekey to change the vaultstoragekey too"
// @Success      200  {object}  main.jsonOutput
// @Failure      400  {object}  main.jsonOutput
// @Failure      401  {object}  main.jsonOutput
// @Router /setCapabilitySetAttributes [put]
func setCapabilitySetAttributes(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError
	var rData LDAPCapabilitySetData

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

// dropCapabilitySet godoc
// @Summary      Deletes a cabillity set from both FERRY’s database and LDAP.
// @Description  Deletes a cabillity set from both FERRY’s database and LDAP. FERRY will not delete a capability set that is in
// @Description  use. (See removeCapabilitySetFromFQAN.)
// @Tags         LDAP
// @Accept       html
// @Produce      json
// @Param        setname        query    string  true  "name of the capability set to delete"
// @Success      200  {object}  main.jsonOutput
// @Failure      400  {object}  main.jsonOutput
// @Failure      401  {object}  main.jsonOutput
// @Router /dropCapabilitySet [post]
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

// addScopeToCapabilitySet godoc
// @Summary      Adds a new scope to a capability set.
// @Description  Adds a new scope to a capability set.
// @Tags         LDAP
// @Accept       html
// @Produce      json
// @Param        pattern           query     string  true  "scope to add the the capability set"
// @Param        setname           query     string  true  "name of set to add the pattern"
// @Success      200  {object}  main.jsonOutput
// @Failure      400  {object}  main.jsonOutput
// @Failure      401  {object}  main.jsonOutput
// @Router /addScopeToCapabilitySet [post]
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

	var patterns []string
	for _, pattern := range strings.Split(i[Pattern].Data.(string), ",") {
		// Strip spaces.  Leading spaces will cause the LDAP lib to insert odd stuff.
		patterns = append(patterns, strings.TrimSpace(pattern))
	}

	err = LDAPaddScope(i[SetName].Data.(string), patterns, con)
	if err != nil {
		con.Close()
		log.Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "Unable to add scope to LDAP"))
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

// removeScopeFromCapabilitySet godoc
// @Summary      Removes, deletes, a scope record from its capability set both in FERRY’s DB and in LDAP.
// @Description  Removes, deletes, a scope record from its capability set both in FERRY’s DB and in LDAP.
// @Tags         LDAP
// @Accept       html
// @Produce      json
// @Param        pattern        query     string  true  "scope to remove from the capability set"
// @Param        setname        query     string  true  "name of the capability set to modify"
// @Success      200  {object}  main.jsonOutput
// @Failure      400  {object}  main.jsonOutput
// @Failure      401  {object}  main.jsonOutput
// @Router /removeScopeFromCapabilitySet [put]
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

// addCapabilitySetToFQAN godoc
// @Summary      Associates a capability set with a FQAN.
// @Description  Associates a capability set with a FQAN.  A FQAN can have one and only one associated capability sets. This method
// @Description  will override any prior setting. LDAP records for all users of the FQAN are immediately updated. That update could take a while.
// @Tags         LDAP
// @Accept       html
// @Produce      json
// @Param        role           query     string  true  "role (part of the fqan) to associate the capability set to"
// @Param        setname        query     string  true  "name of the capability set to create an assoication with"
// @Param        unitname       query     string  true  "affiliation the role belongs to"
// @Success      200  {object}  main.jsonOutput
// @Failure      400  {object}  main.jsonOutput
// @Failure      401  {object}  main.jsonOutput
// @Router /addCapabilitySetToFQAN [put]
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

// removeCapabilitySetFromFQAN godoc
// @Summary      Removes, disassociates, a capability from a FQAN.
// @Description  Removes, disassociates, a capability from a FQAN.  Immediately updates LDAP for all users of the FQAN.
// @Tags         LDAP
// @Accept       html
// @Produce      json
// @Param        role           query     string  true  "role to remove capability set from"
// @Param        setname        query     string  true   "name of the capability set to be removed"
// @Param        unitname       query     string  true  "affiliation associated with the role"
// @Success      200  {object}  main.jsonOutput
// @Failure      400  {object}  main.jsonOutput
// @Failure      401  {object}  main.jsonOutput
// @Router /removeCapabilitySetFromFQAN [put]
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
	rows, err := c.DBtx.Query(`select distinct u.token_subject
							   from users u
								 join grid_access ga using (uid)
								 join grid_fqan gf using (fqanid)
								 join capability_sets cs using (setid)
							   where u.token_subject is not null
								 and gf.fqanid in (select fqanid from grid_fqan join affiliation_units using (unitid)
								                   where name=$1
												     and lower(fqan) like lower($2) )`, i[UnitName], role)
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

// Internal method.  Given a set of user's voPersonIDs, for each user update LDAP.
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
									where u.token_subject = $1
										and ga.is_suspended = false
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

		setsToDrop := arrayCompare(lData.EduPersonEntitlement, ferryCsets)
		setsToAdd := arrayCompare(ferryCsets, lData.EduPersonEntitlement)
		groupsToDrop := arrayCompare(lData.IsMemberOf, ferryWgroups)
		groupsToAdd := arrayCompare(ferryWgroups, lData.IsMemberOf)

		dn = fmt.Sprintf("voPersonID=%s,%s", voPersonID, ldapBaseDN)
		modified, lErr := LDAPmodifyUserScoping(dn, setsToDrop, setsToAdd, groupsToDrop, groupsToAdd, con)
		if lErr != nil {
			log.Errorf("From LDAPmodifyUserScoping - error on dn: %s  Error: %s", dn, lErr)
			apiErr = append(apiErr, DefaultAPIError(ErrorText, "Unable to modify user eduPersonEntitlments or isMemberOf"))
			return updated, apiErr
		}
		if modified {
			updated = append(updated, fmt.Sprintf("uname: %s voPersonId: %s", lData.Uid, lData.TokenSubject))
		}

	}

	return updated, apiErr
}

// updateLdapForAffiliation godoc
// @Summary      Verifies and where necessary updates the LDAP records of every member in the affiliation to conform to what is in FERRY’s database.
// @Description  Verifies and where necessary updates the LDAP records of every member in the affiliation to conform to what is
// @Description  in FERRY’s database.  Be aware, affiliation simply provides the list of people.   However all their LDAP records will be
// @Description  updated, include those in other affiliations.
// @Tags         LDAP
// @Accept       html
// @Produce      json
// @Param        unitname       query     string  true  "affiliation whose member's ldap records are to be updated."
// @Success      200  {object}  main.jsonOutput
// @Failure      400  {object}  main.jsonOutput
// @Failure      401  {object}  main.jsonOutput
// @Router /updateLdapForAffiliation [put]
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

	rows, err := c.DBtx.Query(`select distinct u.token_subject
							   from affiliation_unit_group aug
							   	   join groups using (groupid)
							       join user_group ug using (groupid)
								   join users u using (uid)
							   where aug.unitid = $1
	                             and aug.is_primary = true
	                             and u.status = true
								 and u.token_subject is not null
							   order by u.token_subject`, unitid)
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

// updateLdapForCapabilitySet godoc
// @Summary      Verifies and where necessary updates the LDAP records of every user who has the FQAN associaited with the capability set.
// @Description  Verifies and where necessary updates the LDAP records of every user who has the FQAN associaited with the capability set
// @Description  to what is in in FERRY’s database.  Be aware, the setname simply provides the list of people.   However all their LDAP
// @Description  records will be updated, include those in other capability sets.
// @Tags         LDAP
// @Accept       html
// @Produce      json
// @Param        setname        query     string  true  "setname to use for obtaining the list of members to update""
// @Success      200  {object}  main.jsonOutput
// @Failure      400  {object}  main.jsonOutput
// @Failure      401  {object}  main.jsonOutput
// @Router /updateLdapForCapabilitySet [put]
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

	rows, err := c.DBtx.Query(`select distinct u.token_subject
							   from users u
								 join grid_access ga using (uid)
								 join grid_fqan gf using (fqanid)
								 join capability_sets cs using (setid)
							   where ga.is_suspended = false
								 and u.status = true
								 and cs.setid = $1
							   order by u.token_subject`, &setid)
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

// Yes, this is an API but not adding it to swagger for user documentation.
func modifyUserLdapAttributes(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError
	var voPersonID sql.NullString

	err := DBptr.QueryRow(`select token_subject from users where uname = $1`, i[UserName]).Scan(&voPersonID)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	} else if err != nil {
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "the user does not exist in FERRY"))
		return nil, apiErr
	} else if len(voPersonID.String) == 0 {
		// User not in LDAP, we are out-o-here!
		return nil, nil
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
