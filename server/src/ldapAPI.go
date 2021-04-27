package main

import (
	"database/sql"
	"fmt"
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

	addUserToLdap := BaseAPI{
		InputModel{
			Parameter{UserName, true},
		},
		addUserToLdap,
		RoleWrite,
	}
	c.Add("addUserToLdap", &addUserToLdap)

	addUsersToLdapByAffiliation := BaseAPI{
		InputModel{
			Parameter{UnitName, true},
		},
		addUsersToLdapByAffiliation,
		RoleWrite,
	}
	c.Add("addUsersToLdapByAffiliation", &addUsersToLdapByAffiliation)

	removeUserFromLdap := BaseAPI{
		InputModel{
			Parameter{UserName, true},
		},
		removeUserFromLdap,
		RoleWrite,
	}
	c.Add("removeUserFromLdap", &removeUserFromLdap)

	addCapabilitySet := BaseAPI{
		InputModel{
			Parameter{UnitName, true},
			Parameter{Role, true},
			Parameter{SetName, true},
			Parameter{Pattern, true},
		},
		addCapabilitySet,
		RoleWrite,
	}
	c.Add("addCapabilitySet", &addCapabilitySet)

	removeCapabilitySet := BaseAPI{
		InputModel{
			Parameter{SetName, true},
		},
		removeCapabilitySet,
		RoleWrite,
	}
	c.Add("removeCapabilitySet", &removeCapabilitySet)

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

	updateLdapForUser := BaseAPI{
		InputModel{
			Parameter{UserName, true},
		},
		updateLdapForUser,
		RoleWrite,
	}
	c.Add("updateLdapForUser", &updateLdapForUser)

}

func getUserLdapInfo(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError
	var lData LDAPData

	uid := NewNullAttribute(UID)

	err := DBptr.QueryRow(`select uid from users where uname=$1`, i[UserName]).Scan(&uid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	} else if uid.Valid == false {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
		return nil, apiErr
	}

	err = DBptr.QueryRow(`select value from external_affiliation_attribute
					      where uid = $1 and attribute = 'voPersonID'`, uid).Scan(&lData.voPersonID)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "user is not in LDAP"))
		return nil, apiErr
	}
	if len(lData.voPersonID) == 0 {
		return nil, nil
	}

	con, err := LDAPgetConnection()
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
	out["sn"] = lData.sn
	out["cn"] = lData.cn
	out["givenName"] = lData.givenName
	out["mail"] = lData.mail
	out["eduPersonPrincipalName"] = lData.eduPersonPrincipalName
	out["eduPersonEntitlement"] = lData.eduPersonEntitlement

	return out, apiErr
}

func addUserToLdap(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	con, err := LDAPgetConnection()
	if err != nil {
		msg := fmt.Sprintf("LDAP, connection failed: %v", err)
		log.Error(msg)
		apiErr = append(apiErr, DefaultAPIError(ErrorText, msg))
		return nil, apiErr
	}

	_, apiErr = addUserToLdapBase(c, i, con)
	con.Close()

	return nil, apiErr
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

	voPersonID := NewNullAttribute(UserName)

	err = c.DBtx.QueryRow(`select value from external_affiliation_attribute where uid=$1 and attribute='voPersonID'`, uid).Scan(&voPersonID)
	if err != nil {
		if err == sql.ErrNoRows {
			// Ferry says user is not in LDAP
			return nil, nil
		}
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	con, err := LDAPgetConnection()
	if err != nil {
		msg := fmt.Sprintf("LDAP, connection failed: %v", err)
		log.Error(msg)
		apiErr = append(apiErr, DefaultAPIError(ErrorText, msg))
		return nil, apiErr
	}

	lerr := LDAPremoveUser(voPersonID, con)
	if lerr != nil {
		log.Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "Unable to remove user from LDAP"))
		con.Close()
		return nil, apiErr
	}
	con.Close()

	_, err = c.DBtx.Exec(`delete from external_affiliation_attribute where attribute='voPersonID' and uid = $1`, uid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, apiErr
}

func addUserToLdapBase(c APIContext, i Input, con *ldap.Conn) (interface{}, []APIError) {
	var apiErr []APIError
	var lData LDAPData
	//FIXME? does this need to support more then just fermi?
	site := "FNAL"
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
		return nil, apiErr
	}

	err = DBptr.QueryRow(`select value from external_affiliation_attribute
					      where uid = $1 and attribute = 'voPersonID'`, uid).Scan(&lData.voPersonID)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	if len(lData.voPersonID) > 0 {
		return nil, nil
	}

	seqno := 0
	err = DBptr.QueryRow(`select nextval('ldap_vopersonid_seq')`).Scan(&seqno)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	lData.voPersonID = fmt.Sprintf("%s%09d", site, seqno)
	lData.dn = fmt.Sprintf("voPersonID=%s,%s", lData.voPersonID, ldapBaseDN)
	lData.mail = fmt.Sprintf("%s@%s", uname.Data, emailSuffix)
	lData.eduPersonPrincipalName = lData.mail
	lData.voPersonExternalID = lData.mail

	// get the capability sets for the user and create the eduPersonEntitlement array.
	rows, err := DBptr.Query(`select distinct(cs.name)
							  from users u
							    join grid_access as ga using (uid)
							    join grid_fqan as gf using(fqanid)
							    join capability_sets as cs using(setid)
							  where u.uname = $1
								and ga.is_banned = false`, uname.Data)
	// Allow user to be added even if they have no grid_fqans at this point.
	// This way, ferry-user-update can addd all new users.
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	var setname string
	for rows.Next() {
		rows.Scan(&setname)
		lData.eduPersonEntitlement = append(lData.eduPersonEntitlement, ldapCapabitySet+setname)
	}

	err = LDAPaddUser(lData, con)
	if err != nil {
		log.Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "Unable to store user in LDAP"))
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`insert into external_affiliation_attribute (uid, attribute, value)
							values ($1, 'voPersonID', $2)`, uid, lData.voPersonID)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, nil
}

func addUsersToLdapByAffiliation(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	unitid := NewNullAttribute(UnitID)

	err := c.DBtx.QueryRow(`select unitid from affiliation_units where name=$1`, i[UnitName]).Scan(&unitid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !unitid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
		return nil, apiErr
	}

	rows, err := c.DBtx.Query(`select u.uname
							   from affiliation_unit_group aug, user_group ug, users u
							   where aug.unitid = $1
	                             and aug.is_primary = true
	                             and ug.groupid = aug.groupid
	                             and u.uid = ug.uid
	                             and u.expiration_date > now()
							   order by u.uid limit 5`, unitid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	type usertype []NullAttribute
	var users usertype

	for rows.Next() {
		u := NewNullAttribute(UserName)
		rows.Scan(&u)
		users = append(users, u)
	}

	con, err := LDAPgetConnection()
	if err != nil {
		msg := fmt.Sprintf("LDAP, connection failed: %v", err)
		log.Error(msg)
		apiErr = append(apiErr, DefaultAPIError(ErrorText, msg))
		return nil, apiErr
	}

	for _, u := range users {
		input := Input{UserName: u}
		_, apiErr = addUserToLdapBase(c, input, con)
		if len(apiErr) > 0 {
			con.Close()
			log.Error("ldapAPI: addUsersToLdapByAffiliation: error on uname: ", u.Data)
			return nil, apiErr
		}
	}
	con.Close()

	return nil, nil
}

func addCapabilitySet(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError
	var rData LDAPSetData

	unitid := NewNullAttribute(UnitID)
	setid := NewNullAttribute(SetID)
	var roleCnt int
	role := "%/role=" + i[Role].Data.(string) + "/%"

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
	if !unitid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
	}
	if setid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "Capability set name already exists."))
	}
	if roleCnt == 0 {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, Role))
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	rData.dn = fmt.Sprintf("uid=%s,%s", i[SetName].Data, ldapBaseSetDN)
	rData.objectClass = []string{"account", "eduPerson", "voPerson"}
	rData.voPersonExternalID = i[SetName].Data.(string)
	rData.uid = i[SetName].Data.(string)
	patterns := strings.Split(i[Pattern].Data.(string), ",")
	for _, pattern := range patterns {
		rData.eduPersonEntitlement = append(rData.eduPersonEntitlement, pattern)
	}

	con, err := LDAPgetConnection()
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

	err = c.DBtx.QueryRow(`insert into capability_sets (name)
							 values ($1) RETURNING setid`, i[SetName]).Scan(&setid)
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

	_, err = c.DBtx.Exec(`update grid_fqan set setid = $1 where unitid=$2 and (lower(fqan) like lower($3))`,
		setid, unitid, role)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, apiErr
}

func removeCapabilitySet(c APIContext, i Input) (interface{}, []APIError) {
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

	con, err := LDAPgetConnection()
	if err != nil {
		msg := fmt.Sprintf("LDAP, connection failed: %v", err)
		log.Error(msg)
		apiErr = append(apiErr, DefaultAPIError(ErrorText, msg))
		return nil, apiErr
	}

	err = LDAPremoveCapabilitySet(i[SetName], con)
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

	con, err := LDAPgetConnection()
	if err != nil {
		msg := fmt.Sprintf("LDAP, connection failed: %v", err)
		log.Error(msg)
		apiErr = append(apiErr, DefaultAPIError(ErrorText, msg))
		return nil, apiErr
	}

	err = LDAPaddScope(i[SetName], i[Pattern], con)
	if err != nil {
		con.Close()
		log.Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "Unable to remove scope from LDAP"))
		return nil, apiErr
	}
	con.Close()

	_, err = c.DBtx.Exec(`insert into scopes (setid, pattern) values($1, $2)`, setid, i[Pattern])
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
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

	con, err := LDAPgetConnection()
	if err != nil {
		msg := fmt.Sprintf("LDAP, connection failed: %v", err)
		log.Error(msg)
		apiErr = append(apiErr, DefaultAPIError(ErrorText, msg))
		return nil, apiErr
	}

	err = LDAPremoveScope(i[SetName], i[Pattern], con)
	if err != nil {
		con.Close()
		log.Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "Unable to remove scope from LDAP"))
		return nil, apiErr
	}
	con.Close()

	_, err = c.DBtx.Exec(`delete from scopes where setid=$1 and pattern=$2`, setid, i[Pattern])
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, apiErr
}

func addCapabilitySetToFQAN(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	setid := NewNullAttribute(SetID)
	unitid := NewNullAttribute(UnitID)
	var roleCnt int

	role := "%/role=" + i[Role].Data.(string) + "/%"

	err := c.DBtx.QueryRow(`select (select setid from capability_sets where name=$1),
								   (select unitid from affiliation_units where name=$2),
								   (select count(fqan) from grid_fqan join affiliation_units using (unitid)
								     where name=$2 and (lower(fqan) like lower($3)))`,
		i[SetName], i[UnitName], role).Scan(&setid, &unitid, &roleCnt)
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

	_, err = c.DBtx.Exec(`update grid_fqan set setid = $1 where unitid=$2 and (lower(fqan) like lower($3))`,
		setid, unitid, role)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, apiErr
}

func removeCapabilitySetFromFQAN(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	setid := NewNullAttribute(SetID)
	unitid := NewNullAttribute(UnitID)
	var roleCnt int

	role := "%/role=" + i[Role].Data.(string) + "/%"

	err := c.DBtx.QueryRow(`select (select setid from capability_sets where name=$1),
								   (select unitid from affiliation_units where name=$2),
								   (select count(fqan) from grid_fqan join affiliation_units using (unitid)
								     where name=$2 and (lower(fqan) like lower($3)))`,
		i[SetName], i[UnitName], role).Scan(&setid, &unitid, &roleCnt)
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
		return nil, nil
	}

	_, err = c.DBtx.Exec(`update grid_fqan set setid = null where unitid=$1 and (lower(fqan) like lower($2))`,
		unitid, role)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, apiErr
}

func getCapabilitySet(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError
	// returns all sets with scope data for UnitName or SetName or Role or ALL
	// JSON order   Capability Set (name, patternS), UnitName, RoleS (FQAN)
	return nil, apiErr
}

func updateLdapForSet(c APIContext, voPersonIDs []string) []APIError {
	var apiErr []APIError
	var lData LDAPData
	var dn string

	con, lErr := LDAPgetConnection()
	if lErr != nil {
		msg := fmt.Sprintf("LDAP, connection failed: %v", lErr)
		log.Error(msg)
		apiErr = append(apiErr, DefaultAPIError(ErrorText, msg))
		return apiErr
	}

	for _, voPersonID := range voPersonIDs {

		// get the capability sets for the user as FERRY has them
		// Then get the sets as LDAP has them,
		// compare the two and pass the differences to the modify method.
		rows, err := c.DBtx.Query(`select distinct(cs.name)
							  from users u
							    join external_affiliation_attribute e using (uid)
							    join grid_access as ga using (uid)
							    join grid_fqan as gf using(fqanid)
							    join capability_sets as cs using(setid)
							  where e.value = $1
								and ga.is_banned = false
							  order by cs.name`, voPersonID)
		if err != nil && err != sql.ErrNoRows {
			log.WithFields(QueryFields(c)).Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
			return apiErr
		}
		var ferryCsets []string
		var setname string
		for rows.Next() {
			rows.Scan(&setname)
			ferryCsets = append(ferryCsets, ldapCapabitySet+setname)
		}

		lData, lErr = LDAPgetUserData(voPersonID, con)
		if lErr != nil {
			con.Close()
			log.Error(lErr)
			apiErr = append(apiErr, DefaultAPIError(ErrorText, "Unable to get user'sLDAP data."))
			return apiErr
		}

		setsToDrop := arrayCompare(lData.eduPersonEntitlement, ferryCsets)
		setsToAdd := arrayCompare(ferryCsets, lData.eduPersonEntitlement)

		dn = fmt.Sprintf("voPersonID=%s,%s", voPersonID, ldapBaseDN)
		lErr = LDAPmodifyEduPersonEntitlements(dn, setsToDrop, setsToAdd, con)
		if lErr != nil {
			con.Close()
			log.Error(lErr)
			apiErr = append(apiErr, DefaultAPIError(ErrorText, "Unable to modify eduPersonEntitlment"))
			return apiErr
		}

	}
	con.Close()

	return apiErr
}

func updateLdapForUser(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	uname := NewNullAttribute(UserName)
	var ustatus bool
	var voPersonID string

	err := c.DBtx.QueryRow(`select (select uname from users where uname=$1),
								   (select status from users where uname=$1),
								   (select value from external_affiliation_attribute
									  join users using (uid)
								    where uname=$1 and attribute='voPersonID')`, i[UserName]).Scan(&uname, &ustatus, &voPersonID)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	if err == sql.ErrNoRows {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, SetName))
	}
	if len(voPersonID) == 0 {
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "User is not in LDAP."))
		return nil, apiErr
	}
	if ustatus == false {
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "User is not active."))
		return nil, apiErr
	}

	voPersonIDs := make([]string, 0)
	voPersonIDs = append(voPersonIDs, voPersonID)

	apiErr = updateLdapForSet(c, voPersonIDs)

	return nil, apiErr
}
