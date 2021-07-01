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
			Parameter{Pattern, false},
			Parameter{Role, false},
		},
		getCapabilitySet,
		RoleWrite,
	}
	c.Add("getCapabilitySet", &getCapabilitySet)

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

	con, err := LDAPgetConnection()
	if err != nil {
		msg := fmt.Sprintf("LDAP, connection failed: %v", err)
		log.Error(msg)
		apiErr = append(apiErr, DefaultAPIError(ErrorText, msg))
		return nil, apiErr
	}

	lData, apiErr := addUserToLdapBase(c, i, con)
	if apiErr == nil {
		vops := []string{lData.voPersonID}
		apiErr = updateLdapForUserSet(c, vops)
	}
	con.Close()

	return nil, apiErr
}

// Loads all NEW users into LDAP then updates all LDAP users, both new and pre-existing, with their current
// eduPersonEntitilement data.  FERRY is the source of truth.  This method can be used to rsync all LDAP user data with FERRY.
// One thing it does NOT do, yet, is to remove users from LDAP who should no longer be there.
func syncLdapWithFerry(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	// Get Both those FERRY thinks are in LDAP and those that are
	rows, err := c.DBtx.Query(`select u.uname, e.value
							   from users u
								 left outer join external_affiliation_attribute e using (uid)
							   where u.status is true
									and u.is_groupaccount is false
									and u.is_sharedaccount is false
		 							and e.attribute = 'voPersonID'
							   order by u.uname`)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
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

	con, err := LDAPgetConnection()
	if err != nil {
		msg := fmt.Sprintf("LDAP, connection failed: %v", err)
		log.Error(msg)
		apiErr = append(apiErr, DefaultAPIError(ErrorText, msg))
		return nil, apiErr
	}

	// Add all NEW users to LDAP first.
	var voPersonIDs []string
	for _, u := range users {
		if u.voPersonID == "" {
			n := NewNullAttribute(UserName).Default(u.uname)
			input := Input{UserName: n}
			lData, apiErr := addUserToLdapBase(c, input, con)
			if len(apiErr) > 0 {
				con.Close()
				log.Errorf("ldapAPI: addUsertoLdapBase: error on uname: %s", u.uname)
				return nil, apiErr
			}
			voPersonIDs = append(voPersonIDs, lData.voPersonID)
		} else {
			voPersonIDs = append(voPersonIDs, u.voPersonID)
		}
	}
	con.Close()

	// Add or update the eduPersonEntitilments, groups... for ALL users.
	apiErr = updateLdapForUserSet(c, voPersonIDs)
	if len(apiErr) > 0 {
		log.Errorf("ldapAPI: updateLdapForUserSet: error on uname: %s", apiErr[0].Error)
		return nil, apiErr
	}

	return nil, nil
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

// Adds a user to LDAP but does NOT deal with eduPersonEntitilments or isMemberOf.  see updateLdapForUserSet for that.  Note, that
// this method ensures the user listed in the DB is in LDAP.
func addUserToLdapBase(c APIContext, i Input, con *ldap.Conn) (LDAPData, []APIError) {
	var apiErr []APIError
	var lData LDAPData

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
		return lData, apiErr
	}

	err = DBptr.QueryRow(`select value from external_affiliation_attribute
						      where uid = $1 and attribute = 'voPersonID'`, uid).Scan(&lData.voPersonID)
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
		seqno := 0
		err = DBptr.QueryRow(`select nextval('ldap_vopersonid_seq')`).Scan(&seqno)
		if err != nil {
			log.WithFields(QueryFields(c)).Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
			return lData, apiErr
		}
		lData.voPersonID = fmt.Sprintf("%s%09d", site, seqno)
	}

	lData.dn = fmt.Sprintf("voPersonID=%s,%s", lData.voPersonID, ldapBaseDN)
	lData.mail = fmt.Sprintf("%s@%s", uname.Data, emailSuffix)
	lData.eduPersonPrincipalName = lData.mail
	lData.uid = uname.Data.(string)
	lData.voPersonExternalID = lData.mail

	err = LDAPaddUser(lData, con)
	if err != nil {
		log.Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "Unable to store user in LDAP"))
		return lData, apiErr
	}

	_, err = DBptr.Exec(`insert into external_affiliation_attribute (uid, attribute, value)
								values ($1, 'voPersonID', $2)
								on conflict (uid, attribute) do nothing`, uid, lData.voPersonID)
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

func getCapabilitySet(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	unitid := NewNullAttribute(UnitID)
	setid := NewNullAttribute(SetID)
	role := NewNullAttribute(Role)

	var patternCnt int
	var roleCnt int

	if i[Role].Valid {
		role.Default("%/role=" + i[Role].Data.(string) + "/%")
	}

	err := c.DBtx.QueryRow(`select (select unitid  from affiliation_units  where name=$1),
								   (select setid from capability_sets where name=$2),
								   (select count(fqan) from grid_fqan join affiliation_units using (unitid)
								     where name=$1 and (lower(fqan) like lower($3))),
								   (select count(*) from scopes where pattern = $4 )`,
		i[UnitName], i[SetName], role, i[Pattern]).Scan(&unitid, &setid, &roleCnt, &patternCnt)
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
	if i[Pattern].Valid && patternCnt == 0 {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, Pattern))
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	rows, err := c.DBtx.Query(`select cs.name, cs.last_updated, s.pattern, s.last_updated, au.name, gf.fqan
							   from capability_sets cs
	  						     join scopes s using (setid)
						   	     join grid_fqan gf using (setid)
	                             join affiliation_units au using (unitid)
	                           where (au.unitid = $1 or $1 is null)
	                             and (cs.setid = $2 or $2 is null)
	                             and (s.pattern = $3 or $3 is null)
	                             and ( (lower(fqan) like lower($4)) or $4 is null)
	                           order by cs.name, au.name`, unitid, setid, i[Pattern], role)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	type jsonentry map[Attribute]interface{}
	type jsonlist []interface{}

	const Patterns Attribute = "patterns"
	const Units Attribute = "units"

	entry := jsonentry{
		SetName:     "",
		LastUpdated: "",
		Patterns:    make(jsonlist, 0),
		Units:       make(jsonlist, 0),
	}

	out := make([]jsonentry, 0)

	row := NewMapNullAttribute(SetName, LastUpdated)  // Set Stuff
	prow := NewMapNullAttribute(Pattern, LastUpdated) // Pattern stuff
	grow := NewMapNullAttribute(UnitName, FQAN)       // Grid stuff

	// Has this item already been saved?
	dejavu := make(map[string]string)  // Patterns
	dejavu2 := make(map[string]string) // Units

	for rows.Next() {
		rows.Scan(row[SetName], row[LastUpdated], prow[Pattern], prow[LastUpdated], grow[UnitName], grow[FQAN])

		if entry[SetName] == "" {
			entry[SetName] = row[SetName].Data
			entry[LastUpdated] = row[LastUpdated].Data
		} else if entry[SetName] != row[SetName].Data {
			newEntry := make(jsonentry)
			newEntry[SetName] = entry[SetName]
			newEntry[LastUpdated] = entry[LastUpdated]
			newEntry[Patterns] = entry[Patterns]
			newEntry[Units] = entry[Units]
			out = append(out, newEntry)
			entry[SetName] = row[SetName].Data
			entry[Patterns] = make(jsonlist, 0)
			entry[Units] = make(jsonlist, 0)
			dejavu2 = make(map[string]string)
		}
		var pkey = prow[Pattern].Data.(string)
		if _, ok := dejavu[pkey]; !ok {
			newPentry := make(jsonentry)
			newPentry[Pattern] = prow[Pattern].Data
			newPentry[LastUpdated] = prow[LastUpdated].Data
			entry[Patterns] = append(entry[Patterns].(jsonlist), newPentry)
			dejavu[pkey] = "blah"
		}

		var gkey = grow[UnitName].Data.(string)
		if _, ok := dejavu2[gkey]; !ok {
			newUentry := make(jsonentry)
			newUentry[UnitName] = grow[UnitName].Data
			newUentry[FQAN] = grow[FQAN].Data
			entry[Units] = append(entry[Units].(jsonlist), newUentry)
			dejavu2[gkey] = "blah"
		}
	}
	out = append(out, entry)

	return out, nil
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
	rData.voPersonExternalID = i[SetName].Data.(string) + "@fnal.gov"
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
	rows, err := c.DBtx.Query(`select distinct e.value
							   from external_affiliation_attribute e
							     join users u using (uid)
								 join grid_access ga using (uid)
								 join grid_fqan gf using (fqanid)
								 join capability_sets cs using (setid)
							   where e.attribute = 'voPersonID'
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

	con, lErr := LDAPgetConnection()
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
		lErr = LDAPmodifyUserScoping(dn, setsToDrop, setsToAdd, groupsToDrop, groupsToAdd, con)
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

// Given a set of users, by their voPersonID's, for each user update LDAP.  This method tests LDAP insuring each user is
// there.  It DOES add users missing from LDAP to it.
func updateLdapForUserSet(c APIContext, voPersonIDs []string) []APIError {
	var apiErr []APIError
	var dn string

	con, lErr := LDAPgetConnection()
	if lErr != nil {
		msg := fmt.Sprintf("LDAP, connection failed: %v", lErr)
		log.Error(msg)
		apiErr = append(apiErr, DefaultAPIError(ErrorText, msg))
		return apiErr
	}

	// Make sure they really are in LDAP. We don't have 2 phase commit between ldap and ferry, so we must test this.
	// If they are not then add them.
	for _, voPersonID := range voPersonIDs {
		lData, lErr := LDAPgetUserData(voPersonID, con)
		if lErr != nil {
			con.Close()
			log.Error(lErr)
			apiErr = append(apiErr, DefaultAPIError(ErrorText, "Unable to get user's LDAP data."))
			return apiErr
		}
		if len(lData.dn) == 0 {
			uname := NewNullAttribute(UserName)

			err := c.DBtx.QueryRow(`select u.uname
								   from users as u
								   	 join external_affiliation_attribute as e using (uid)
								   where e.attribute = 'voPersonID'
								     and e.value = $1`, voPersonID).Scan(&uname)
			if err != nil && err != sql.ErrNoRows {
				log.WithFields(QueryFields(c)).Error(err)
				apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
				return apiErr
			}
			if err == sql.ErrNoRows {
				log.WithFields(QueryFields(c)).Error(err)
				apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
				return apiErr
			}
			input := Input{UserName: uname}
			_, apiErr = addUserToLdapBase(c, input, con)
			if apiErr != nil {
				log.Errorf("UpdateLdapForUserSet - failed in call to addUserToLdapBase for user: %s voPersonId: %s", uname.Data.(string), voPersonID)
				return apiErr
			}
		}

		// get the capability sets for the user as FERRY has them
		// Then get the sets as LDAP has them,
		// compare the two and pass the differences to the modify method.
		rows, err := c.DBtx.Query(` select distinct cs.name, gf.fqan, au.name
									from users u
										join external_affiliation_attribute e using (uid)
										join grid_access as ga using (uid)
										join grid_fqan as gf using(fqanid)
										join capability_sets as cs using(setid)
										join affiliation_units as au using(unitid)
									where e.value = $1
										and ga.is_banned = false
									order by cs.name`, voPersonID)
		if err != nil && err != sql.ErrNoRows {
			log.WithFields(QueryFields(c)).Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
			return apiErr
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

		lData, lErr = LDAPgetUserData(voPersonID, con)
		if lErr != nil {
			con.Close()
			log.Error(lErr)
			apiErr = append(apiErr, DefaultAPIError(ErrorText, "Unable to get user'sLDAP data."))
			return apiErr
		}

		setsToDrop := arrayCompare(lData.eduPersonEntitlement, ferryCsets)
		setsToAdd := arrayCompare(ferryCsets, lData.eduPersonEntitlement)
		groupsToDrop := arrayCompare(lData.isMemberOf, ferryWgroups)
		groupsToAdd := arrayCompare(ferryWgroups, lData.isMemberOf)

		dn = fmt.Sprintf("voPersonID=%s,%s", voPersonID, ldapBaseDN)
		lErr = LDAPmodifyUserScoping(dn, setsToDrop, setsToAdd, groupsToDrop, groupsToAdd, con)
		if lErr != nil {
			con.Close()
			log.Errorf("LDAPmodifyUserScoping - error on dn: %s  Error: %s", dn, lErr)
			apiErr = append(apiErr, DefaultAPIError(ErrorText, "Unable to modify user eduPersonEntitlments or isMemberOf"))
			return apiErr
		}

	}
	con.Close()

	return apiErr
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

	rows, err := c.DBtx.Query(`select distinct e.value
							   from affiliation_unit_group aug
							   	   join groups using (groupid)
							       join user_group ug using (groupid)
								   join users u using (uid)
								   join external_affiliation_attribute e using (uid)
							   where aug.unitid = $1
	                             and aug.is_primary = true
	                             and u.status = true
								 and e.attribute = 'voPersonID'
							   order by e.value`, unitid)
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

	apiErr = updateLdapForUserSet(c, voPersonIDs)

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

	rows, err := c.DBtx.Query(`select distinct e.value
							   from external_affiliation_attribute e
							     join users u using (uid)
								 join grid_access ga using (uid)
								 join grid_fqan gf using (fqanid)
								 join capability_sets cs using (setid)
							   where ga.is_banned = false
								 and u.status = true
								 and e.attribute = 'voPersonID'
								 and cs.setid = $1 order by e.value`, &setid)
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

	apiErr = updateLdapForUserSet(c, voPersonIDs)

	return nil, apiErr
}
