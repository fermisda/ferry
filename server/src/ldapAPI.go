package main

// https://godoc.org/gopkg.in/ldap.v3  (documentation)
// https://github.com/go-ldap/ldap

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/go-ldap/ldap/v3"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var ldapURL string
var ldapDN string
var ldapPass string
var ldapBaseDN string

type LDAPData struct {
	dn                     string
	objectClass            []string
	voPersonID             string
	voPersonExternalID     string
	sn                     string
	cn                     string
	givenName              string
	mail                   string
	eduPersonPrincipalName string
	eduPersonEntitlement   []string
}

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

	addAffiliationUsersToLdap := BaseAPI{
		InputModel{
			Parameter{UnitName, true},
		},
		addAffiliationUsersToLdap,
		RoleWrite,
	}
	c.Add("addAffiliationUsersToLdap", &addAffiliationUsersToLdap)

}

func LDAPinitialize() error {

	ldapConfig := viper.GetStringMapString("ldap")
	ldapURL = ldapConfig["url"]
	ldapDN = ldapConfig["dn"]
	ldapPass = ldapConfig["password"]
	ldapBaseDN = ldapConfig["basedn"]
	if len(ldapURL) < 0 || len(ldapDN) < 0 || len(ldapPass) < 0 || len(ldapBaseDN) < 0 {
		err := errors.New("config - ldap is missing at least one of: url, dn, password or baseDN")
		return err
	}
	return nil
}

// Caller MUST close connection when done.
func LDAPgetConnection() (*ldap.Conn, error) {

	l, err := ldap.DialURL(ldapURL)
	if err != nil {
		return nil, err
	}
	//defer l.Close()
	err = l.Bind(ldapDN, ldapPass)
	if err != nil {
		return nil, err
	}
	return l, nil
}

func LDAPgetUserData(voPersonID string, con *ldap.Conn) (LDAPData, error) {
	var lData LDAPData
	attributes := []string{"dn", "objectClass", "voPersonID", "voPersonExternalID", "sn", "cn", "givenName", "mail", "eduPersonPrincipalName", "eduPersonEntitlement"}

	filter := fmt.Sprintf("(voPersonID=%s)", ldap.EscapeFilter(voPersonID)) // fmt.Sprintf("%v", i[UserName].Data)
	searchReq := ldap.NewSearchRequest("ou=people,o=Fermilab,o=CO,dc=cilogon,dc=org", ldap.ScopeWholeSubtree, 0, 0, 0, false, filter, attributes, []ldap.Control{})

	result, err := con.Search(searchReq)
	if err != nil {
		return lData, err
	}

	if len(result.Entries) == 1 {
		lData.dn = result.Entries[0].DN
		lData.objectClass = result.Entries[0].GetAttributeValues("objectClass")
		lData.voPersonID = result.Entries[0].GetAttributeValue("voPersonID")
		lData.voPersonExternalID = result.Entries[0].GetAttributeValue("voPersonExternalID")
		lData.sn = result.Entries[0].GetAttributeValue("sn")
		lData.cn = result.Entries[0].GetAttributeValue("cn")
		lData.givenName = result.Entries[0].GetAttributeValue("givenName")
		lData.mail = result.Entries[0].GetAttributeValue("mail")
		lData.eduPersonPrincipalName = result.Entries[0].GetAttributeValue("eduPersonPrincipalName")
		lData.eduPersonEntitlement = result.Entries[0].GetAttributeValues("eduPersonEntitlement")
	} else if len(result.Entries) > 1 {
		err := errors.New(fmt.Sprintf(" Multiple ldap entries (%d) were found for voPersonId %s", len(result.Entries), voPersonID))
		return lData, err
	}

	return lData, nil
}

func LDAPaddUser(lData LDAPData, con *ldap.Conn) error {

	givenName := []string{lData.givenName}
	sn := []string{lData.sn}
	cn := []string{lData.cn}
	mail := []string{lData.mail}
	eduPersonPrincipalName := []string{lData.eduPersonPrincipalName}
	voPersonExternalID := []string{lData.voPersonExternalID}
	voPersonID := []string{lData.voPersonID}

	addReq := ldap.NewAddRequest(lData.dn, []ldap.Control{})
	addReq.Attribute("objectClass", lData.objectClass)
	addReq.Attribute("givenName", givenName)
	addReq.Attribute("sn", sn)
	addReq.Attribute("cn", cn)
	addReq.Attribute("mail", mail)
	addReq.Attribute("eduPersonPrincipalName", eduPersonPrincipalName)
	addReq.Attribute("voPersonExternalID", voPersonExternalID)
	addReq.Attribute("voPersonID", voPersonID)
	addReq.Attribute("eduPersonEntitlement", lData.eduPersonEntitlement)
	err := con.Add(addReq)
	if err != nil {
		return err
	}

	return nil
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
					      where uid = $1 and attribute = 'voPersonId'`, uid).Scan(&lData.voPersonID)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "user is not in LDAP"))
		return nil, apiErr
	}
	if len(lData.voPersonID) == 0 {
		// FIXME:  Should also check LDAP as there is no two phase commit between LDAP and the DB
		// ferry says its already in ldap
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
		log.Error(fmt.Sprintf("ldap: getLdapData: %s", err))
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "Unable to get LDAP data."))
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

	if !uname.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
		return nil, apiErr
	}

	err = DBptr.QueryRow(`select value from external_affiliation_attribute
					      where uid = $1 and attribute = 'voPersonId'`, uid).Scan(&lData.voPersonID)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	if len(lData.voPersonID) > 0 {
		// FIXME:  Should also check LDAP as there is no two phase commit between LDAP and the DB
		// ferry says its already in ldap
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
	//FIXME: eduPersonEntitlement
	scratch := fmt.Sprintf("storage.create:/dune/scratch/users/%s", uname.Data)
	lData.eduPersonEntitlement = []string{"compute.create:/", "compute.modify:/", "compute.cancel:/", "storage.read:/dune/", "wlcg.groups:/dune/dunepro", scratch}

	err = LDAPaddUser(lData, con)
	if err != nil {
		msg := fmt.Sprintf("ldap: addLdapUser: %s", err)
		log.Error(msg)
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "Unable to store user in LDAP"))
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`insert into external_affiliation_attribute (uid, attribute, value)
							values ($1, 'voPersonId', $2)`, uid, lData.voPersonID)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, nil
}

func addAffiliationUsersToLdap(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	unitid := NewNullAttribute(UnitID)

	err := c.DBtx.QueryRow(`select unitid from affiliation_units where name=$1`, i[UnitName]).Scan(&unitid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !unitid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, i[UnitName]))
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
			log.Error("ldapAPI: addAffiliationUsersToLdap: error on uname: ", u.Data)
			return nil, apiErr
		}
	}
	con.Close()

	return nil, nil
}
