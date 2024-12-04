package main

// go get github.com/go-ldap/ldap/v3
// https://godoc.org/gopkg.in/ldap.v3
// https://github.com/go-ldap/ldap

import (
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-ldap/ldap/v3"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var ldapURL string
var ldapWriteDN string
var ldapReadDN string
var ldapPass string
var ldapReadPass string
var ldapBaseDN string
var ldapBaseSetDN string
var ldapCapabitySet string
var ldapTimeout string
var requiredAccounts string

var ldapErrNoSuchObject = "LDAP Result Code 32 \"No Such Object\": "

func LDAPinitialize() error {
	var fields []string

	ldapConfig := viper.GetStringMapString("ldap")
	ldapURL = ldapConfig["url"]
	ldapWriteDN = ldapConfig["writedn"]
	ldapReadDN = ldapConfig["readdn"]
	ldapBaseDN = ldapConfig["basedn"]
	ldapBaseSetDN = ldapConfig["basesetdn"]
	ldapCapabitySet = ldapConfig["capabilityset"]
	ldapTimeout = ldapConfig["timeoutinseconds"]
	requiredAccounts = ldapConfig["requiredaccounts"]

	x := viper.Get("ldap_password")
	if x != nil {
		ldapPass = x.(string)
	} else {
		ldapPass = ldapConfig["password"]
	}
	x = viper.Get("ldap_readpassword")
	if x != nil {
		ldapReadPass = x.(string)
	} else {
		ldapReadPass = ldapConfig["readpassword"]
	}

	if len(ldapURL) == 0 {
		fields = append(fields, "url")
	}
	if len(ldapWriteDN) == 0 {
		fields = append(fields, "writedn")
	}
	if len(ldapReadDN) == 0 {
		fields = append(fields, "readdn")
	}
	if len(ldapPass) == 0 {
		fields = append(fields, "password")
	}
	if len(ldapReadPass) == 0 {
		fields = append(fields, "readpassword")
	}
	if len(ldapBaseDN) == 0 {
		fields = append(fields, "basedn")
	}
	if len(ldapBaseSetDN) == 0 {
		fields = append(fields, "basesetdn")
	}
	if len(ldapTimeout) > 0 {
		t, _ := strconv.ParseInt(ldapTimeout, 10, 0)
		ldap.DefaultTimeout = time.Duration(t) * time.Second
	}
	if len(requiredAccounts) == 0 {
		fields = append(fields, "requiredaccounts")
	}
	if len(fields) > 0 {
		err := errors.New("in the  ldap section, the config file is missing: " + strings.Join(fields, ","))
		return err
	}
	return nil
}

func ldapError(method string, ldapMethod string, e error) {
	msg := fmt.Sprintf("LDAPERROR in %s:%s --> %s", method, ldapMethod, e)
	log.Errorf(msg)
	// _ = SlackMessage(context.TODO(), msg)
}

// Caller MUST close connection when done.
// readonly=true provides a connection to a DN which allows paging but is readyonly
func LDAPgetConnection(readonly bool) (*ldap.Conn, error) {

	l, err := ldap.DialURL(ldapURL)
	if err != nil {
		ldapError("LDAPgetConnection", "DialURL", err)
		return nil, err
	}
	if readonly {
		err = l.Bind(ldapReadDN, ldapReadPass)
		if err != nil {
			ldapError("LDAPgetConnection", "Bind", err)
			return nil, err
		}
	} else {
		err = l.Bind(ldapWriteDN, ldapPass)
		if err != nil {
			ldapError("LDAPgetConnection", "Bind 2", err)
			return nil, err
		}
	}

	return l, nil
}

func LDAPgetUserData(voPersonID string, con *ldap.Conn) (LDAPUserData, error) {
	var lData LDAPUserData
	attributes := []string{"dn", "objectClass", "voPersonID", "voPersonExternalID", "uid", "sn", "cn", "givenName", "mail",
		"eduPersonPrincipalName", "eduPersonEntitlement", "isMemberOf"}

	filter := fmt.Sprintf("(voPersonID=%s)", ldap.EscapeFilter(voPersonID))
	searchReq := ldap.NewSearchRequest(ldapBaseDN, ldap.ScopeWholeSubtree, 0, 0, 0, false, filter, attributes, []ldap.Control{})

	result, err := con.Search(searchReq)
	if err != nil {
		ldapError("LDAPgetUserData", "Search", err)
		return lData, err
	}

	if len(result.Entries) == 1 {
		lData.Dn = result.Entries[0].DN
		lData.ObjectClass = result.Entries[0].GetAttributeValues("objectClass")
		lData.TokenSubject = result.Entries[0].GetAttributeValue("voPersonID")
		lData.VoPersonExternalID = result.Entries[0].GetAttributeValue("voPersonExternalID")
		lData.Uid = result.Entries[0].GetAttributeValue("uid")
		lData.Sn = result.Entries[0].GetAttributeValue("sn")
		lData.Cn = result.Entries[0].GetAttributeValue("cn")
		lData.GivenName = result.Entries[0].GetAttributeValue("givenName")
		lData.Mail = result.Entries[0].GetAttributeValue("mail")
		lData.EduPersonPrincipalName = result.Entries[0].GetAttributeValue("eduPersonPrincipalName")
		lData.EduPersonEntitlement = result.Entries[0].GetAttributeValues("eduPersonEntitlement")
		lData.IsMemberOf = result.Entries[0].GetAttributeValues("isMemberOf")
	} else if len(result.Entries) > 1 {
		err := fmt.Errorf(" Multiple ldap entries (%d) were found for voPersonID %s", len(result.Entries), voPersonID)
		return lData, err
	}

	return lData, nil
}

func LDAPgetAllVoPersonIDs(con *ldap.Conn) ([]string, error) {
	var voPersonIDs []string

	attributes := []string{"voPersonID"}
	searchReq := ldap.NewSearchRequest(ldapBaseDN, ldap.ScopeWholeSubtree, 0, 0, 0, false,
		"(&(objectClass=organizationalPerson))", attributes, []ldap.Control{ldap.NewControlPaging(1000)})
	result, err := con.SearchWithPaging(searchReq, 1000)
	if err != nil {
		ldapError("LDAPgetAllVoPersonIDs", "SearchWithPaging", err)
		return nil, err
	}

	for _, entry := range result.Entries {
		voPersonIDs = append(voPersonIDs, entry.GetAttributeValues("voPersonID")[0])
	}
	return voPersonIDs, err
}

func LDAPaddUser(lData LDAPUserData, con *ldap.Conn) error {

	givenName := []string{lData.GivenName}
	uid := []string{lData.Uid}
	sn := []string{lData.Sn}
	cn := []string{lData.Cn}
	mail := []string{lData.Mail}
	eduPersonPrincipalName := []string{lData.EduPersonPrincipalName}
	voPersonExternalID := []string{lData.VoPersonExternalID}
	voPersonID := []string{lData.TokenSubject}

	addReq := ldap.NewAddRequest(lData.Dn, []ldap.Control{})
	addReq.Attribute("objectClass", lData.ObjectClass)
	addReq.Attribute("givenName", givenName)
	addReq.Attribute("uid", uid)
	addReq.Attribute("sn", sn)
	addReq.Attribute("cn", cn)
	addReq.Attribute("mail", mail)
	addReq.Attribute("eduPersonPrincipalName", eduPersonPrincipalName)
	addReq.Attribute("voPersonExternalID", voPersonExternalID)
	addReq.Attribute("voPersonID", voPersonID)
	err := con.Add(addReq)
	if err != nil {
		ldapError("LDAPaddUser", "Add", err)
	}
	return err
}

func LDAPremoveUser(voPersonID string, con *ldap.Conn) error {

	DN := fmt.Sprintf("voPersonID=%s,%s", voPersonID, ldapBaseDN)
	delReq := ldap.NewDelRequest(DN, []ldap.Control{})
	err := con.Del(delReq)
	// If the user was not in LDAP, don't put out an error.
	if err != nil && !ldap.IsErrorWithCode(err, ldap.LDAPResultNoSuchObject) {
		ldapError("LDAPremoveUser", "Del", err)
	}

	return err
}

func LDAPgetCapabilitySetData(dn string, con *ldap.Conn) (LDAPCapabilitySetData, error) {
	var rData LDAPCapabilitySetData
	attributes := []string{"dn", "objectClass", "voPersonExternalID", "eduPersonEntitlement", "uid", "eduPersonPrincipalName", "voPersonApplicationUID"}

	searchReq := ldap.NewSearchRequest(dn, ldap.ScopeWholeSubtree, 0, 0, 0, false, "(objectclass=account)", attributes, []ldap.Control{})

	result, err := con.Search(searchReq)
	if err != nil {
		ldapError("LDAPgetCapabilitySetData", "Search", err)
		return rData, err
	}

	if len(result.Entries) == 1 {
		rData.dn = result.Entries[0].DN
		rData.objectClass = result.Entries[0].GetAttributeValues("objectClass")
		rData.voPersonExternalID = result.Entries[0].GetAttributeValue("voPersonExternalID")
		rData.eduPersonEntitlement = result.Entries[0].GetAttributeValues("eduPersonPrincipalName")
		rData.uid = result.Entries[0].GetAttributeValue("uid")
		rData.eduPersonPrincipalName = result.Entries[0].GetAttributeValues("eduPersonPrincipalName")
		rData.voPersonApplicationUID = result.Entries[0].GetAttributeValues("voPersonApplicationUID")
	} else if len(result.Entries) > 1 {
		err := fmt.Errorf(" Multiple ldap entries (%d) were found for dn %s", len(result.Entries), dn)
		return rData, err
	}

	return rData, nil
}

func LDAPaddCapabilitySet(rData LDAPCapabilitySetData, con *ldap.Conn) error {

	voPersonExternalID := []string{rData.voPersonExternalID}
	uid := []string{rData.uid}

	addReq := ldap.NewAddRequest(rData.dn, []ldap.Control{})
	addReq.Attribute("objectClass", rData.objectClass)
	addReq.Attribute("voPersonExternalID", voPersonExternalID)
	addReq.Attribute("eduPersonEntitlement", rData.eduPersonEntitlement)
	addReq.Attribute("uid", uid)
	if len(rData.eduPersonPrincipalName[0]) > 0 {
		addReq.Attribute("eduPersonPrincipalName", rData.eduPersonPrincipalName)
	}
	if len(rData.voPersonApplicationUID[0]) > 0 {
		addReq.Attribute("voPersonApplicationUID", rData.voPersonApplicationUID)
	}
	err := con.Add(addReq)
	if err != nil {
		ldapError("LDAPaddCapabilitySet", "Add", err)
	}

	return err
}

func LDAPremoveCapabilitySet(voPersonExternalID string, con *ldap.Conn) error {

	DN := fmt.Sprintf("uid=%s,%s", voPersonExternalID, ldapBaseSetDN)
	delReq := ldap.NewDelRequest(DN, []ldap.Control{})
	err := con.Del(delReq)
	if err != nil {
		ldapError("LDAPremoveCapabilitySet", "Del", err)
	}

	return err
}

func LDAPaddScope(setName string, patterns []string, con *ldap.Conn) error {

	DN := fmt.Sprintf("uid=%s,%s", setName, ldapBaseSetDN)
	modify := ldap.NewModifyRequest(DN, nil)
	modify.Add("eduPersonEntitlement", patterns)
	err := con.Modify(modify)
	if err != nil {
		ldapError("LDAPaddScope", "Modify", err)
	}

	return err

}

func LDAPremoveScope(setName string, pattern []string, con *ldap.Conn) error {

	DN := fmt.Sprintf("uid=%s,%s", setName, ldapBaseSetDN)
	modify := ldap.NewModifyRequest(DN, nil)
	modify.Delete("eduPersonEntitlement", pattern)
	err := con.Modify(modify)
	if err != nil {
		ldapError("LDAPremoveScope", "Modify", err)
	}

	return err

}

func LDAPmodifyUserScoping(dn string, setsToDrop []string, setsToAdd []string, groupsToDrop []string, groupsToAdd []string,
	con *ldap.Conn) (bool, error) {
	var err error
	var adjSetsToDrop, adjSetsToAdd, adjGroupsToDrop, adjGroupsToAdd []string
	modified := false

	// LDAP returns an error if it tries to insert a value that already exists or remove an attribute that does not exist.
	// To avoid those errors, we will check for those issues and adjust the arrays accordingly.
	// Errors, we are working to avoid: "modify/delete: eduPersonEntitlement: no such value"  AND "modify/delete: eduPersonEntitlement: no such attribute"
	re := regexp.MustCompile(`=(.*?),`) // There is probably a way to get only what is between the equals and comma but I regex and don't know it well.
	voPersonID := re.FindString(dn)
	vop := voPersonID[1:(len(voPersonID) - 1)]
	lData, err := LDAPgetUserData(vop, con)
	if err != nil {
		return modified, err
	}
	for _, pattern := range setsToDrop {
		if stringInSlice(pattern, lData.EduPersonEntitlement) {
			adjSetsToDrop = append(adjSetsToDrop, pattern)
		}
	}
	for _, pattern := range setsToAdd {
		if !stringInSlice(pattern, lData.EduPersonEntitlement) {
			adjSetsToAdd = append(adjSetsToAdd, pattern)
		}
	}
	for _, pattern := range groupsToDrop {
		if stringInSlice(pattern, lData.IsMemberOf) {
			adjGroupsToDrop = append(adjGroupsToDrop, pattern)
		}
	}
	for _, pattern := range groupsToAdd {
		if !stringInSlice(pattern, lData.IsMemberOf) {
			adjGroupsToAdd = append(adjGroupsToAdd, pattern)
		}
	}

	modify := ldap.NewModifyRequest(dn, nil)

	for _, cset := range adjSetsToDrop {
		modify.Delete("eduPersonEntitlement", []string{cset})
	}
	for _, cset := range adjSetsToAdd {
		modify.Add("eduPersonEntitlement", []string{cset})
	}
	for _, gset := range adjGroupsToDrop {
		modify.Delete("isMemberOf", []string{gset})
	}
	for _, gset := range adjGroupsToAdd {
		modify.Add("isMemberOf", []string{gset})
	}

	if (len(adjSetsToDrop) > 0) || (len(adjSetsToAdd) > 0) || (len(adjGroupsToDrop) > 0) || (len(adjGroupsToAdd) > 0) {
		err = con.Modify(modify)
		if err == nil {
			modified = true
		} else {
			ldapError("LDAPmodifyUserScoping", "Modify", err)
		}
	}
	return modified, err
}

func LdapModifyAttributes(dn string, m map[string]string, con *ldap.Conn) error {
	var err error

	modify := ldap.NewModifyRequest(dn, nil)

	for key, value := range m {
		if key == "givenName" {
			givenName := []string{value}
			modify.Replace("givenName", givenName)
			name := strings.SplitN(value, " ", 2)
			cn := []string{name[0]}
			modify.Replace("cn", cn)
			if len(name) >= 2 {
				sn := []string{name[1]}
				modify.Replace("sn", sn)
			}
		} else {
			return fmt.Errorf("attribute %s is not supported", key)
		}
	}
	err = con.Modify(modify)
	if ldap.IsErrorWithCode(err, ldap.LDAPResultNoSuchObject) {
		return nil
	} else if err != nil {
		ldapError("LdapModifyAttributes", "Modify", err)
	}
	return err
}

func LDAPmodifyCapabilitySetAttributes(rData LDAPCapabilitySetData, eData LDAPCapabilitySetData, con *ldap.Conn) error {
	var err error
	var doit = false

	modify := ldap.NewModifyRequest(eData.dn, nil)

	if len(rData.eduPersonPrincipalName) > 0 {
		doit = true
		if rData.eduPersonPrincipalName[0] == "none" {
			if len(eData.eduPersonPrincipalName) > 0 {
				modify.Delete("eduPersonPrincipalName", eData.eduPersonPrincipalName)
			}
		} else {
			if len(eData.eduPersonPrincipalName) > 0 {
				modify.Replace("eduPersonPrincipalName", rData.eduPersonPrincipalName)
			} else {
				modify.Add("eduPersonPrincipalName", rData.eduPersonPrincipalName)
			}
		}
	}

	if len(rData.voPersonApplicationUID) > 0 {
		doit = true
		if rData.voPersonApplicationUID[0] == "none" {
			if len(eData.voPersonApplicationUID) > 0 {
				modify.Delete("voPersonApplicationUID", eData.voPersonApplicationUID)
			}
		} else {
			if len(eData.voPersonApplicationUID) > 0 {
				modify.Replace("voPersonApplicationUID", rData.voPersonApplicationUID)
			} else {
				modify.Add("voPersonApplicationUID", rData.voPersonApplicationUID)
			}
		}
	}

	if doit {
		err = con.Modify(modify)
		if err != nil {
			ldapError("LDAPmodifyCapabilitySetAttributes", "Modify", err)
		}
	}
	return err

}

// Constructs a wlcggroup from the fqan and unitname.
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
func addUserToLdapBase(c APIContext, i Input, con *ldap.Conn) (LDAPUserData, []APIError) {
	var apiErr []APIError
	var lData LDAPUserData

	emailSuffix := "fnal.gov"
	uname := NewNullAttribute(UserName)
	uid := NewNullAttribute(UID)
	lData.ObjectClass = []string{"person", "organizationalPerson", "inetOrgPerson", "eduMember", "eduPerson", "voPerson"}

	err := c.DBtx.QueryRow(`select uid, uname, full_name,
								   split_part(full_name, ' ', 2),
								   split_part(full_name, ' ', 1)  from users where uname=$1`,
		i[UserName]).Scan(&uid, &uname, &lData.GivenName, &lData.Sn, &lData.Cn)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return lData, apiErr
	}

	var vop sql.NullString
	err = c.DBtx.QueryRow(`select token_subject from users where uid = $1`, uid).Scan(&vop)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return lData, apiErr
	} else if vop.Valid {
		lData.TokenSubject = vop.String
	}

	// Ensure the user really is in LDAP (we don't have 2 phase commit - so we must test), if a record is in LDAP, we are done.
	// If not, then use the voPersonID from the DB and add them.  If no voPersonID exists for the use, then create one.
	if len(lData.TokenSubject) > 0 {
		llData, err := LDAPgetUserData(lData.TokenSubject, con)
		if err != nil {
			log.Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorText, "Unable to get user's LDAP data."))
			return llData, apiErr
		}
		if llData.Dn != "" {
			// User is in both DB and LDAP, we're outta here!
			return llData, nil
		}
	}
	// Create a voPersionID iff the DB did not find one for this user.  Always reuse an existing voPersonID.
	if len(lData.TokenSubject) == 0 {
		lData.TokenSubject = uuid.New().String()
		_, err = c.DBtx.Exec(`update users set token_subject=$1 where uid=$2`, lData.TokenSubject, uid)
		if err != nil {
			log.WithFields(QueryFields(c)).Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
			return lData, apiErr
		}
	}

	lData.Dn = fmt.Sprintf("voPersonID=%s,%s", lData.TokenSubject, ldapBaseDN)
	lData.Mail = fmt.Sprintf("%s@%s", uname.Data, emailSuffix)
	lData.EduPersonPrincipalName = lData.TokenSubject
	lData.Uid = uname.Data.(string)
	lData.VoPersonExternalID = lData.Mail

	err = LDAPaddUser(lData, con)
	if err != nil {
		log.Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "Unable to store user in LDAP"))
		return lData, apiErr
	}

	log.Infof("addUserToLdapBase - added to ldap, uid: %d set token_subject to: %s", uid.Data.(int64), lData.TokenSubject)

	return lData, nil
}
