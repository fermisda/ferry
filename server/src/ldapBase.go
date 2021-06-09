package main

// go get github.com/go-ldap/ldap/v3
// https://godoc.org/gopkg.in/ldap.v3
// https://github.com/go-ldap/ldap

import (
	"errors"
	"fmt"
	"strings"

	"github.com/go-ldap/ldap/v3"
	"github.com/spf13/viper"
)

var ldapURL string
var ldapWriteDN string
var ldapPass string
var ldapBaseDN string
var ldapBaseSetDN string
var ldapCapabitySet string
var wlcgGroups string

// For LDAP USER data
type LDAPData struct {
	dn                     string
	objectClass            []string
	voPersonID             string
	voPersonExternalID     string
	uid                    string
	sn                     string
	cn                     string
	givenName              string
	mail                   string
	eduPersonPrincipalName string
	eduPersonEntitlement   []string
}

// For LDAP Compatability Set Data
type LDAPSetData struct {
	dn                   string
	objectClass          []string
	voPersonExternalID   string
	eduPersonEntitlement []string
	uid                  string
}

func LDAPinitialize() error {
	var fields []string

	ldapConfig := viper.GetStringMapString("ldap")
	ldapURL = ldapConfig["url"]
	ldapWriteDN = ldapConfig["writedn"]
	ldapPass = ldapConfig["password"]
	ldapBaseDN = ldapConfig["basedn"]
	ldapBaseSetDN = ldapConfig["basesetdn"]
	ldapCapabitySet = ldapConfig["capabilityset"]
	wlcgGroups = ldapConfig["wlcggroups"]

	if len(ldapURL) == 0 {
		fields = append(fields, "url")
	}
	if len(ldapWriteDN) == 0 {
		fields = append(fields, "writedn")
	}
	if len(ldapPass) == 0 {
		fields = append(fields, "password")
	}
	if len(ldapBaseDN) == 0 {
		fields = append(fields, "basedn")
	}
	if len(ldapBaseSetDN) == 0 {
		fields = append(fields, "basesetdn")
	}
	if len(wlcgGroups) == 0 {
		fields = append(fields, "wlcggroups")
	}
	if len(fields) > 0 {
		err := errors.New("in the  ldap section, the config file is missing: " + strings.Join(fields, ","))
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
	err = l.Bind(ldapWriteDN, ldapPass)
	if err != nil {
		return nil, err
	}
	return l, nil
}

func LDAPgetUserData(voPersonID string, con *ldap.Conn) (LDAPData, error) {
	var lData LDAPData
	attributes := []string{"dn", "objectClass", "voPersonID", "voPersonExternalID", "uid", "sn", "cn", "givenName", "mail", "eduPersonPrincipalName", "eduPersonEntitlement"}

	filter := fmt.Sprintf("(voPersonID=%s)", ldap.EscapeFilter(voPersonID))
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
		lData.uid = result.Entries[0].GetAttributeValue("uid")
		lData.sn = result.Entries[0].GetAttributeValue("sn")
		lData.cn = result.Entries[0].GetAttributeValue("cn")
		lData.givenName = result.Entries[0].GetAttributeValue("givenName")
		lData.mail = result.Entries[0].GetAttributeValue("mail")
		lData.eduPersonPrincipalName = result.Entries[0].GetAttributeValue("eduPersonPrincipalName")
		lData.eduPersonEntitlement = result.Entries[0].GetAttributeValues("eduPersonEntitlement")
	} else if len(result.Entries) > 1 {
		err := errors.New(fmt.Sprintf(" Multiple ldap entries (%d) were found for voPersonID %s", len(result.Entries), voPersonID))
		return lData, err
	}

	return lData, nil
}

func LDAPaddUser(lData LDAPData, con *ldap.Conn) error {

	givenName := []string{lData.givenName}
	uid := []string{lData.uid}
	sn := []string{lData.sn}
	cn := []string{lData.cn}
	mail := []string{lData.mail}
	eduPersonPrincipalName := []string{lData.eduPersonPrincipalName}
	voPersonExternalID := []string{lData.voPersonExternalID}
	voPersonID := []string{lData.voPersonID}

	addReq := ldap.NewAddRequest(lData.dn, []ldap.Control{})
	addReq.Attribute("objectClass", lData.objectClass)
	addReq.Attribute("givenName", givenName)
	addReq.Attribute("uid", uid)
	addReq.Attribute("sn", sn)
	addReq.Attribute("cn", cn)
	addReq.Attribute("mail", mail)
	addReq.Attribute("eduPersonPrincipalName", eduPersonPrincipalName)
	addReq.Attribute("voPersonExternalID", voPersonExternalID)
	addReq.Attribute("voPersonID", voPersonID)
	err := con.Add(addReq)

	return err
}

func LDAPremoveUser(voPersonID NullAttribute, con *ldap.Conn) error {

	DN := fmt.Sprintf("voPersonID=%s,%s", voPersonID.Data, ldapBaseDN)
	delReq := ldap.NewDelRequest(DN, []ldap.Control{})
	err := con.Del(delReq)

	return err
}

func LDAPaddCapabilitySet(rData LDAPSetData, con *ldap.Conn) error {

	voPersonExternalID := []string{rData.voPersonExternalID}
	uid := []string{rData.uid}

	addReq := ldap.NewAddRequest(rData.dn, []ldap.Control{})
	addReq.Attribute("objectClass", rData.objectClass)
	addReq.Attribute("voPersonExternalID", voPersonExternalID)
	addReq.Attribute("eduPersonEntitlement", rData.eduPersonEntitlement)
	addReq.Attribute("uid", uid)
	err := con.Add(addReq)

	return err
}

func LDAPremoveCapabilitySet(voPersonExternalID NullAttribute, con *ldap.Conn) error {

	DN := fmt.Sprintf("uid=%s,%s", voPersonExternalID.Data, ldapBaseSetDN)
	delReq := ldap.NewDelRequest(DN, []ldap.Control{})
	err := con.Del(delReq)

	return err
}

func LDAPaddScope(setName NullAttribute, pattern NullAttribute, con *ldap.Conn) error {

	DN := fmt.Sprintf("uid=%s,%s", setName.Data, ldapBaseSetDN)
	modify := ldap.NewModifyRequest(DN, nil)
	modify.Add("eduPersonEntitlement", []string{pattern.Data.(string)})
	err := con.Modify(modify)

	return err

}

func LDAPremoveScope(setName NullAttribute, pattern NullAttribute, con *ldap.Conn) error {

	DN := fmt.Sprintf("uid=%s,%s", setName.Data, ldapBaseSetDN)
	modify := ldap.NewModifyRequest(DN, nil)
	modify.Delete("eduPersonEntitlement", []string{pattern.Data.(string)})
	err := con.Modify(modify)

	return err

}

func LDAPmodifyEduPersonEntitlements(dn string, setsToDrop []string, setsToAdd []string, con *ldap.Conn) error {
	var err error
	var adjSetsToDrop, adjSetsToAdd []string

	// LDAP returns an error if it tries to insert a value that already exists or remove an attribute that does not exist.
	// To avoid those errors, we will check for those issues and adjust the arrays accordingly.
	// Errors, working to avoid: "modify/delete: eduPersonEntitlement: no such value"  AND "modify/delete: eduPersonEntitlement: no such attribute"
	terms := strings.Split(dn, "voPersonID=")
	vop := strings.Split(terms[0], ",")
	lData, err := LDAPgetUserData(vop[0], con)
	if err != nil {
		return err
	}
	for _, pattern := range setsToDrop {
		if stringInSlice(pattern, lData.eduPersonEntitlement) {
			adjSetsToDrop = append(adjSetsToDrop, pattern)
		}
	}
	for _, pattern := range setsToAdd {
		if !stringInSlice(pattern, lData.eduPersonEntitlement) {
			adjSetsToAdd = append(adjSetsToAdd, pattern)
		}
	}

	modify := ldap.NewModifyRequest(dn, nil)

	for _, cset := range adjSetsToDrop {
		modify.Delete("eduPersonEntitlement", []string{cset})
	}
	for _, cset := range adjSetsToAdd {
		modify.Add("eduPersonEntitlement", []string{cset})
	}

	if (len(setsToDrop) > 0) || (len(setsToAdd) > 0) {
		err = con.Modify(modify)
	}
	return err
}
