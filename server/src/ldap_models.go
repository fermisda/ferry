package main

type LDAPUserData struct {
	Dn                     string   `json:"dn"`
	ObjectClass            []string `json:"objectClass"`
	TokenSubject           string   `json:"tokensubject"`
	VoPersonExternalID     string   `json:"voPersonExternalID"`
	Uid                    string   `json:"uid"`
	Sn                     string   `json:"sn"`
	Cn                     string   `json:"cn"`
	GivenName              string   `json:"givenName"`
	Mail                   string   `json:"mail"`
	EduPersonPrincipalName string   `json:"eduPersonPrincipalName"`
	EduPersonEntitlement   []string `json:"eduPersonEntitlement"`
	IsMemberOf             []string `json:"isMemberof"`
}

type LDAPCapabilitySetData struct {
	dn                     string
	objectClass            []string
	voPersonExternalID     string
	eduPersonEntitlement   []string
	uid                    string
	eduPersonPrincipalName []string
	voPersonApplicationUID []string
}

type ldapRole struct {
	FQAN        string `json:"fqan"`
	MappedGroup string `json:"mappedgroup"`
	MappedUser  string `json:"mappeduser"`
	Role        string `json:"role"`
	UnitName    string `json:"unitname"`
}
type ldapCapabilitySet struct {
	SetName string     `json:"setname"`
	Pattern []string   `json:"patterns"`
	Role    []ldapRole `json:"roles"`
}
