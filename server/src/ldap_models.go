package main

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
