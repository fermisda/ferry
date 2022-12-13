package main

type userAttributes struct {
	Banned         bool   `json:"banned"`
	ExpirationDate string `json:"expirationdate"`
	FullName       string `json:"fullname"`
	GroupAccount   string `json:"groupaccount"`
	Status         bool   `json:"status"`
	UID            int    `json:"uid"`
	VoPersonID     string `json:"vopersonid"`
}

type allUsersAttributes struct {
	Banned         bool   `json:"banned"`
	ExpirationDate string `json:"expirationdate"`
	FullName       string `json:"fullname"`
	Status         bool   `json:"status"`
	UID            int    `json:"uid"`
	UserName       string `json:"username"`
	VoPersonID     string `json:"vopersonid"`
}

type userCertificates struct {
	Certificates []string `json:"certificates"`
	UserName     string   `json:"username"`
}

type userFQANS struct {
	FQAN     string `json:"fqan"`
	UnitName string `json:"unitname"`
}

type userGroups struct {
	GID       int    `json:"gid"`
	GroupName string `json:"groupname"`
	GroupType string `json:"grouptype"`
}

type userShellAndHomeDir struct {
	HomeDir string `json:"homedir"`
	Shell   string `json:"shell"`
}

type userStorageQuota struct {
	Path           string `json:"path"`
	Value          int    `json:"value"`
	QuotaUnit      string `json:"quotaunit"`
	ExpirationDate string `json:"expirationdate"`
}

type userAttributeValues struct {
	Attribute string `json:"attribute"`
	Value     string `json:"value"`
}
type userExternalAttributes struct {
	UserName     string `json:"username"`
	UserAttValue []userAttributeValues
}

type userMemberAffiliations struct {
	AlternativeName string `json:"alternativename"`
	UnitName        string `json:"unitname"`
}

type userComputeResources struct {
	GroupName    string `json:"groupname"`
	HomeDir      string `json:"homedir"`
	ResourceName string `json:"resourcename"`
	ResourceType string `json:"resourcetype"`
	Shell        string `json:"shell"`
}
type userStoreQuotas struct {
	ExpirationDate string `json:"expirationdate"`
	Path           string `json:"path"`
	Quota          int    `json:"quota"`
	QuotaUnit      string `json:"quotaunit"`
}
type userStorageQuotas struct {
	Groups map[string]map[string]userStoreQuotas `json:"groups"`
	Users  map[string]map[string]userStoreQuotas `json:"users"`
}

type userAllUserFQAN struct {
	FQAN     string `json:"fqan"`
	Suspend  bool   `json:"suspend"`
	UnitName string `json:"unitname"`
}
type userAllUserFQANs struct {
	Users map[string]userAllUserFQAN `json:"users"`
}

type userUserComputeResources struct {
	GroupName string `json:"groupname"`
	Primary   bool   `json:"primary"`
	Status    bool   `json:"status"`
	Username  string `json:"username"`
}
type userGroupComputeResources struct {
	ResourceName string                              `json:"resourcename"`
	ResourceType string                              `json:"resourcetype"`
	UnitName     string                              `json:"unitname"`
	Users        map[string]userUserComputeResources `json:"users"`
}

type userGroupComputeResourcesMap []userGroupComputeResources
