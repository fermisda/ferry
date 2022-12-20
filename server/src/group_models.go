package main

type groupUnits struct {
	AlternativeName string `json:"alternativename"`
	UnitName        string `json:"unitname"`
	UnitType        string `json:"unittype"`
	VomsUrl         string `json:"vomsurl"`
}

type groupsUnitsMap []groupUnits

type groupBatchPriorities struct {
	CondorGroup    string `json:"condorgroup"`
	Value          string `json:"value"`
	ExpirationDate string `json:"expiratioindate"`
}

type groupBatchPrioritiesMap []groupBatchPriorities

type groupCondorQuotas struct {
	CondorGroup    string `json:"condorgroup"`
	ExpirationDate string `json:"expirationdate"`
	ResourcedType  string `json:"resourcetype"`
	Surplus        bool   `json:"surplus"`
	UnitName       string `json:"unitname"`
	Value          string `json:"value"`
}

type groupCondorQuotasMap []groupCondorQuotas

type groupStorageQuota struct {
	Quota          float64 `json:"quota"`
	QuotaUnit      string  `json:"quotaunit"`
	ExpirationDate string  `json:"expirationdate"`
}

type groupAllGroups struct {
	GID       int    `json:"gid"`
	GroupName string `json:"groupname"`
	GroupType string `json:"grouptype"`
}

type groupAllGroupsMap []groupAllGroups

type groupMembers struct {
	UID      int    `json:"uid"`
	UserName string `json:"username"`
}

type groupAllGroupsMembers struct {
	GID       int            `json:"gid"`
	GroupName string         `json:"groupname"`
	GroupType string         `json:"grouptype"`
	Members   []groupMembers `json:"members"`
}

type groupAllGroupsMembersMap []groupAllGroupsMembers
