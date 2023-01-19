package main

type unitAffUnitMembers []struct {
	Status   bool   `json:"status"`
	UID      int    `json:"uid"`
	UserName string `json:"username"`
}

type unitAffMembers struct {
	UID      int    `json:"uid"`
	UserName string `json:"username"`
	UUID     string `json:"uuid"`
}
type unitAffMemberList []struct {
	UnitName string           `json:"unitname:"`
	Users    []unitAffMembers `json:"users"`
}

type unitAffGroups []struct {
	GID       int    `json:"gid"`
	GroupName string `json:"groupname"`
	GroupType string `json:"grouptype"`
	Primary   bool   `json:"primary"`
	UnitName  string `json:"unitname"`
}

type unitAffGroupLeaders []struct {
	GroupName string `json:"groupname"`
	GroupType string `json:"grouptype"`
	UID       int    `json:"uid"`
	UserName  string `json:"username"`
}

type unitAffComputeResources []struct {
	HomeDir      string `json:"homedir"`
	ResourceName string `json:"resourcename"`
	ResourceType string `json:"resourcetype"`
	UserName     string `json:"shell"`
}

type unitAffUnits []struct {
	UnitName string `json:"unitname"`
	VomsURL  string `json:"vomsurl"`
}
