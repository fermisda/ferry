package main

type miscUserPasswd struct {
	GECOS    string `json:"gecos"`
	GID      int    `json:"gid"`
	HomeDir  string `json:"homedir"`
	Shell    string `json:"shell"`
	UID      int    `json:"uid"`
	UserName string `json:"username"`
}

type miscUserPasswdGrps struct {
	Lastupdated int64                     `json:"lastupdated"`
	Resources   map[string]miscUserPasswd `json:"resources"`
}

type miscGroupFile []struct {
	GID         int      `json:"gid"`
	GroupName   string   `json:"groupname"`
	Lastupdated int64    `json:"lastupdated"`
	Users       []string `json:"users"`
}

//type miscGroupFile []miscGroup

type miscGridMapFile []struct {
	DN       string `json:"dn"`
	UserName string `json:"username"`
}

//type miscGridMapFile []miscGridMap

//type miscGridMapFileByVO map[string]miscGridMap // Uses getGridMapFile's struct
type miscGridMapFileByVO map[string]struct {
	DN       string `json:"dn"`
	UserName string `json:"username"`
}

type miscVORoleMapFile []struct {
	FQAN     string `json:"fqan"`
	UnitName string `json:"unitname"`
	UserName string `json:"username"`
}

type miscGroupGID struct {
	GID     int `json:"gid"`
	GroupId int `json:"groupid"`
}

type miscGroupName struct {
	GroupName string `json:"groupname"`
}

type miscDNowner struct {
	UID      int    `json:"uid"`
	UserName string `json:"username"`
}

type miscMappedGidFile []struct {
	FQAN        string `json:"fqan"`
	MappedGID   int    `json:"mapped_gid"`
	MappedUname string `json:"mapped_uname"`
}

type miscAffMembRoles map[string][]struct {
	FQAN     string `json:"fqan"`
	FullName string `json:"fullname"`
	UserName string `json:"username"`
}

type miscStorageResourceInfo []struct {
	Path         string `json:"path"`
	Quota        int    `json:"quota"`
	QuotaUnit    string `json:"quotaunit"`
	ResourceName string `json:"resourcename"`
	ResourceType string `json:"resourcetype"`
}

type miscComputeResources []struct {
	HomeDir      string `json:"homedir"`
	ResourceName string `json:"resourcename"`
	ResourceType string `json:"resourcetype"`
	Shell        string `json:"shell"`
	UnitName     string `json:"unitname"`
}

type miscVOUserMap map[string]map[string]struct {
}
