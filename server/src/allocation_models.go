package main

type adjustments struct {
	CreateDate    string  `json:"createdate"`
	AdjustedHours float32 `json:"adjustedhours"`
	Comments      string  `json:"comments"`
}

type allocations struct {
	GroupName      string        `json:"groupname"`
	GID            int           `json:"gid"`
	FiscalYear     int           `json:"fiscalyear"`
	AllocationType string        `json:"allocationtype"`
	OriginalHours  float32       `json:"originalhours"`
	AdjustedHours  float32       `json:"sumhoursandadjs"`
	Adjustments    []adjustments `json:"adjustments"`
}
