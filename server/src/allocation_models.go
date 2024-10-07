package main

type adjustments struct {
	CreateDate    string  `json:"createdate"`
	AdjustedHours float32 `json:"adjustedhours"`
	Comments      string  `json:"comments"`
}

type allocations struct {
	GroupName       string        `json:"groupname"`
	GID             int           `json:"gid"`
	FiscalYear      int           `json:"fiscalyear"`
	AllocationType  string        `json:"allocationtype"`
	AllocationClass string        `json:"allocationclass"`
	OriginalHours   float32       `json:"originalhours"`
	NetHours        float32       `json:"nethours"`
	UsedHours       float32       `json:"usedhours"`
	Piname          string        `json:"piname"`
	Email           NullAttribute `json:"email"`
	LastUpdated     string        `json:"lastupdated"`
	Adjustments     []adjustments `json:"adjustments"`
}
