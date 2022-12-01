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

type allUserAttributes struct {
	Banned         bool   `json:"banned"`
	ExpirationDate string `json:"expirationdate"`
	FullName       string `json:"fullname"`
	Status         bool   `json:"status"`
	UID            int    `json:"uid"`
	UserName       string `json:"username"`
	VoPersonID     string `json:"vopersonid"`
}
