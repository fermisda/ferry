package main

import (
	"errors"
	"database/sql"
	"fmt"
	log "github.com/sirupsen/logrus"
	"net/http"
	"strconv"
	"strings"
	"time"
	_ "github.com/lib/pq"
)

// IncludeUserAPIs includes all APIs described in this file in an APICollection
func IncludeUserAPIs(c *APICollection) {
	getUserInfo := BaseAPI {
		InputModel {
			Parameter{UserName, true},
		},
		getUserInfo,
	}
	c.Add("getUserInfo", &getUserInfo)

	setUserInfo := BaseAPI {
		InputModel {
			Parameter{UserName, true},
			Parameter{FullName, false},
			Parameter{Status, false},
			Parameter{GroupAccount, false},
			Parameter{ExpirationDate, false},
		},
		setUserInfo,
	}
	c.Add("setUserInfo", &setUserInfo)

	createUser := BaseAPI {
		InputModel {
			Parameter{UID, true},
			Parameter{UserName, true},
			Parameter{FullName, true},
			Parameter{Status, true},
			Parameter{GroupName, true},
			Parameter{ExpirationDate, false},
		},
		createUser,
	}
	c.Add("createUser", &createUser)

	getSuperUserList := BaseAPI {
		InputModel {
			Parameter{UnitName, true},
		},
		getSuperUserList,
	}
	c.Add("getSuperUserList", &getSuperUserList)

	addCertificateDNToUser := BaseAPI {
		InputModel {
			Parameter{UserName, true},
			Parameter{UnitName, true},
			Parameter{DN, true},
		},
		addCertificateDNToUser,
	}
	c.Add("addCertificateDNToUser", &addCertificateDNToUser)

	getUserExternalAffiliationAttributes := BaseAPI {
		InputModel {
			Parameter{UserName, false},
			Parameter{LastUpdated, false},
		},
		getUserExternalAffiliationAttributes,
	}
	c.Add("getUserExternalAffiliationAttributes", &getUserExternalAffiliationAttributes)

	setUserExternalAffiliationAttribute := BaseAPI {
		InputModel {
			Parameter{UserName, true},
			Parameter{UserAttribute, true},
			Parameter{Value, true},
		},
		setUserExternalAffiliationAttribute,
	}
	c.Add("setUserExternalAffiliationAttribute", &setUserExternalAffiliationAttribute)

	setUserStorageQuota := BaseAPI {
		InputModel {
			Parameter{UserName, false},
			Parameter{GroupName, false},
			Parameter{UnitName, true},
			Parameter{ResourceName, true},
			Parameter{Quota, true},
			Parameter{QuotaUnit, true},
			Parameter{Path, false},
			Parameter{GroupAccount, false},
			Parameter{ExpirationDate, false},
		},
		setUserStorageQuota,
	}
	c.Add("setUserStorageQuota", &setUserStorageQuota)

	getUserStorageQuota := BaseAPI {
		InputModel {
			Parameter{UserName, true},
			Parameter{UnitName, true},
			Parameter{ResourceName, true},
		},
		getUserStorageQuota,
	}
	c.Add("getUserStorageQuota", &getUserStorageQuota)

	getStorageQuotas := BaseAPI {
		InputModel {
			Parameter{UserName, false},
			Parameter{GroupName, false},
			Parameter{ResourceName, false},
			Parameter{LastUpdated, false},
		},
		getStorageQuotas,
	}
	c.Add("getStorageQuotas", &getStorageQuotas)

	getUserAccessToComputeResources := BaseAPI {
		InputModel {
			Parameter{UserName, true},
			Parameter{LastUpdated, false},
		},
		getUserAccessToComputeResources,
	}
	c.Add("getUserAccessToComputeResources", &getUserAccessToComputeResources)

	setUserAccessToComputeResource := BaseAPI {
		InputModel {
			Parameter{UserName, true},
			Parameter{GroupName, true},
			Parameter{ResourceName, true},
			Parameter{Shell, false},
			Parameter{HomeDir, false},
			Parameter{Primary, false},
		},
		setUserAccessToComputeResource,
	}
	c.Add("setUserAccessToComputeResource", &setUserAccessToComputeResource)

	setUserExperimentFQAN := BaseAPI {
		InputModel {
			Parameter{UserName, true},
			Parameter{UnitName, true},
			Parameter{FQAN, false},
			Parameter{Role, false},
		},
		setUserExperimentFQAN,
	}
	c.Add("setUserExperimentFQAN", &setUserExperimentFQAN)

	getUserFQANs := BaseAPI {
		InputModel {
			Parameter{UserName, true},
			Parameter{UnitName, false},
			Parameter{LastUpdated, false},
		},
		getUserFQANs,
	}
	c.Add("getUserFQANs", &getUserFQANs)

	getUserCertificateDNs := BaseAPI {
		InputModel {
			Parameter{UserName, false},
			Parameter{UnitName, false},
		},
		getUserCertificateDNs,
	}
	c.Add("getUserCertificateDNs", &getUserCertificateDNs)

	getAllUsersCertificateDNs := BaseAPI {
		InputModel {
			Parameter{UnitName, false},
			Parameter{Status, false},
			Parameter{LastUpdated, false},
		},
		getAllUsersCertificateDNs,
	}
	c.Add("getAllUsersCertificateDNs", &getAllUsersCertificateDNs)

	getUserGroups := BaseAPI {
		InputModel {
			Parameter{UserName, true},
			Parameter{LastUpdated, false},
		},
		getUserGroups,
	}
	c.Add("getUserGroups", &getUserGroups)

	addUserToGroup := BaseAPI {
		InputModel {
			Parameter{UserName, true},
			Parameter{GroupName, true},
			Parameter{GroupType, true},
			Parameter{Leader, false},
		},
		addUserToGroup,
	}
	c.Add("addUserToGroup", &addUserToGroup)

	setUserShellAndHomeDir := BaseAPI {
		InputModel {
			Parameter{UserName, true},
			Parameter{ResourceName, true},
			Parameter{Shell, true},
			Parameter{HomeDir, true},
		},
		setUserShellAndHomeDir,
	}
	c.Add("setUserShellAndHomeDir", &setUserShellAndHomeDir)

	getUserShellAndHomeDir := BaseAPI {
		InputModel {
			Parameter{UserName, true},
			Parameter{ResourceName, true},
			Parameter{LastUpdated, false},
		},
		getUserShellAndHomeDir,
	}
	c.Add("getUserShellAndHomeDir", &getUserShellAndHomeDir)

	setUserShell := BaseAPI {
		InputModel {
			Parameter{UserName, true},
			Parameter{UnitName, true},
			Parameter{Shell, true},
		},
		setUserShell,
	}
	c.Add("setUserShell", &setUserShell)

	removeUserFromGroup := BaseAPI {
		InputModel {
			Parameter{UserName, true},
			Parameter{GroupName, true},
			Parameter{GroupType, true},
		},
		removeUserFromGroup,
	}
	c.Add("removeUserFromGroup", &removeUserFromGroup)

	getUserUname := BaseAPI {
		InputModel {
			Parameter{UID, true},
		},
		getUserUname,
	}
	c.Add("getUserUname", &getUserUname)

	getUserUID := BaseAPI {
		InputModel {
			Parameter{UserName, true},
		},
		getUserUID,
	}
	c.Add("getUserUID", &getUserUID)

	getAllUsers := BaseAPI {
		InputModel {
			Parameter{Status, false},
			Parameter{LastUpdated, false},
		},
		getAllUsers,
	}
	c.Add("getAllUsers", &getAllUsers)

	getAllUsersFQANs := BaseAPI {
		InputModel {
			Parameter{LastUpdated, false},
		},
		getAllUsersFQANs,
	}
	c.Add("getAllUsersFQANs", &getAllUsersFQANs)

	getMemberAffiliations := BaseAPI {
		InputModel {
			Parameter{UserName, true},
			Parameter{Experiment, false},
			Parameter{LastUpdated, false},
		},
		getMemberAffiliations,
	}
	c.Add("getMemberAffiliations", &getMemberAffiliations)
}

func getUserCertificateDNs(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	uid := NewNullAttribute(UID)
	unitid := NewNullAttribute(UnitID)
	queryerr := c.DBtx.QueryRow(`select (select uid from users where uname=$1),
									    (select unitid from affiliation_units where name=$2)`,
							  i[UserName], i[UnitName]).Scan(&uid, &unitid)
	if queryerr != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(queryerr)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !uid.Valid && i[UserName].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
	}
	if !unitid.Valid && i[UnitName].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	rows, queryerr := c.DBtx.Query(`select distinct uname, dn from
							   user_certificates as uc
							   join users as u using(uid)
							   join affiliation_unit_user_certificate ac using(dnid)
							   where uid = coalesce($1, uid) and unitid = coalesce($2, unitid)
							   order by uname`,
							uid, unitid)
	if queryerr != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(queryerr)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	const Certificates Attribute = "certificates"

	type jsonentry map[Attribute]interface{}
	type jsoncerts []interface{}

	entry := jsonentry{
		UserName: 		"",
		Certificates:	make(jsoncerts, 0),
	}
	var out jsoncerts

	row := NewMapNullAttribute(UserName, DN)
	for rows.Next() {
		rows.Scan(row[UserName], row[DN])
		if row[DN].Valid {
			if entry[UserName] == "" {
				entry[UserName] = row[UserName].Data
			}
			if row[UserName].Data != entry[UserName] {
				newEntry := make(jsonentry)
				newEntry[UserName] = entry[UserName]
				newEntry[Certificates] = entry[Certificates]
				out = append(out, newEntry)
				entry[UserName] = row[UserName].Data
				entry[Certificates] = make(jsoncerts, 0)
			}
			entry[Certificates] = append(entry[Certificates].(jsoncerts), row[DN].Data)
		}
	}
	out = append(out, entry)

	return out, nil
}

func getAllUsersCertificateDNs(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	unitid := NewNullAttribute(UnitID)

	queryerr := c.DBtx.QueryRow(`select unitid from affiliation_units where name=$1`,
								i[UnitName]).Scan(&unitid)
	if queryerr != nil && queryerr != sql.ErrNoRows {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(queryerr)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !unitid.Valid && i[UnitName].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
		return nil, apiErr
	}

	activeOnly := i[Status].Default(false)
	
	rows, queryerr := c.DBtx.Query(`select uname, name, dn from
										affiliation_unit_user_certificate as ac
										join user_certificates using(dnid)
										join users using(uid)
										join affiliation_units using(unitid)
										where unitid = coalesce($1, unitid) and (status = $2 or not $2) and (ac.last_updated >= $3 or $3 is null)
									order by uname`, unitid, activeOnly, i[LastUpdated])
	if queryerr != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(queryerr)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	const Certificates Attribute = "certificates"

	type jsoncert map[Attribute]interface{}
	type jsonuser map[Attribute]interface{}
	var out []jsonuser

	prevUname := NewNullAttribute(UserName)
	for rows.Next() {
		row := NewMapNullAttribute(UserName, UnitName, DN)
		rows.Scan(row[UserName], row[UnitName], row[DN])
		if row[UserName].Valid {
			if prevUname != *row[UserName] {
				user := make(jsonuser)
				user[UserName] = row[UserName].Data
				user[Certificates] = make([]jsoncert, 0)
				out = append(out, user)
				prevUname = *row[UserName]
			}
			cert := make(jsoncert)
			cert[UnitName] = row[UnitName].Data
			cert[DN] = row[DN].Data
			out[len(out)-1][Certificates] = append(out[len(out)-1][Certificates].([]jsoncert), cert)
		}
	}

	return out, nil
}

func getUserFQANs(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	uid 	:= NewNullAttribute(UID)
	unitid	:= NewNullAttribute(UnitID)

	err := c.DBtx.QueryRow(`select (select uid from users where uname = $1),
								   (select unitid from affiliation_units where name=$2)`,
						   i[UserName], i[UnitName]).Scan(&uid, &unitid)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !uid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
	}
	if !unitid.Valid && i[UnitName].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	rows, err := c.DBtx.Query(`select name, fqan from
								grid_access as ga
							  	join grid_fqan using(fqanid)
							  	left join affiliation_units using(unitid)
							   where
								uid = $1 and
								(unitid = $2 or $2 is null) and
							  	(ga.last_updated >= $3 or $3 is null)
							   order by name;`, uid, unitid, i[LastUpdated])
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	type jsonfqan map[Attribute]interface{}
	out := make([]jsonfqan, 0)

	for rows.Next() {
		row := NewMapNullAttribute(UnitName, FQAN)
		rows.Scan(row[UnitName], row[FQAN])
		if row[FQAN].Valid {
			out = append(out, jsonfqan{UnitName: row[UnitName].Data, FQAN: row[FQAN].Data})
		}
	}

	return out, nil
}

func getSuperUserList(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	unitID := NewNullAttribute(UnitID)
	err := c.DBtx.QueryRow(`select unitid from affiliation_units where name = $1`, i[UnitName]).Scan(&unitID)
	if err == sql.ErrNoRows {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
		return nil, apiErr
	} else if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	rows, err := c.DBtx.Query(`select distinct u.uname from
								users as u
								join grid_access as ga on u.uid=ga.uid
								join grid_fqan as gf on ga.fqanid = gf.fqanid
							   where ga.is_superuser=true and gf.unitid=$1
							   order by u.uname`, unitID)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	var out []interface{}

	for rows.Next() {
		row := NewNullAttribute(UserName)
		rows.Scan(&row)
		out = append(out, row.Data)
	}

	return out, nil
}

func setSuperUser(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

	//call authorize function
	authorized, authout := authorize(r)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"ferry_error\": \""+authout+"not authorized.\" }")
		return
	}

	q := r.URL.Query()
	uName := strings.TrimSpace(q.Get("username"))
	unitName := strings.TrimSpace(q.Get("unitname"))
	if uName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No user name specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No username specified.\" }")
		return
	}
	if unitName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No unit name specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No unitname specified.\" }")
		return
	}

	var uid,unitid sql.NullInt64
	var member bool
 
	DBtx, cKey, err := LoadTransaction(r, DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"ferry_error\": \"Unable to start database transaction.\" }")
		return
	}
	defer DBtx.Rollback(cKey)

	queryerr := DBtx.tx.QueryRow(`select (select uid from users where uname=$1),
										 (select unitid from affiliation_units au where au.name=$2),
										 (select ($1, $2) in (select distinct uname, name from users as u
																join grid_access as ga on u.uid = ga.uid
																join grid_fqan as gf on ga.fqanid = gf.fqanid
																join affiliation_units as au on gf.unitid = au.unitid) as member
															 )`, uName, unitName).Scan(&uid, &unitid, &member)
	
	if queryerr == sql.ErrNoRows {
		log.WithFields(QueryFields(r, startTime)).Error("User and unit names do not exist.")
		if cKey != 0 {
			fmt.Fprintf(w, "{ \"ferry_error\": \"User and unit names do not exist.\" }")
		} else {
			DBtx.Report("User and unit names do not exist.")
		}
		return
	} else if queryerr != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error: " + queryerr.Error())
		if cKey !=0 {
			fmt.Fprintf(w, "{ \"ferry_error\": \"Something went wrong.\" }")
		} 
		return
	}
	if ! uid.Valid {
		log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
		if cKey != 0 {
			fmt.Fprintf(w, "{ \"ferry_error\": \"User does not exist.\" }")
		} else {
			DBtx.Report("User does not exist.")	
		}
		return
	} 
	if ! unitid.Valid {
		log.WithFields(QueryFields(r, startTime)).Error("Unit does not exist.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"Unit does not exist.\" }")
		return
	}
	if ! member {
		log.WithFields(QueryFields(r, startTime)).Error("User does not belong to unit.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"User does not belong to unit.\" }")
		return
	}

	_, err = DBtx.Exec(`update grid_access set is_superuser = true, last_updated = NOW() 
						where uid = $1 and fqanid in (select fqanid from grid_fqan  
						where unitid = $2)`, uid.Int64, unitid.Int64)

	if err == nil {
		if cKey != 0 { DBtx.Commit(cKey) }
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		fmt.Fprintf(w, "{ \"ferry_status\": \"success\" }")
	} else {
		log.WithFields(QueryFields(r, startTime)).Error("Error: " + err.Error())
		fmt.Fprintf(w, "{ \"ferry_error\": \"Something went wrong.\" }")
	}
}

func removeSuperUser(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

	//call authorize function
	authorized, authout := authorize(r)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"ferry_error\": \""+authout+"not authorized.\" }")
		return
	}

	q := r.URL.Query()
	uName := strings.TrimSpace(q.Get("username"))
	unitName := strings.TrimSpace(q.Get("unitname"))
	if uName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No user name specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No username specified.\" }")
		return
	}
	if unitName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No unit name specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No unitname specified.\" }")
		return
	}

	var uid,unitid sql.NullInt64
	var member bool
 
	DBtx, cKey, err := LoadTransaction(r, DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"ferry_error\": \"Unable to start database transaction.\" }")
		return
	}
	defer DBtx.Rollback(cKey)

	queryerr := DBtx.tx.QueryRow(`select (select uid from users where uname=$1),
										 (select unitid from affiliation_units au where au.name=$2),
										 (select ($1, $2) in (select distinct uname, name from users as u
																join grid_access as ga on u.uid = ga.uid
																join grid_fqan as gf on ga.fqanid = gf.fqanid
																join affiliation_units as au on gf.unitid = au.unitid) as member
															 )`, uName, unitName).Scan(&uid, &unitid, &member)
	
	if queryerr == sql.ErrNoRows {
		log.WithFields(QueryFields(r, startTime)).Error("User and unit names do not exist.")
		if cKey != 0 {
			fmt.Fprintf(w, "{ \"ferry_error\": \"User and unit names do not exist.\" }")
		} else {
			DBtx.Report("User and unit names do not exist.")
		}
		return
	} else if queryerr != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error: " + queryerr.Error())
		if cKey !=0 {
			fmt.Fprintf(w, "{ \"ferry_error\": \"Something went wrong.\" }")
		} 
		return
	}
	if ! uid.Valid {
		log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
		if cKey != 0 {
			fmt.Fprintf(w, "{ \"ferry_error\": \"User does not exist.\" }")
		} else {
			DBtx.Report("User does not exist.")	
		}
		return
	} 
	if ! unitid.Valid {
		log.WithFields(QueryFields(r, startTime)).Error("Unit does not exist.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"Unit does not exist.\" }")
		return
	}
	if ! member {
		log.WithFields(QueryFields(r, startTime)).Error("User does not belong to unit.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"User does not belong to unit.\" }")
		return
	}

	_, err = DBtx.Exec(`update grid_access set is_superuser = false, last_updated = NOW() 
						where uid = $1 and fqanid in (select fqanid from grid_fqan  
						where unitid = $2)`, uid.Int64, unitid.Int64)

	if err == nil {
		if cKey != 0 { DBtx.Commit(cKey) }
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		fmt.Fprintf(w, "{ \"ferry_status\": \"success\" }")
	} else {
		log.WithFields(QueryFields(r, startTime)).Error("Error: " + err.Error())
		fmt.Fprintf(w, "{ \"ferry_error\": \"Something went wrong.\" }")
	}
}

func getUserGroups(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	uid := NewNullAttribute(UID)
	err := c.DBtx.QueryRow(`select uid from users where uname = $1`, i[UserName]).Scan(&uid)
	if err == sql.ErrNoRows {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
		return nil, apiErr
	}
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	rows, err := c.DBtx.Query(`select gid, name, type from
									groups join
									user_group using(groupid)
							   where uid = $1 and (user_group.last_updated >= $2 or $2 is null)`,
							  uid, i[LastUpdated])
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	type jsongroup map[Attribute]interface{}
	out := make([]jsongroup, 0)

	for rows.Next() {
		row := NewMapNullAttribute(GID, GroupName, GroupType)
		rows.Scan(row[GID], row[GroupName], row[GroupType])
		out = append(out, jsongroup{
			GID: 		row[GID].Data,
			GroupName:	row[GroupName].Data,
			GroupType:	row[GroupType].Data,
		})
	}
	
	return out, nil
}

func getUserInfo(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	rows, err := c.DBtx.Query(`select full_name, uid, status, is_groupaccount, expiration_date from users where uname=$1`, i[UserName])
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	out := make(map[Attribute]interface{})
	row := NewMapNullAttribute(FullName, UID, Status, GroupAccount, ExpirationDate)
	
	for rows.Next() {
		rows.Scan(row[FullName], row[UID], row[Status], row[GroupAccount], row[ExpirationDate])
		for _, column := range row {
			out[column.Attribute] = column.Data
		}
	}
	if len(out) == 0 {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
		return nil, apiErr
	} else {
		return out, nil
	}
}

func addUserToGroup(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	uid := NewNullAttribute(UID)
	groupid := NewNullAttribute(GroupID)
	leader := i[Leader].Default(false)

	var validType bool

	err := c.DBtx.QueryRow(`select $1 = any (enum_range(null::groups_group_type)::text[])`, i[GroupType]).Scan(&validType)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !validType {
		apiErr = append(apiErr, DefaultAPIError(ErrorInvalidData, GroupType))
		return nil, apiErr
	}

	err = c.DBtx.QueryRow(`select (select uid from users where uname = $1),
								  (select groupid from groups where name = $2 and type = $3)`,
						  i[UserName], i[GroupName], i[GroupType]).Scan(&uid, &groupid)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !uid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
	}
	if !groupid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, GroupName))
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`insert into user_group (uid, groupid, is_leader, last_updated) values ($1, $2, $3, NOW())
						  on conflict (uid, groupid) do
						  update set is_leader = $3, last_updated = NOW() where $4`,
						 uid, groupid, leader, i[Leader].Valid)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, nil
}

func removeUserFromGroup(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	uid := NewNullAttribute(UID)
	groupid := NewNullAttribute(GroupID)

	var validType bool

	err := c.DBtx.QueryRow(`select $1 = any (enum_range(null::groups_group_type)::text[])`, i[GroupType]).Scan(&validType)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !validType {
		apiErr = append(apiErr, DefaultAPIError(ErrorInvalidData, GroupType))
		return nil, apiErr
	}

	err = c.DBtx.QueryRow(`select (select uid from users where uname = $1),
								  (select groupid from groups where name = $2 and type = $3)`,
						  i[UserName], i[GroupName], i[GroupType]).Scan(&uid, &groupid)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !uid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
	}
	if !groupid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, GroupName))
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`delete from user_group where uid = $1 and groupid = $2`, uid, groupid)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, nil
}

func setUserExperimentFQAN(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	fqan := NewNullAttribute(FQAN)
	if i[FQAN].Valid {
		fqan = i[FQAN]
	} else {
		if i[Role].Valid {
			fqan.Scan("%Role=" + strings.TrimSpace(i[Role].Data.(string)) + "%")
		} else if i[Role].AbsoluteNull {
			fqan.Scan("%Role=NULL%")
		} else {
			apiErr = append(apiErr, APIError{errors.New("no role or fqan specified"), ErrorAPIRequirement})
			return nil, apiErr
		}
	}

	uid := NewNullAttribute(UID)
	unitid := NewNullAttribute(UnitID)
	queryerr := c.DBtx.QueryRow(`select (select uid from users where uname=$1),
									    (select unitid from affiliation_units where name=$2)`,
							  i[UserName], i[UnitName]).Scan(&uid, &unitid)
	if queryerr != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(queryerr)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !uid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
	}
	if !unitid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	var hasCert bool
	queryerr = c.DBtx.QueryRow(`select count(*) > 0 from affiliation_unit_user_certificate as ac
							  join user_certificates as uc on ac.dnid = uc.dnid
							  where uid = $1 and unitid = $2`, uid, unitid).Scan(&hasCert)
	if queryerr != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(queryerr)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !hasCert {
		apiErr = append(apiErr, APIError{errors.New("the user is not a member of the affiliation unit"), ErrorAPIRequirement})
		return nil, apiErr
	}

	rows, queryerr := c.DBtx.Query(`select fqanid from grid_fqan where unitid = $1 and fqan like $2`, unitid, fqan)
	if queryerr != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(queryerr)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	var fqanids []int
	for rows.Next() {
		var fqanid int
		rows.Scan(&fqanid)
		fqanids = append(fqanids, fqanid)
	}
	rows.Close()
	if len(fqanids) == 0 {
		apiErr = append(apiErr, APIError{errors.New("no FQANs found for this query"), ErrorAPIRequirement})
		return nil, apiErr
	}

	for _, fqanid := range fqanids {
		_, queryerr = c.DBtx.Exec(`insert into grid_access (uid, fqanid, is_superuser, is_banned, last_updated)
								   values($1, $2, false, false, NOW())
								   on conflict (uid, fqanid) do nothing`, uid, fqanid)
		if queryerr != nil {
			log.WithFields(QueryFields(c.R, c.StartTime)).Error(queryerr)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
			return nil, apiErr
		}
	}

	return nil, nil
}

func setUserShellAndHomeDir(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	uid 		:= NewNullAttribute(UID)
	resourceid	:= NewNullAttribute(ResourceID)

	err := c.DBtx.QueryRow(`select (select uid from users where uname = $1),
								   (select compid from compute_resources where name = $2)`,
						   i[UserName], i[ResourceName]).Scan(&uid, &resourceid)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !uid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
	}
	if !resourceid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, ResourceName))
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	var member bool
	err = c.DBtx.QueryRow(`select ($1, $2) in (select uid, compid from compute_access)`,
						  uid, resourceid).Scan(&member)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !member {
		apiErr = append(apiErr, APIError{
			errors.New("user does not have access to compute resource"),
			ErrorAPIRequirement,
		})
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`update compute_access set shell = $1, home_dir = $2, last_updated = NOW()
						   where compid = $3 and uid = $4`, i[Shell], i[HomeDir], resourceid, uid)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, nil
}

func setUserShell(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	uid 	:= NewNullAttribute(UID)
	unitid	:= NewNullAttribute(UnitID)

	err := c.DBtx.QueryRow(`select (select uid from users where uname = $1),
								   (select unitid from affiliation_units where name = $2)`,
						   i[UserName], i[UnitName]).Scan(&uid, &unitid)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !uid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
	}
	if !unitid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	var member bool
	err = c.DBtx.QueryRow(`select ($1, $2) in (select uid, unitid from
						   compute_access join compute_resources using(compid))`,
						  uid, unitid).Scan(&member)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !member {
		apiErr = append(apiErr, APIError{
			errors.New("user does not have access to compute resource"),
			ErrorAPIRequirement,
		})
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`update compute_access set shell = $1, last_updated = NOW()
						  where uid = $2 and
						  compid in (select compid from compute_resources where unitid = $3)`, i[Shell], uid, unitid)
	if err != nil {	
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, nil
}

func getUserShellAndHomeDir(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	uid 		:= NewNullAttribute(UID)
	resourceid	:= NewNullAttribute(ResourceID)

	err := c.DBtx.QueryRow(`select (select uid from users where uname = $1),
								   (select compid from compute_resources where name = $2)`,
						   i[UserName], i[ResourceName]).Scan(&uid, &resourceid)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !uid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
	}
	if !resourceid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, ResourceName))
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	var member bool
	err = c.DBtx.QueryRow(`select ($1, $2) in (select uid, compid from compute_access)`,
						  uid, resourceid).Scan(&member)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !member {
		apiErr = append(apiErr, APIError{
			errors.New("user does not have access to compute resource"),
			ErrorAPIRequirement,
		})
		return nil, apiErr
	}

	row := NewMapNullAttribute(Shell, HomeDir)
	err = c.DBtx.QueryRow(`select shell, home_dir from compute_access
						   where uid = $1 and compid = $2 and (last_updated >= $3 or $3 is null)`,
						  uid, resourceid, i[LastUpdated]).Scan(row[Shell], row[HomeDir])
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	type jsonout map[Attribute]interface{}
	var out jsonout

	out = jsonout{
		Shell:	 row[Shell].Data,
		HomeDir: row[HomeDir].Data,
	}

	return out, nil
}

func getUserStorageQuota(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	uid 		:= NewNullAttribute(UID)
	unitid		:= NewNullAttribute(UnitID)
	resourceid	:= NewNullAttribute(ResourceID)

	err := c.DBtx.QueryRow(`select (select uid from users where uname = $1),
								   (select unitid from affiliation_units where name = $2),
								   (select storageid from storage_resources where name = $3)`,
						   i[UserName], i[UnitName], i[ResourceName]).Scan(&uid, &unitid, &resourceid)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !uid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
	}
	if !unitid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
	}
	if !resourceid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, ResourceName))
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	rows, err := DBptr.Query(`select path, value, unit, valid_until from storage_quota
							  where uid = $1 AND unitid = $2 and storageid = $3 and (valid_until is null or valid_until >= NOW())
							  order by valid_until desc`, uid, unitid, resourceid)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	type jsonentry map[Attribute]interface{}
	out := make(jsonentry)
	
	for rows.Next() {
		row := NewMapNullAttribute(Path, Value, QuotaUnit, ExpirationDate)
		rows.Scan(row[Path], row[Value], row[QuotaUnit], row[ExpirationDate])
		if row[Value].Valid {
			out = jsonentry{
				Path:			row[Path].Data,
				Value:			row[Value].Data,
				QuotaUnit:		row[QuotaUnit].Data,
				ExpirationDate:	row[ExpirationDate].Data,
			}
		}
	}

	return out, nil
}

func setUserStorageQuota(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError
	var vStorageid, vDataid, vUnitid sql.NullInt64

	gAccount := i[GroupAccount].Default(false)

	var dataAttr Attribute
	if !gAccount.Data.(bool) {
		dataAttr = UserName
	} else {
		dataAttr = GroupName
	}

	if !i[dataAttr].Valid {
		apiErr = append(apiErr, APIError{fmt.Errorf("required parameter %s not provided", dataAttr), ErrorAPIRequirement})
		return nil, apiErr
	}
	
	// get storageID, unitid, uid/gid
	var querystr string
	if gAccount.Data.(bool) {
		querystr = `select (select storageid from storage_resources where name=$1), (select groupid as id from groups where name=$2), (select unitid from affiliation_units where name=$3)`
	} else {
		querystr = `select (select storageid from storage_resources where name=$1), (select uid as id from users where uname=$2), (select unitid from affiliation_units where name=$3)`
	}
	queryerr := c.DBtx.QueryRow(querystr, i[ResourceName], i[dataAttr], i[UnitName]).Scan(&vStorageid, &vDataid, &vUnitid)
	if queryerr != nil && queryerr != sql.ErrNoRows {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(queryerr)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	if !vDataid.Valid {
		var dataID Attribute
		if !gAccount.Data.(bool) {
			dataID = UserName
		} else {
			dataID = GroupName
		}
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, dataID))
	} 
	if !vStorageid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, ResourceName))
	}
	if !vUnitid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
	}

	// We want to store the value in the DB in bytes, no matter what the input unit is. Convert the value here and then set the unit of "B" for bytes	
	newquota, converr := convertValue(i[Quota].Data, i[QuotaUnit].Data.(string), "B")
	if converr != nil {
		apiErr = append(apiErr, APIError{converr, ErrorInvalidData})
	}

	// set the quota value to be stored to newquota, which is now in bytes
	quota := strconv.FormatFloat(newquota, 'f', 0, 64)
	unit := "B"

	if len(apiErr) > 0 {
		return nil, apiErr
	}
	
	var vPath sql.NullString
	var column string

	if gAccount.Data.(bool) {
		column = `groupid`
	} else { 
		column = `uid` 
	}

	if i[Path].Valid {
		vPath.String = i[Path].Data.(string)
		vPath.Valid = i[Path].Valid
	} else if !i[Path].AbsoluteNull {
		queryerr = c.DBtx.tx.QueryRow(`select path from storage_quota
									 where storageid = $1 and ` + column + ` = $2 and
									 unitid = $3 and valid_until is NULL`,
									 vStorageid, vDataid, vUnitid).Scan(&vPath)
		if queryerr != nil && queryerr != sql.ErrNoRows {
			log.WithFields(QueryFields(c.R, c.StartTime)).Error(queryerr)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
			return nil, apiErr
		}
	}
	if !gAccount.Data.(bool) && !vPath.Valid {
		var msg string
		if !i[ExpirationDate].Valid { 
			msg = "null path for user quota"
		} else {
			msg = "no permanent quota"
		}
		apiErr = append(apiErr, APIError{errors.New(msg), ErrorAPIRequirement})
		return nil, apiErr
	}

	var tmpNull string
	if i[ExpirationDate].Valid {
		tmpNull = "not "
	}

	c.DBtx.Exec(`insert into storage_quota (storageid, ` + column + `, unitid, value, unit, valid_until, path, last_updated)
				values ($1, $2, $3, $4, $5, $6, $7, NOW())
				on conflict (storageid, ` + column + `) where valid_until is ` + tmpNull + `null
				do update set value = $4, unit = $5, valid_until = $6, path = $7, last_updated = NOW()`,
				vStorageid, vDataid, vUnitid, quota, unit, i[ExpirationDate], vPath)
	if !i[ExpirationDate].Valid {
		c.DBtx.Exec(`delete from storage_quota where storageid = $1 and ` + column + ` = $2 and valid_until is not null`, vStorageid, vDataid)
	}
	
	if c.DBtx.Error() != nil {
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	return nil, nil
}

func setUserExternalAffiliationAttribute(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	uid := NewNullAttribute(UID)
	err := c.DBtx.QueryRow(`select uid from users where uname = $1`, i[UserName]).Scan(&uid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	if !uid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
	}

	var validAttribute bool
	err = c.DBtx.QueryRow(`select $1 = any (enum_range(null::external_affiliation_attribute_attribute_type)::text[])`, i[UserAttribute]).Scan(&validAttribute)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	if !validAttribute {
		apiErr = append(apiErr, DefaultAPIError(ErrorInvalidData, UserAttribute))
	}
	
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`insert into external_affiliation_attribute (uid, attribute, value) values ($1, $2, $3)
						on conflict (uid, attribute) do update set value = $3`, uid, i[UserAttribute], i[Value])
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, nil
}
func removeUserExternalAffiliationAttribute(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	uName := strings.TrimSpace(q.Get("username"))
	attribute := strings.TrimSpace(q.Get("attribute"))

	if uName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No username specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No username specified.\" }")
		return
	}
	if attribute == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No attribute specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No attribute specified.\" }")
		return
	}

	authorized, authout := authorize(r)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"ferry_error\": \""+authout+"not authorized.\" }")
		return
	}

	DBtx, cKey, err := LoadTransaction(r, DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
	}
	defer DBtx.Rollback(cKey)

	var uid int
	var att sql.NullString

	queryerr := DBtx.tx.QueryRow(`select us.uid,eaa.attribute from (select uid from users where uname = $1) as us left join (select uid, attribute from external_affiliation_attribute where attribute = $2) as eaa on us.uid=eaa.uid`,uName, attribute).Scan(&uid,&att)
	if queryerr != nil {
		if queryerr == sql.ErrNoRows {
			log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"User does not exist.\" }")
			return
		} else if strings.Contains(queryerr.Error(), "invalid input value for enum") {
			log.WithFields(QueryFields(r, startTime)).Error("Invalid attribute.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"Invalid attribute.\" }")
			return
		} else {
			log.WithFields(QueryFields(r, startTime)).Error("Error in DB query: " + queryerr.Error())
			fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query. Check logs.\" }")
			return
		}
	}

	if !att.Valid {
		log.WithFields(QueryFields(r, startTime)).Error("User doesn't have this attribute.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"User doesn't have this attribute.\" }")
		return	
	}
	
	//	_, err = DBtx.Exec(fmt.Sprintf(`do $$
	//									declare v_uid int;
//									
//									declare c_uname text = '%s';
//									declare c_attribute text = '%s';
//
//									begin
//										select uid into v_uid from users where uname = c_uname;
//										if v_uid is null then
//											raise 'uname does not exist';
//										end if;
//
//										if (v_uid, c_attribute) not in (select uid, attribute from external_affiliation_attribute) then
//											raise 'attribute does not exist';
//										end if;
//
//										delete from external_affiliation_attribute where uid = v_uid and attribute = c_attribute;
//									end $$;`, uName, attribute))
//
	_, err = DBtx.Exec(`delete from external_affiliation_attribute where uid = $1 and attribute = $2`, uid, att.String)

	if err == nil {
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		fmt.Fprintf(w, "{ \"ferry_status\": \"success\" }")
	} else {
		if strings.Contains(err.Error(), `uname does not exist`) {
			log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"User does not exist.\" }")
		} else if strings.Contains(err.Error(), `attribute does not exist`) {
			log.WithFields(QueryFields(r, startTime)).Error("External affiliation attribute does not exist.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"External affiliation attribute does not exist.\" }")
		} else {
			log.WithFields(QueryFields(r, startTime)).Error(err.Error())
			fmt.Fprintf(w, "{ \"ferry_error\": \"Something went wrong.\" }")
		}
	}

	DBtx.Commit(cKey)
}
func getUserExternalAffiliationAttributes(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	uid := NewNullAttribute(UID)

	err := c.DBtx.tx.QueryRow(`select uid from users where uname = $1`, i[UserName]).Scan(&uid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !uid.Valid && i[UserName].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
		return nil, apiErr
	}

	rows, err := DBptr.Query(`select uname, attribute, value from
								external_affiliation_attribute as a
								join users as u using(uid)
							  where u.uid = coalesce($1, uid) and (a.last_updated >= $2 or $2 is null)
							  order by uname`, uid, i[LastUpdated])
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	type jsonentry map[Attribute]interface{}
	out := make(map[string][]jsonentry)

	for rows.Next() {
		row := NewMapNullAttribute(UserName, UserAttribute, Value)
		err = rows.Scan(row[UserName], row[UserAttribute], row[Value])

		if row[UserAttribute].Valid {
			entry := make(jsonentry)
			entry[UserAttribute] = row[UserAttribute].Data
			entry[Value] = row[Value].Data
			out[row[UserName].Data.(string)] = append(out[row[UserName].Data.(string)], entry)
		}
	}

	return out, nil
}

func addCertificateDNToUser(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	// DN validation
	dn, err := ExtractValidDN(i[DN].Data.(string))
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err.Error())
		apiErr = append(apiErr, DefaultAPIError(ErrorInvalidData, DN))
		return nil, apiErr
	}

	uid    := NewNullAttribute(UID)
	dnid   := NewNullAttribute(DNID)
	unitid := NewNullAttribute(UnitID)

	err = c.DBtx.QueryRow(`select (select uid from users where uname = $1),
								  (select dnid from user_certificates where dn=$2),
								  (select unitid from affiliation_units where name=$3)`,
								i[UserName], dn, i[UnitName]).Scan(&uid, &dnid, &unitid)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !uid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
	}
	if !unitid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	if !dnid.Valid {
		_, err := c.DBtx.Exec(`insert into user_certificates (dn, uid, last_updated) values ($1, $2, NOW()) returning dnid`, dn, uid)
		if err != nil {
			log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
			return nil, apiErr
		}
		err = c.DBtx.QueryRow(`select dnid from user_certificates where dn=$1`, dn).Scan(&dnid)
		if err != nil {
			log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
			return nil, apiErr
		}
	}

	_, err = c.DBtx.Exec(`insert into affiliation_unit_user_certificate (unitid, dnid, last_updated) values ($1, $2, NOW())
						  on conflict (unitid, dnid) do nothing`, unitid, dnid)
	if err != nil && !strings.Contains(err.Error(), `pk_affiliation_unit_user_certificate`) {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, nil
}

func removeUserCertificateDN(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

	authorized, authout := authorize(r)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"ferry_error\": \""+authout+"not authorized.\" }")
		return
	}

	q := r.URL.Query()
	uName := strings.TrimSpace(q.Get("username"))
	subjDN := strings.TrimSpace(q.Get("dn"))
	if uName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No username specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No username specified.\" }")
		return
	}
	if subjDN == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No DN specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No dn specified.\" }")
		return
	} else {
		dn, err := ExtractValidDN(subjDN)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error(err.Error())
			fmt.Fprintf(w, "{ \"ferry_error\": \"%s\" }", err.Error())
			return
		}
		subjDN = dn
	}

	DBtx, cKey, err := LoadTransaction(r, DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
	}
	defer DBtx.Rollback(cKey)

	var uid, dnid sql.NullInt64
	queryerr := DBtx.tx.QueryRow(`select us.uid, uc.dnid from (select 1 as key, uid from users where uname=$1 for update) as us full outer join (select 1 as key, dnid from user_certificates where dn=$2 for update) as uc on uc.key=us.key`,uName, subjDN).Scan(&uid,&dnid)
	if queryerr == sql.ErrNoRows {
		log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"User does not exist.\" }")
		return
	} else if queryerr != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error in DB query: " + queryerr.Error())
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query. Check logs.\" }")
		return
	}
	if ! uid.Valid {
		log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"User does not exist.\" }")
		return		
	}
	if ! dnid.Valid {
		log.WithFields(QueryFields(r, startTime)).Error("DN does not exist.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"DN does not exist.\" }")
		return		
	}
	_, err = DBtx.Exec(fmt.Sprintf(`do $$ 	
										declare  u_uid constant int := %d;
										declare  u_dnid constant int := %d;
										declare  v_count int;
									
									begin

										if (u_dnid, u_uid) not in (select dnid, uid from user_certificates) then
											raise 'dnid uid association does not exist';
										end if;

										select count(*) into v_count from
											 (select uid, unitid, count(unitid)
											  from affiliation_unit_user_certificate as ac
											  join user_certificates as uc on ac.dnid = uc.dnid
											  where uid = u_uid and unitid in (select unitid
																			   from affiliation_unit_user_certificate
																			   where dnid = u_dnid)
											  group by unitid, uid order by uid, unitid, count) as c
										where c.count = 1;

										if v_count > 0 then
											raise 'unique dnid unitid association';
										end if;

										delete from affiliation_unit_user_certificate where dnid=u_dnid;
										delete from user_certificates where dnid=u_dnid and uid=u_uid;
									end $$;`, uid.Int64, dnid.Int64))
	if err == nil {
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		fmt.Fprintf(w, "{ \"ferry_status\": \"success\" }")
	} else {
		if strings.Contains(err.Error(), `dnid uid association does not exist`) {
			log.WithFields(QueryFields(r, startTime)).Error("USER DN association does not exist.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"USER DN association does not exist.\" }")
		} else if strings.Contains(err.Error(), `unique dnid unitid association`) {
			log.WithFields(QueryFields(r, startTime)).Error("This certificate is unique for the user in one or more affiliation units.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"This certificate is unique for the user in one or more affiliation units.\" }")
		} else {
			log.WithFields(QueryFields(r, startTime)).Error(err.Error())
			fmt.Fprintf(w, "{ \"ferry_error\": \"Something went wrong.\" }")
			return
		}
	}

	DBtx.Commit(cKey)
}

func setUserInfo(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	if !i[FullName].Valid && !i[Status].Valid && !i[GroupAccount].Valid &&
	   !i[ExpirationDate].Valid && !i[ExpirationDate].AbsoluteNull {
		apiErr = append(apiErr, APIError{errors.New("not enough arguments"), ErrorAPIRequirement})
		return nil, apiErr
	}

	uid := NewNullAttribute(UID)
	expDate := NewNullAttribute(ExpirationDate)

	queryerr := c.DBtx.tx.QueryRow(`select uid, expiration_date from users where uname = $1`,
								   i[UserName]).Scan(&uid, &expDate)
	if queryerr == sql.ErrNoRows {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
		return nil, apiErr
	} else if queryerr != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(queryerr)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	expDate = i[ExpirationDate].Default(expDate.Data)

	_, queryerr = c.DBtx.Exec(`update users set	full_name = coalesce($2, full_name),
												status = coalesce($3, status),
												is_groupaccount = coalesce($4, is_groupaccount),
												expiration_date = $5,
												last_updated = NOW()
							   where uid = $1`,
							  uid, i[FullName], i[Status], i[GroupAccount], expDate)
	if queryerr != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(queryerr)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, nil
}

func createUser(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError
	
	groupid	:= NewNullAttribute(GroupID)
	expDate	:= i[ExpirationDate].Default("2038-01-01")

	err := c.DBtx.QueryRow(`select (select groupid from groups where name = $1)`, i[GroupName]).Scan(&groupid)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !groupid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, GroupName))
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`insert into users (uname, uid, full_name, status, expiration_date, last_updated)
						  values ($1, $2, $3, $4, $5, NOW())`,
						 i[UserName], i[UID], i[FullName], i[Status], expDate)
	if err != nil {
		if strings.Contains(err.Error(), "duplicate key value violates unique constraint \"pk_users\"") {
			apiErr = append(apiErr, DefaultAPIError(ErrorDuplicateData, UID))
		} else if strings.Contains(err.Error(), "duplicate key value violates unique constraint \"unq_users_uname\"") {
			apiErr = append(apiErr, DefaultAPIError(ErrorDuplicateData, UserName))
		} else {
			log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))	
		}
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`insert into user_group (uid, groupid, is_leader, last_updated)
						  values ($1, $2, false, NOW())`,
						 i[UID], groupid)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, nil
}

func getMemberAffiliations(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	uid			:= NewNullAttribute(UID)
	experiment	:= i[Experiment].Default(false)

	err := c.DBtx.QueryRow(`select uid from users where uname = $1`, i[UserName]).Scan(&uid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !uid.Valid && i[UserName].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
		return nil, apiErr
	}

	rows, err := DBptr.Query(`select distinct name, alternative_name from affiliation_units
							  join affiliation_unit_user_certificate as ac using(unitid)
							  join user_certificates using(dnid)
							  where uid = $1 and (
								  (((unitid in (select unitid from voms_url)) = $2) or not $2)
								  and (ac.last_updated >= $3 or $3 is null)
							  )`, uid, experiment, i[LastUpdated])
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	type jsonentry map[Attribute]interface{}
	var out []jsonentry

	for rows.Next() {
		row := NewMapNullAttribute(UnitName, AlternativeName)
		rows.Scan(row[UnitName], row[AlternativeName])

		if row[UnitName].Valid {
			out = append(out, jsonentry{
				UnitName:			row[UnitName].Data,
				AlternativeName:	row[AlternativeName].Data,
			})
		}
	}

	return out, nil
}

func getUserUname(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError
	
	uname := NewNullAttribute(UserName)

	err := c.DBtx.QueryRow(`select uname from users where uid = $1`, i[UID]).Scan(&uname)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !uname.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UID))
		return nil, apiErr
	}

	return uname.Data, nil
}

func getUserUID(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError
	
	uid := NewNullAttribute(UID)

	err := c.DBtx.QueryRow(`select uid from users where uname = $1`, i[UserName]).Scan(&uid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !uid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
		return nil, apiErr
	}

	return uid.Data, nil
}

func deleteUser(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	uName := q.Get("username")
	if uName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No username specified in http query.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No username specified.\" }")
		return		
	}

	authorized, authout := authorize(r)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"ferry_error\": \""+authout+"not authorized.\" }")
		return
	}

	// check if the username is already in the DB. If it is not, say so and exit since there is nothing to delete.
	var uname string
	checkerr := DBptr.QueryRow(`select uid from users where uname=$1`, uName).Scan(&uname)
	
	switch {
	case checkerr == sql.ErrNoRows: 
		// set the header for success since we are already at the desired result
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "{ \"ferry_error\": \"Nothing to delete; user does not exist.\" }")
		log.WithFields(QueryFields(r, startTime)).Info("user ID " + uName + " not found in DB.")
		return	
	case checkerr != nil:
		fmt.Fprintf(w, "{ \"ferry_error\": \"Nothing to delete; user does not exist.\" }")
		log.WithFields(QueryFields(r, startTime)).Error("deleteUser: Error querying DB for user " + uName + ".")
		return	
	default:
		// actually do the deletion now
		DBtx, cKey, err := LoadTransaction(r, DBptr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error(err)
		}
		defer DBtx.Rollback(cKey)
	
		_, err = DBtx.Exec(`delete from users where uname=$1`,uName) 
		if err == nil {	
			fmt.Fprintf(w, "{ \"ferry_status\": \"success\" }")
			log.WithFields(QueryFields(r, startTime)).Info("Success!")
			DBtx.Commit(cKey)
			return
		} else {
			fmt.Fprintf(w, "{ \"ferry_error\": \"%s\" }",err.Error())
			log.WithFields(QueryFields(r, startTime)).Error("deleteUser: Error during delete action for user " + uName + ": " + err.Error())
			return			
		}	
	}
}

func getUserAccessToComputeResources(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError
	
	uid := NewNullAttribute(UID)

	err := c.DBtx.tx.QueryRow(`select uid from users where uname = $1`, i[UserName]).Scan(&uid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !uid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
		return nil, apiErr
	}

	rows, err := c.DBtx.Query(`select name, type, shell, home_dir from
								compute_access as ca join
								compute_resources using(compid)
							   where uid = $1 and (ca.last_updated>=$2 or $2 is null)`,
							  uid, i[LastUpdated])
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	type jsonentry map[Attribute]interface{}
	out := make([]jsonentry, 0)

	for rows.Next() {
		row := NewMapNullAttribute(ResourceName, ResourceType, Shell, HomeDir)
		rows.Scan(row[ResourceName], row[ResourceType], row[Shell], row[HomeDir])

		if row[ResourceName].Valid {
			entry := jsonentry {
				ResourceName:	row[ResourceName].Data,
				ResourceType:	row[ResourceType].Data,
				Shell:			row[Shell].Data,
				HomeDir:		row[HomeDir].Data,
			}
			out = append(out, entry)
		}
	}

	return out, nil
}

func getStorageQuotas(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	uid 		:= NewNullAttribute(UID)
	groupid		:= NewNullAttribute(GroupID)
	resourceid	:= NewNullAttribute(ResourceID)

	err := c.DBtx.QueryRow(`select
								(select uid from users where uname = $1),
								(select groupid from groups where name = $2 and type = 'UnixGroup'),
								(select storageid from storage_resources where name = $3)`,
						   i[UserName], i[GroupName], i[ResourceName]).Scan(&uid, &groupid, &resourceid)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !uid.Valid && i[UserName].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
	}
	if !groupid.Valid && i[GroupName].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, GroupName))
	}
	if !resourceid.Valid && i[ResourceName].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, ResourceName))
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	rows, err := DBptr.Query(`select uname, g.name, sr.name, path, value, unit, valid_until from
								storage_quota as sq
								left join users as u on sq.uid = u.uid
								left join groups as g on sq.groupid = g.groupid
								join storage_resources as sr on sq.storageid = sr.storageid
							  where
							  		(u.uid = $1 or $1 is null)
								and (g.groupid = $2 or $2 is null)
								and (sr.storageid = coalesce($3, sr.storageid))
								and (valid_until is null or valid_until >= NOW())
								and (sq.last_updated >= $4 or $4 is null)
							  order by uname asc, g.name asc, sr.name asc, valid_until desc`,
							 uid, groupid, resourceid, i[LastUpdated])
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	const Users Attribute = "users"
	const Groups Attribute = "groups"

	type jsonquota 		map[Attribute]interface{}
	type jsonstorage	map[string]jsonquota
	type jsonowner		map[string]jsonstorage
	
	out := make(map[Attribute]jsonowner)
	out[Users] 	= make(jsonowner)
	out[Groups]	= make(jsonowner)

	for rows.Next() {
		row := NewMapNullAttribute(UserName, GroupName, ResourceName, Path, Quota, QuotaUnit, ExpirationDate)
		rows.Scan(row[UserName], row[GroupName], row[ResourceName], row[Path], row[Quota], row[QuotaUnit], row[ExpirationDate])

		var ownerName, ownerType Attribute
		if row[UserName].Valid {
			ownerName = UserName
			ownerType = Users
		}
		if row[GroupName].Valid {
			ownerName = GroupName
			ownerType = Groups
		}

		if ownerName != "" {
			if _, ok := out[ownerType][row[ownerName].Data.(string)]; !ok {
				out[ownerType][row[ownerName].Data.(string)] = make(jsonstorage)
			}
			out[ownerType][row[ownerName].Data.(string)][row[ResourceName].Data.(string)] = jsonquota {
				Path: 			row[Path].Data,
				Quota: 			row[Quota].Data,
				QuotaUnit:		row[QuotaUnit].Data,
				ExpirationDate:	row[ExpirationDate].Data,
			}
		}
	}

	return out, nil
}

func setUserAccessToComputeResource(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	primary := i[Primary].Default(false)

	dShell	:= NewNullAttribute(Shell)
	dHome	:= NewNullAttribute(HomeDir)
	groupid	:= NewNullAttribute(GroupID)
	compid	:= NewNullAttribute(ResourceID)
	uid		:= NewNullAttribute(UID)

	err := c.DBtx.QueryRow(`select
								(select uid from users where uname = $1),
								(select groupid from groups where name = $2 and type = 'UnixGroup'),
								(select compid from compute_resources where name = $3)`,
						   i[UserName], i[GroupName], i[ResourceName]).Scan(&uid, &groupid, &compid)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !uid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
	}
	if !groupid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, GroupName))
	}
	if !compid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, ResourceName))
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	// Ensure user is member of group (user_group)
	_, err = c.DBtx.Exec(`insert into user_group (uid, groupid) values ($1, $2)
						  on conflict (uid, groupid) do nothing`, uid, groupid)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	
	// Grant user access to the resource (compute_access)
	err = c.DBtx.QueryRow(`select default_shell, default_home_dir from compute_resources where compid = $1`,
						 compid).Scan(&dShell, &dHome)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if dHome.Valid {
		homeStr := strings.TrimRight(dHome.Data.(string), "/")
		if homeStr == "/nashome" {
			homeStr = "/nashome/" + i[UserName].Data.(string)[0:1]
		}
		homeStr = homeStr + "/" + i[UserName].Data.(string)
		dHome.Scan(homeStr)
	}
	
	shell := i[Shell].Default(dShell.Data.(string))
	home  := i[HomeDir].Default(dHome.Data.(string))

	if !shell.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorInvalidData, Shell))
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`insert into compute_access as ca (compid, uid, shell, home_dir) values ($1, $2, $3, $4)
						  on conflict (compid, uid) do update set shell = coalesce($5, ca.shell), home_dir = coalesce($6, ca.home_dir)`,
						compid, uid, shell, home, i[Shell], i[HomeDir])
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	// Add user to group in the resource (compute_access_group)
	var priCount int
	err = c.DBtx.QueryRow(`select count(*) from compute_access_group where uid = $1 and compid = $2 and is_primary`,
							uid, groupid).Scan(&priCount)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	if priCount == 0 {
		primary.Scan(true)
	}

	if primary.Data.(bool) {
		_, err = c.DBtx.Exec(`update compute_access_group set is_primary = false where uid = $1 and compid = $2 and is_primary`,
							uid, compid)
		if err != nil {
			log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
			return nil, apiErr
		}
	}

	_, err = c.DBtx.Exec(`insert into compute_access_group as cg (compid, uid, groupid, is_primary)
						  values ($1, $2, $3, $4)
						  on conflict (compid, uid, groupid) do update set is_primary = coalesce($5, cg.is_primary)`,
						compid, uid, groupid, primary, i[Primary])
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, nil
}

func getAllUsers(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	status := i[Status].Default(false)

	rows, err := DBptr.Query(`select uname, uid, full_name, status, expiration_date from users
							  where (status=$1 or not $1) and (last_updated>=$2 or $2 is null)
							  order by uname`, status, i[LastUpdated])
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	type jsonout map[Attribute]interface{}
	var out []jsonout
	
	for rows.Next() {
		row := NewMapNullAttribute(UserName, UID, FullName, Status, ExpirationDate)
		rows.Scan(row[UserName], row[UID], row[FullName], row[Status], row[ExpirationDate])
		out = append(out, jsonout{
			UserName:		row[UserName].Data,
			UID:			row[UID].Data,
			FullName:		row[FullName].Data,
			Status:			row[Status].Data,
			ExpirationDate:	row[ExpirationDate].Data,
		})
	}

	return out, nil
}

func getAllUsersFQANs(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	rows, err := DBptr.Query(`select uname, fqan, name from grid_access as ga
							  join grid_fqan as gf using(fqanid)
							  join users as u using(uid)
							  join affiliation_units as au using(unitid)
							  where (ga.last_updated>=$1 or gf.last_updated>=$1 or
									  u.last_updated>=$1 or au.last_updated>=$1 or $1 is null) order by uname;`, i[LastUpdated])
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()
	
	type jsonfqan map[Attribute]interface{}
	out := make(map[string][]jsonfqan)
	
	for rows.Next() {
		row := NewMapNullAttribute(UserName, FQAN, UnitName)
		rows.Scan(row[UserName], row[FQAN], row[UnitName])
		out[row[UserName].Data.(string)] = append(out[row[UserName].Data.(string)], jsonfqan{
			FQAN: row[FQAN].Data,
			UnitName: row[UnitName].Data,
		})
	}

	return out, nil
}
