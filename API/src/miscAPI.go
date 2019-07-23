package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq"
	log "github.com/sirupsen/logrus"
)

// build parameters
var (
	release_ver = "unknown"
	build_date  = "unknown"
)

// IncludeMiscAPIs includes all APIs described in this file in an APICollection
func IncludeMiscAPIs(c *APICollection) {
	testBaseAPI := BaseAPI{
		InputModel{
			Parameter{UserName, true},
			Parameter{UID, true},
			Parameter{ExpirationDate, false},
		},
		func(c APIContext, i Input) (interface{}, []APIError) {
			out := make(map[Attribute]interface{})
			out[UserName] = "TEST"
			return out, nil
		},
	}
	c.Add("testBaseAPI", &testBaseAPI)

	setStorageQuota := BaseAPI{
		InputModel{
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
		setStorageQuota,
	}
	c.Add("setStorageQuota", &setStorageQuota)

	getGroupGID := BaseAPI{
		InputModel{
			Parameter{GroupName, true},
		},
		getGroupGID,
	}
	c.Add("getGroupGID", &getGroupGID)

	getGroupFile := BaseAPI{
		InputModel{
			Parameter{UnitName, false},
			Parameter{ResourceName, false},
			Parameter{LastUpdated, false},
		},
		getGroupFile,
	}
	c.Add("getGroupFile", &getGroupFile)

	getGridMapFile := BaseAPI{
		InputModel{
			Parameter{UnitName, false},
			Parameter{ResourceName, false},
			Parameter{LastUpdated, false},
		},
		getGridMapFile,
	}
	c.Add("getGridMapFile", &getGridMapFile)

	getGridMapFileByVO := BaseAPI{
		InputModel{
			Parameter{UnitName, false},
			Parameter{LastUpdated, false},
		},
		getGridMapFileByVO,
	}
	c.Add("getGridMapFileByVO", &getGridMapFileByVO)

	getVORoleMapFile := BaseAPI{
		InputModel{
			Parameter{ResourceName, false},
			Parameter{LastUpdated, false},
		},
		getVORoleMapFile,
	}
	c.Add("getVORoleMapFile", &getVORoleMapFile)

	getGroupName := BaseAPI{
		InputModel{
			Parameter{GID, true},
		},
		getGroupName,
	}
	c.Add("getGroupName", &getGroupName)

	lookupCertificateDN := BaseAPI{
		InputModel{
			Parameter{DN, true},
		},
		lookupCertificateDN,
	}
	c.Add("lookupCertificateDN", &lookupCertificateDN)

	getMappedGidFile := BaseAPI{
		InputModel{},
		getMappedGidFile,
	}
	c.Add("getMappedGidFile", &getMappedGidFile)

	getStorageAuthzDBFile := BaseAPI{
		InputModel{
			Parameter{PasswdMode, false},
			Parameter{LastUpdated, false},
		},
		getStorageAuthzDBFile,
	}
	c.Add("getStorageAuthzDBFile", &getStorageAuthzDBFile)

	getAffiliationMembersRoles := BaseAPI{
		InputModel{
			Parameter{UnitName, false},
			Parameter{Role, false},
		},
		getAffiliationMembersRoles,
	}
	c.Add("getAffiliationMembersRoles", &getAffiliationMembersRoles)

	getStorageAccessLists := BaseAPI{
		InputModel{
			Parameter{ResourceName, false},
		},
		getStorageAccessLists,
	}
	c.Add("getStorageAccessLists", &getStorageAccessLists)

	createComputeResource := BaseAPI{
		InputModel{
			Parameter{ResourceName, true},
			Parameter{ResourceType, true},
			Parameter{HomeDir, true},
			Parameter{Shell, false},
			Parameter{UnitName, false},
		},
		createComputeResource,
	}
	c.Add("createComputeResource", &createComputeResource)

	setComputeResourceInfo := BaseAPI{
		InputModel{
			Parameter{ResourceName, true},
			Parameter{ResourceType, false},
			Parameter{HomeDir, false},
			Parameter{Shell, false},
			Parameter{UnitName, false},
		},
		setComputeResourceInfo,
	}
	c.Add("setComputeResourceInfo", &setComputeResourceInfo)

	createStorageResource := BaseAPI{
		InputModel{
			Parameter{ResourceName, true},
			Parameter{ResourceType, true},
			Parameter{QuotaUnit, false},
			Parameter{Path, false},
			Parameter{Quota, false},
		},
		createStorageResource,
	}
	c.Add("createStorageResource", &createStorageResource)
}

func NotDoneYet(w http.ResponseWriter, r *http.Request, t time.Time) {
	fmt.Fprintf(w, `{"ferry_error": "This function is not done yet!"}`)
	log.WithFields(QueryFields(r, t)).Error("This function is not done yet!")
}

func getPasswdFile(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	unit := strings.TrimSpace(q.Get("unitname"))
	comp := strings.TrimSpace(q.Get("resourcename"))

	lastupdate, parserr := stringToParsedTime(strings.TrimSpace(q.Get("last_updated")))
	if parserr != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error parsing provided update time: " + parserr.Error())
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error parsing last_updated time. Check ferry logs. If provided, it should be an integer representing an epoch time.\"}")
		return
	}

	rows, err := DBptr.Query(`select aname, rname, uname, uid, gid, full_name, home_dir, shell, unit_exists, comp_exists, last_updated from (
                                                              select 1 as key, au.name as aname, u.uname, u.uid, g.gid, u.full_name, ca.home_dir, ca.shell, cr.name as rname, cag.last_updated as last_updated
                                                              from compute_access_group as cag 
                                                              left join compute_access as ca using (compid, uid) 
                                                              join groups as g on g.groupid=cag.groupid 
                                                              join compute_resources as cr on cr.compid=cag.compid 
                                                              left join affiliation_units as au on au.unitid=cr.unitid 
                                                              join users as u on u.uid=cag.uid
                                                              where  cag.is_primary = true and (au.name = $1 or $3) and (cr.name = $2 or $4) and (ca.last_updated>=$5 or u.last_updated>=$5 or au.last_updated>=$5 or cr.last_updated>=$5 or g.last_updated>=$5 or $5 is null) order by au.name, cr.name
							) as t
								right join (select 1 as key,
								$1 in (select name from affiliation_units) as unit_exists,
								$2 in (select name from compute_resources) as comp_exists
							) as c on t.key = c.key;`, unit, comp, unit == "", comp == "", lastupdate)

	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Error(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
		return
	}
	defer rows.Close()

	var unitExists bool
	var compExists bool

	type jsonuser struct {
		Uname string `json:"username"`
		Uid   string `json:"uid"`
		Gid   string `json:"gid"`
		Gecos string `json:"gecos"`
		Hdir  string `json:"homedir"`
		Shell string `json:"shell"`
	}
	type jsonunit struct {
		Resources map[string][]jsonuser `json:"resources"`
		Lasttime  int64                 `json:"last_updated"`
	}
	Out := make(map[string]jsonunit)

	lasttime := int64(0)
	prevAname := ""
	prevRname := ""
	tmpResources := make(map[string][]jsonuser, 0)
	tmpUsers := make([]jsonuser, 0)
	for rows.Next() {
		var tmpAname, tmpRname, tmpUname, tmpUid, tmpGid, tmpGecos, tmpHdir, tmpShell, tmpTime sql.NullString
		rows.Scan(&tmpAname, &tmpRname, &tmpUname, &tmpUid, &tmpGid, &tmpGecos, &tmpHdir, &tmpShell, &unitExists, &compExists, &tmpTime)
		log.WithFields(QueryFields(r, startTime)).Debugln(tmpAname.String + " " + tmpRname.String + " " + tmpUname.String)

		if !tmpRname.Valid {
			continue
		}

		if !tmpAname.Valid {
			tmpAname.Valid = true
			tmpAname.String = "null"
		}
		if prevRname == "" {
			prevRname = tmpRname.String
		}
		if prevAname == "" {
			prevAname = tmpAname.String
		}

		if tmpRname.Valid {
			if prevRname != tmpRname.String {
				tmpResources[prevRname] = tmpUsers
				tmpUsers = make([]jsonuser, 0)
				prevRname = tmpRname.String
			}
			if prevAname != tmpAname.String {
				Out[prevAname] = jsonunit{tmpResources, lasttime}
				tmpResources = make(map[string][]jsonuser, 0)
				lasttime = 0
				if tmpAname.Valid {
					prevAname = tmpAname.String
				} else {
					prevAname = "null"
				}

			}
			if tmpTime.Valid {
				log.WithFields(QueryFields(r, startTime)).Debugln("tmpTime is valid" + tmpTime.String)

				parseTime, parserr := time.Parse(time.RFC3339, tmpTime.String)
				if parserr != nil {
					log.WithFields(QueryFields(r, startTime)).Error("Error parsing last updated time of " + tmpTime.String)
				} else {
					if lasttime == 0 || (parseTime.Unix() > lasttime) {
						lasttime = parseTime.Unix()
					}
				}
			} else {
				log.WithFields(QueryFields(r, startTime)).Debugln("tmpTime is not valid")
			}
			tmpUsers = append(tmpUsers, jsonuser{tmpUname.String, tmpUid.String, tmpGid.String, tmpGecos.String, tmpHdir.String, tmpShell.String})
		}
	}
	if prevAname != "" {
		tmpResources[prevRname] = tmpUsers
		Out[prevAname] = jsonunit{tmpResources, lasttime}
	}

	var output interface{}
	if len(Out) == 0 {
		type jsonerror struct {
			Error string `json:"ferry_error"`
		}
		var Err []jsonerror
		if !unitExists && unit != "" {
			Err = append(Err, jsonerror{"Affiliation unit does not exist."})
			log.WithFields(QueryFields(r, startTime)).Error("Affiliation unit does not exist.")
		}
		if !compExists && comp != "" {
			Err = append(Err, jsonerror{"Resource does not exist."})
			log.WithFields(QueryFields(r, startTime)).Error("Resource does not exist.")
		}
		if len(Err) == 0 {
			Err = append(Err, jsonerror{"No entries were found for this query."})
			log.WithFields(QueryFields(r, startTime)).Error("No entries were found for this query.")
		}
		output = Err
	} else {
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		output = Out
	}
	jsonout, err := json.Marshal(output)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
	}
	fmt.Fprintf(w, string(jsonout))
}
func getGroupFile(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	unitid := NewNullAttribute(UnitID)
	compid := NewNullAttribute(ResourceID)
	err := c.DBtx.QueryRow(`select (select unitid from affiliation_units where name = $1),
								   (select compid from compute_resources where name = $2)`,
						   i[UnitName], i[ResourceName]).Scan(&unitid, &compid)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !unitid.Valid && i[UnitName].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
	}
	if !compid.Valid && i[ResourceName].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, ResourceName))
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	rows, err := c.DBtx.Query(`select distinct g.name, gid, uname, is_primary, cg.last_updated
                                from compute_access_group cg
								join compute_resources using (compid)
								join groups g using(groupid)
								join users using(uid)
							   where (unitid = $1 or $1 is null) and (compid = $2 or $2 is null)
									  and (g.type = 'UnixGroup') and (cg.last_updated>=$3 or $3 is null)
							   order by name, uname`,
							  unitid, compid, i[LastUpdated])
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	type jsongroup map[Attribute]interface{}
	out := make([]jsongroup, 0)
	entry := make(jsongroup)

	const Users Attribute = "users"
	users := make([]interface{}, 0)

	lastTime  := NewNullAttribute(LastUpdated)
	prevGname := NewNullAttribute(GroupName)
	for rows.Next() {
		row := NewMapNullAttribute(GroupName, GID, UserName, Primary, LastUpdated)
		rows.Scan(row[GroupName], row[GID], row[UserName], row[Primary], row[LastUpdated])

		if row[GroupName].Valid {
			if prevGname != *row[GroupName] {
				if prevGname.Valid {
					entry[Users] = users
					entry[LastUpdated] = lastTime.Data.(time.Time).Unix()
					out = append(out, entry)
					users = make([]interface{}, 0)
					lastTime = NewNullAttribute(LastUpdated)
				}
				entry = make(jsongroup)
				entry[GroupName] = row[GroupName].Data
				entry[GID] = row[GID].Data
				prevGname = *row[GroupName]
			}
			if !row[Primary].Data.(bool) {
				users = append(users, row[UserName].Data)
			}
			
			if row[LastUpdated].Valid {
				if !lastTime.Valid || (row[LastUpdated].Data.(time.Time).Unix() > lastTime.Data.(time.Time).Unix()) {
					lastTime = *row[LastUpdated]
				}
			}
		}
	}
	if len(entry) > 0 {
		entry[Users] = users
		entry[LastUpdated] = lastTime.Data.(time.Time).Unix()
		out = append(out, entry)
	}

	return out, nil
}
func getGridMapFile(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	unitid := NewNullAttribute(UnitID)
	compid := NewNullAttribute(ResourceID)
	err := c.DBtx.QueryRow(`select (select unitid from affiliation_units where name = $1),
								   (select compid from compute_resources where name = $2)`,
						   i[UnitName], i[ResourceName]).Scan(&unitid, &compid)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !unitid.Valid && i[UnitName].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
	}
	if !compid.Valid && i[ResourceName].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, ResourceName))
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	rows, err := c.DBtx.Query(`select distinct dn, uname 
								from affiliation_unit_user_certificate as ac
								left join user_certificates as uc using(dnid)
								left join users as us using(uid)
								left join compute_access as ca using(uid)
							   where (unitid = $1 or $1 is null) and (compid = $2 or $2 is null) and
									 (ac.last_updated>=$3 or uc.last_updated>=$3 or us.last_updated>=$3 or $3 is null)`,
							  unitid, compid, i[LastUpdated])
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	type jsondnmap map[Attribute]interface{}
	var out []jsondnmap

	for rows.Next() {
		row := NewMapNullAttribute(DN, UserName)
		rows.Scan(row[DN], row[UserName])
		if row[DN].Valid {
			out = append(out, jsondnmap{
				DN: row[DN].Data,
				UserName: row[UserName].Data,
			})
		}
	}

	return out, nil
}

func getGridMapFileByVO(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	unitid := NewNullAttribute(UnitID)
	err := c.DBtx.QueryRow(`select unitid from affiliation_units where name = $1`, i[UnitName]).Scan(&unitid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !unitid.Valid && i[UnitName].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
		return nil, apiErr
	}

	rows, err := c.DBtx.Query(`select name, dn, uname 
								from  affiliation_unit_user_certificate as ac
								left join user_certificates as uc using(dnid)
								left join users as us using(uid)
								left join affiliation_units as au using(unitid)
								where (unitid = $1 or $1 is null) and (ac.last_updated >= $2 or uc.last_updated >= $2 or
									   us.last_updated >= $2 or au.last_updated >= $2 or $2 is null)`,
							  unitid, i[LastUpdated])
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	type jsondnmap map[Attribute]interface{}
	out := make(map[string][]jsondnmap)

	for rows.Next() {
		row := NewMapNullAttribute(UnitName, DN, UserName)
		rows.Scan(row[UnitName], row[DN], row[UserName])
		if row[DN].Valid {
			out[row[UnitName].Data.(string)] = append(out[row[UnitName].Data.(string)], jsondnmap{
				DN: row[DN].Data,
				UserName: row[UserName].Data,
			})
		}
	}

	return out, nil
}

func getVORoleMapFile(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	compid := NewNullAttribute(UnitID)
	err := c.DBtx.QueryRow(`select compid from compute_resources where name = $1`, i[ResourceName]).Scan(&compid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !compid.Valid && i[UnitName].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, compid))
		return nil, apiErr
	}

	rows, err := DBptr.Query(`select distinct fqan, uname, name
								from grid_fqan as gf
								join users as u on gf.mapped_user = u.uid
								join compute_access_group as cag on (cag.groupid=gf.mapped_group and gf.mapped_user=cag.uid)
								left join affiliation_units using(unitid)
							  where (compid = $1 or $1 is null) and (gf.last_updated >= $2 or u.last_updated >= $2 or $2 is null)`,
							 compid, i[LastUpdated])
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	type jsonmapping map[Attribute]interface{}
	var out []jsonmapping

	for rows.Next() {
		row := NewMapNullAttribute(FQAN, UserName, UnitName)
		rows.Scan(row[FQAN], row[UserName], row[UnitName])
		if row[FQAN].Valid {
			out = append(out, jsonmapping{
				FQAN: row[FQAN].Data,
				UserName: row[UserName].Data,
				UnitName: row[UnitName].Data,
			})
		}
	}

	return out, nil
}

func getGroupGID(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	gid := NewNullAttribute(GID)
	groupid := NewNullAttribute(GroupID)

	err := c.DBtx.QueryRow(`select groupid, gid from groups where name = $1 and type = 'UnixGroup'`, i[GroupName]).Scan(&groupid, &gid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	type jsongroup map[Attribute]interface{}
	out := make(jsongroup)

	if groupid.Valid {
		out[GroupID] = groupid.Data
		out[GID] = gid.Data
	} else {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, GroupName))
		return nil, apiErr
	}

	return out, nil
}

func getGroupName(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	groupname := NewNullAttribute(GroupName)

	err := c.DBtx.QueryRow(`select name from groups where gid=$1`, i[GID]).Scan(&groupname)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	type jsongid map[Attribute]interface{}
	out := make(jsongid)
	if groupname.Valid {
		out[GroupName] = groupname.Data
	} else {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, GroupID))
		return nil, apiErr
	}
	return out, nil
}

func lookupCertificateDN(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	dnid := NewNullAttribute(DNID)
	err := c.DBtx.QueryRow(`select dnid from user_certificates where dn = $1`, i[DN]).Scan(&dnid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !dnid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, DN))
		return nil, apiErr
	}

	row := NewMapNullAttribute(UID, UserName)
	err = c.DBtx.QueryRow(`select uid, uname from user_certificates join users using(uid) where dnid = $1;`,
						  dnid).Scan(row[UID], row[UserName])
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	type jsonuser map[Attribute]interface{}
	out := make(jsonuser)

	if row[UID].Valid {
		out = jsonuser{
			UID: row[UID].Data,
			UserName: row[UserName].Data,
		}
	}

	return out, nil
}

func getMappedGidFile(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	rows, err := c.DBtx.Query(`select fqan, uname, gid from grid_fqan as gf
							   left join groups as g on g.groupid = gf.mapped_group
							   left join users as u on u.uid = gf.mapped_user`)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	type jsonmapping map[Attribute]interface{}
	var out []jsonmapping

	for rows.Next() {
		row := NewMapNullAttribute(FQAN, UserName, GID)
		rows.Scan(row[FQAN], row[UserName], row[GID])

		if row[FQAN].Valid {
			//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
			//!!REMOVE THIS EXCEPTION ONCE DCACHE RESOURCE EXISTS!!
			//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
			if !((strings.Contains(row[FQAN].Data.(string), "Role=Analysis") && row[UserName].Valid) ||
				(row[FQAN].Data.(string) == "/des/Role=Production/Capability=NULL" && row[UserName].Data.(string) == "des")) {
				out = append(out, jsonmapping{
					FQAN: row[FQAN].Data,
					UserName: row[UserName].Data,
					GID: row[GID].Data,
				})
			}
		}
	}

	return out, nil
}

func getStorageAuthzDBFile(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	rows, err := c.DBtx.Query(`select full_name, uname, uid, gid, ug.last_updated
								from users
								join user_group as ug using(uid)
								join groups using(groupid)
                               where type = 'UnixGroup' and (ug.last_updated>=$1 or $1 is null)
							   order by uname`, i[LastUpdated])
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	authMode := func(rows *sql.Rows) interface{} {
		const Decision Attribute = "decision"
		const Privileges Attribute = "privileges"
		const Groups Attribute = "groups"
		const Root = "root"

		type jsonentry map[Attribute]interface{}

		entry := make(jsonentry)
		out := make([]jsonentry, 0)

		prevUser := NewNullAttribute(UserName)
		for rows.Next() {
			row := NewMapNullAttribute(FullName, UserName, UID, GID, LastUpdated)
			rows.Scan(row[FullName], row[UserName], row[UID], row[GID], row[LastUpdated])

			if row[GID].Valid {
				if prevUser != *row[UserName] {
					if prevUser.Valid {
						out = append(out, entry)
						entry = make(jsonentry)
					}
					entry[Decision] = "authorize"
					entry[UserName] = row[UserName].Data
					entry[Privileges] = "read-write"
					entry[UID] = row[UID].Data
					entry[Groups] = make([]interface{}, 0)
					entry[HomeDir] = "/"
					entry[Root] = "/pnfs/fnal.gov/usr"
					entry[Path] = "/"
				}
				entry[Groups] = append(entry[Groups].([]interface{}), row[GID].Data)
				prevUser = *row[UserName]
			}
		}
		out = append(out, entry)
		return out
	}

	passwdMode := func(rows *sql.Rows) interface{} {
		const GECOS Attribute = "gecos"
		const Resources Attribute = "resources"
		type jsonmap map[Attribute]interface{}

		out := make(jsonmap)

		tmpMap := make(map[string][]jsonmap)
		lasttime := int64(0)
		prevUname := NewNullAttribute(UserName)
		for rows.Next() {
			row := NewMapNullAttribute(FullName, UserName, UID, GID, LastUpdated)
			rows.Scan(row[FullName], row[UserName], row[UID], row[GID], row[LastUpdated])

			if lasttime == 0 || (row[LastUpdated].Data.(time.Time).Unix() > lasttime) {
				lasttime = row[LastUpdated].Data.(time.Time).Unix()
			}

			if *row[UserName] != prevUname {
				tmpMap["all"] = append(tmpMap["all"], jsonmap {
					UserName: row[UserName].Data,
					UID: row[UID].Data,
					GID: row[GID].Data,
					GECOS: row[FullName].Data,
					HomeDir: "/home/" + row[UserName].Data.(string),
					Shell: "/sbin/nologin",
				})
				prevUname = *row[UserName]
			}
		}
		out["fermilab"] = jsonmap{
			Resources: tmpMap,
			LastUpdated: lasttime,
		}

		return out
	}

	var out interface{}
	if !i[PasswdMode].Valid {
		out = authMode(rows)
	} else {
		out = passwdMode(rows)
	}

	return out, nil
}

func getAffiliationMembersRoles(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	role := i[Role].Default("%")

	unitid := NewNullAttribute(UnitID)
	err := c.DBtx.QueryRow(`select unitid from affiliation_units where name = $1`, i[UnitName]).Scan(&unitid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !unitid.Valid && i[UnitName].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
		return nil, apiErr
	}

	rows, err := DBptr.Query(`select name, fqan, uname, full_name
								from grid_access
								join grid_fqan using(fqanid)
								join users using(uid)
								left join affiliation_units using(unitid)
							  where (unitid = $1 or $1 is null) and (lower(fqan) like lower($2))`,
							 unitid, "%/role=" + role.Data.(string) + "/%")
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	type jsonentry map[Attribute]interface{}
	out := make(map[string][]jsonentry)

	for rows.Next() {
		row := NewMapNullAttribute(UnitName, FQAN, UserName, FullName)
		rows.Scan(row[UnitName], row[FQAN], row[UserName], row[FullName])

		if row[FQAN].Valid {
			out[row[UnitName].Data.(string)] = append(out[row[UnitName].Data.(string)], jsonentry {
				FQAN: row[FQAN].Data,
				UserName: row[UserName].Data,
				FullName: row[FullName].Data,
			})
		}
	}

	return out, nil
}

func getStorageAccessLists(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	resource := i[ResourceName].Default("%")

	rows, err := DBptr.Query(`select server, volume, access_level, host from nas_storage where server like $1;`, resource)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	const Host Attribute = "host"
	const AccessLevel Attribute = "accesslevel"
	type jsonhost map[Attribute]interface{}

	out := make(map[string][]map[string][]jsonhost)
	entry := make(map[string][]jsonhost)

	prevServer := ""
	for rows.Next() {
		var tmpServer, tmpVolume, tmpAccess, tmpHost sql.NullString
		rows.Scan(&tmpServer, &tmpVolume, &tmpAccess, &tmpHost)

		if tmpVolume.Valid {
			if prevServer != "" && prevServer != tmpServer.String {
				out[prevServer] = append(out[prevServer], entry)
				entry = make(map[string][]jsonhost)
			}
			entry[tmpVolume.String] = append(entry[tmpVolume.String], jsonhost{
				Host: tmpHost.String,
				AccessLevel: tmpAccess.String,
			})
		}
		prevServer = tmpServer.String
	}
	if prevServer != "" {
		out[prevServer] = append(out[prevServer], entry)
	}

	return out, nil
}

func createComputeResource(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	shell := i[Shell].Default("/sbin/nologin")

	unitid := NewNullAttribute(UnitID)
	compid := NewNullAttribute(ResourceID)
	err := c.DBtx.QueryRow(`select (select unitid from affiliation_units where name = $1),
								   (select compid from compute_resources where name = $2)`,
						   i[UnitName], i[ResourceName]).Scan(&unitid, &compid)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if compid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDuplicateData, ResourceName))
	}
	if !unitid.Valid && i[UnitName].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
	}
	if i[ResourceType].AbsoluteNull {
		apiErr = append(apiErr, DefaultAPIError(ErrorInvalidData, ResourceType))
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`insert into compute_resources (name, default_shell, unitid, last_updated, default_home_dir, type)
						  values ($1, $2, $3, NOW(), $4, $5)`,
						 i[ResourceName], shell, unitid, i[HomeDir], i[ResourceType])
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, nil
}

func setComputeResourceInfo(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	unitid := NewNullAttribute(UnitID)
	compid := NewNullAttribute(ResourceID)
	err := c.DBtx.QueryRow(`select (select unitid from affiliation_units where name = $1),
								   (select compid from compute_resources where name = $2)`,
						   i[UnitName], i[ResourceName]).Scan(&unitid, &compid)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !compid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, ResourceName))
	}
	if !unitid.Valid && i[UnitName].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
	}
	if i[ResourceType].AbsoluteNull {
		apiErr = append(apiErr, DefaultAPIError(ErrorInvalidData, ResourceType))
	}
	if !i[UnitName].Valid && !i[Shell].Valid && !i[HomeDir].Valid && !i[ResourceType].Valid {
		apiErr = append(apiErr, APIError{errors.New("not enough arguments"), ErrorAPIRequirement})
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`update compute_resources set
							unitid = coalesce($1, unitid),
							default_shell = coalesce($2, default_shell),
							default_home_dir = coalesce($3, default_home_dir),
							type = coalesce($4, type),
							last_updated = NOW()
						  where compid = $5`, unitid, i[Shell], i[HomeDir], i[ResourceType], compid)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, nil
}

func createStorageResource(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	storageid := NewNullAttribute(ResourceID)
	err := c.DBtx.QueryRow(`select storageid from storage_resources where name = $1`,
						   i[ResourceName]).Scan(&storageid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if storageid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDuplicateData, ResourceName))
	}
	if i[ResourceType].AbsoluteNull {
		apiErr = append(apiErr, DefaultAPIError(ErrorInvalidData, ResourceType))
	}
	if i[QuotaUnit].Valid && !checkUnits(i[QuotaUnit].Data.(string)) {
		apiErr = append(apiErr, APIError{fmt.Errorf("allowed quotaunit values are B,KB,KIB,MB,MIB,GB,GIB,TB,TIB"), ErrorAPIRequirement})
	}
	if i[Quota].Valid != i[QuotaUnit].Valid {
		apiErr = append(apiErr, APIError{fmt.Errorf("quota requires quotaunit"), ErrorAPIRequirement})
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`insert into storage_resources (name, default_path, default_quota, last_updated, default_unit, type)
						  values ($1,$2,$3,NOW(),$4,$5)`,
						i[ResourceName], i[Path], i[Quota], i[QuotaUnit], i[ResourceType])
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, nil
}

func getStorageResourceInfo(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	type jsonerror struct {
		Error string `json:"ferry_error"`
	}

	name := q.Get("resourcename")

	if name == "" {
		name = "%"
	}

	rows, err := DBptr.Query(`select name, default_path, default_quota, default_unit, type from storage_resources where name like $1 order by name`, name)
	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Error(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
		return
	}
	defer rows.Close()

	type jsonentry struct {
		Name  string `json:"name"`
		Path  string `json:"default_path"`
		Quota string `json:"default_quota"`
		Unit  string `json:"default_unit"`
		Type  string `json:"type"`
	}
	var Entry jsonentry
	var Out []jsonentry

	for rows.Next() {
		rows.Scan(&Entry.Name, &Entry.Path, &Entry.Quota, &Entry.Unit, &Entry.Type)

		if Entry.Name != "" {
			Out = append(Out, Entry)
		}
	}

	var output interface{}
	if len(Out) == 0 {
		var queryErr []jsonerror
		log.WithFields(QueryFields(r, startTime)).Error("No storage resources found for this query.")
		queryErr = append(queryErr, jsonerror{"No storage resources found for this query."})
		output = queryErr
	} else {
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		output = Out
	}
	jsonout, err := json.Marshal(output)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
	}
	fmt.Fprintf(w, string(jsonout))
}

func setStorageResourceInfo(w http.ResponseWriter, r *http.Request) {

	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	rName := strings.TrimSpace(q.Get("resourcename"))
	defunit := strings.TrimSpace(strings.ToUpper(q.Get("default_unit")))
	rType := strings.TrimSpace(strings.ToLower(q.Get("type")))

	defpath := strings.TrimSpace(q.Get("default_path"))
	defquota := strings.TrimSpace(q.Get("default_quota"))

	if rName == "" {
		log.WithFields(QueryFields(r, startTime)).Print("No resource name specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No resourcename specified.\" }")
		return
	}
	if rType == "null" {
		log.WithFields(QueryFields(r, startTime)).Print("'null' is an invalid resource type.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"Resource type of null is not allowed.\" }")
		return
	}

	//require auth
	authorized, authout := authorize(r)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"ferry_error\": \""+authout+"not authorized.\" }")
		return
	}

	var (
		nullpath, nullunit sql.NullString
		nullquota          sql.NullInt64
		currentType        string
		storageid          int
	)
	// check if resource exists and grab existing values of everything if so
	err := DBptr.QueryRow(`select distinct storageid, default_path, default_quota, default_unit, type from storage_resources where name=$1`, rName).Scan(&storageid, &nullpath, &nullquota, &nullunit, &currentType)

	switch {
	case err == sql.ErrNoRows:
		// nothing returned from the select, so the resource does not exist.
		log.WithFields(QueryFields(r, startTime)).Print("Storage resource with name " + rName + " not found in compute_resources table. Exiting.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"resource does not exist. Use createStorageResource to add a new resource.\" }")
		return
	case err != nil:
		w.WriteHeader(http.StatusNotFound)
		log.WithFields(QueryFields(r, startTime)).Print("Error in DB query: " + err.Error())
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
		return
	default:

		//actually change stuff
		// if you specfied a new type in the API call (not NULL, as checked for earlier), change it here. Otherwise we keep the existing one
		if rType != "" {
			currentType = rType
		}
		// if you are changing the default path, do it here. Variations of "NULL" as the string will assume you want it to be null in the database. If you did not specify shell in the query, then we keep the existing value.
		if defpath != "" {
			if strings.ToUpper(defpath) != "NULL" {
				nullpath.Valid = true
				nullpath.String = defpath
			} else {
				nullpath.Valid = false
				nullpath.String = ""
			}
		}

		// and the same for default_home_dir, following the same rule as path.
		if defquota != "" {
			if strings.ToUpper(defquota) != "NULL" {
				nullquota.Valid = true
				convquota, converr := strconv.ParseInt(defquota, 10, 64)
				if converr != nil {
					log.WithFields(QueryFields(r, startTime)).Error("Error converting default_quota to int: " + converr.Error())
					fmt.Fprintf(w, "{ \"ferry_error\": \"Error converting default_quota to int. Check format.\" }")
					return
				}
				nullquota.Int64 = convquota
			} else {
				nullquota.Valid = false
			}
		}

		// if you specified a new default unit, change it, following the same rule as path.
		if defunit != "" {
			if strings.ToUpper(defunit) != "NULL" {
				if checkUnits(defunit) {
					nullunit.Valid = true
					nullunit.String = strings.ToUpper(defunit)
				} else {
					log.WithFields(QueryFields(r, startTime)).Error("Invalid value for default unit. Allowed values are B,KB,KIB,MB,MIB,GB,GIB,TB,TIB.)")
					fmt.Fprintf(w, "{ \"ferry_error\": \"Invalid value for default_unit.\" }")
					return
				}
			} else {
				nullunit.Valid = false
				nullunit.String = ""
			}
		} // end if unitName != ""

		//transaction start, and update command
		DBtx, cKey, err := LoadTransaction(r, DBptr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Error starting DB transaction: " + err.Error())
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w, "{ \"ferry_error\": \"Error starting database transaction.\" }")
			return
		}
		defer DBtx.Rollback(cKey)

		_, commerr := DBtx.Exec(`update storage_resources set default_path=$1, default_quota=$2, last_updated=NOW(), default_unit=$3, type=$4 where name=$5`, nullpath, nullquota, nullunit, currentType, rName)
		if commerr != nil {
			w.WriteHeader(http.StatusNotFound)
			log.WithFields(QueryFields(r, startTime)).Error("Error during DB update: " + commerr.Error())
			fmt.Fprintf(w, "{ \"ferry_error\": \"Database error during update.\" }")
			return
		} else {
			// if no error, commit and all that
			DBtx.Commit(cKey)
			log.WithFields(QueryFields(r, startTime)).Info("Successfully updated " + rName + ".")
			fmt.Fprintf(w, "{ \"ferry_status\": \"success\" }")
		}
	} //end switch

}

func getAllComputeResources(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	lastupdate, parserr := stringToParsedTime(strings.TrimSpace(q.Get("last_updated")))
	if parserr != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error parsing provided update time: " + parserr.Error())
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error parsing last_updated time. Check ferry logs. If provided, it should be an integer representing an epoch time.\"}")
		return
	}

	rows, err := DBptr.Query(`select cr.name, cr.default_shell, cr.default_home_dir, cr.type, au.name as affiliation_unit
							  from compute_resources as cr left join affiliation_units as au on cr.unitid = au.unitid where (cr.last_updated>=$1 or $1 is null);`, lastupdate)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		log.WithFields(QueryFields(r, startTime)).Error("Error in DB query: " + err.Error())
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
		return
	}
	defer rows.Close()

	type computeResource struct {
		Name  string `json:"name"`
		Shell string `json:"default_shell"`
		Home  string `json:"default_home_dir"`
		Type  string `json:"type"`
		Unit  string `json:"affiliation_unit"`
	}
	var Out []computeResource
	var tmpName, tmpShell, tmpHome, tmpType, tmpUnit sql.NullString

	for rows.Next() {
		rows.Scan(&tmpName, &tmpShell, &tmpHome, &tmpType, &tmpUnit)
		Out = append(Out, computeResource{tmpName.String, tmpShell.String, tmpHome.String,
			tmpType.String, tmpUnit.String})
	}

	var output interface{}
	if len(Out) == 0 {
		type jsonerror struct {
			Error string `json:"ferry_error"`
		}
		var queryErr []jsonerror
		queryErr = append(queryErr, jsonerror{"Query returned no compute resources."})
		log.WithFields(QueryFields(r, startTime)).Error("Query returned no compute resources.")
		output = queryErr
	} else {
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		output = Out
	}
	jsonoutput, err := json.Marshal(output)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err.Error())
	}
	fmt.Fprintf(w, string(jsonoutput))
}

func ping(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	fmt.Fprintf(w, `[{ "ferry_status": "success"}, { "release_version" : "`+release_ver+`"}, {"build_date" : "`+build_date+`"}]`)
	return
}

func getVOUserMap(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	user := strings.TrimSpace(q.Get("username"))
	unit := strings.TrimSpace(q.Get("unitname"))
	fqan := strings.TrimSpace(q.Get("fqan"))

	if user == "" {
		user = "%"
	}
	if unit == "" {
		unit = "%"
	}
	if fqan == "" {
		fqan = "%"
	}

	var userExists, unitExists, fqanExists bool
	err := DBptr.QueryRow(`SELECT
							$1 IN (SELECT uname FROM users),
							$2 IN (SELECT name FROM affiliation_units),
							$3 IN (SELECT fqan FROM grid_fqan)`,
		user, unit, fqan).Scan(&userExists, &unitExists, &fqanExists)
	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Error(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
		return
	}

	type jsonerror struct {
		Error string `json:"ferry_error"`
	}
	var inputErr []jsonerror

	if !userExists && user != "%" {
		log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
		inputErr = append(inputErr, jsonerror{"User does not exist."})
	}
	if !unitExists && unit != "%" {
		log.WithFields(QueryFields(r, startTime)).Error("Affiliation unit does not exist.")
		inputErr = append(inputErr, jsonerror{"Affiliation unit does not exist."})
	}
	if !fqanExists && fqan != "%" {
		log.WithFields(QueryFields(r, startTime)).Error("FQAN does not exist.")
		inputErr = append(inputErr, jsonerror{"FQAN does not exist."})
	}

	if len(inputErr) > 0 {
		jsonout, err := json.Marshal(inputErr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error(err)
		}
		fmt.Fprintf(w, string(jsonout))
		return
	}

	rows, err := DBptr.Query(`SELECT DISTINCT
								u1.uname AS username,
								au.name AS unitname,
								gf.fqan AS fqan,
								COALESCE(u2.uname, u1.uname) AS mapped_user
							  FROM     
								grid_access g 
								INNER JOIN users u1 ON g.uid = u1.uid 
								INNER JOIN grid_fqan gf ON gf.fqanid = g.fqanid 
								INNER JOIN affiliation_units au ON au.unitid = gf.unitid
								LEFT OUTER JOIN users u2 ON gf.mapped_user = u2.uid 
							  WHERE 
								u1.uname like $1 
								AND au.name like $2
								AND gf.fqan like $3
							  ORDER BY u1.uname`, user, unit, fqan)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		log.WithFields(QueryFields(r, startTime)).Error("Error in DB query: " + err.Error())
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
		return
	}
	defer rows.Close()

	voUserMap := make(map[string]map[string]map[string]string)

	for rows.Next() {
		var tmpUser, tmpUnit, tmpFQAN, tmpMappedUser sql.NullString
		rows.Scan(&tmpUser, &tmpUnit, &tmpFQAN, &tmpMappedUser)

		if tmpUser.Valid {
			if _, ok := voUserMap[tmpUser.String]; !ok {
				voUserMap[tmpUser.String] = make(map[string]map[string]string)
			}
			if _, ok := voUserMap[tmpUser.String][tmpUnit.String]; !ok {
				voUserMap[tmpUser.String][tmpUnit.String] = make(map[string]string)
			}
			voUserMap[tmpUser.String][tmpUnit.String][tmpFQAN.String] = tmpMappedUser.String
		}
	}

	var output interface{}
	if len(voUserMap) == 0 {
		var queryErr []jsonerror
		log.WithFields(QueryFields(r, startTime)).Error("No mappings found.")
		queryErr = append(queryErr, jsonerror{"No mappings found."})
		output = queryErr
	} else {
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		output = voUserMap
	}
	jsonout, err := json.Marshal(output)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
	}
	fmt.Fprintf(w, string(jsonout))
}

func cleanStorageQuotas(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

	DBtx, cKey, err := LoadTransaction(r, DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error starting DB transaction: " + err.Error())
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error starting database transaction.\" }")
		return
	}
	defer DBtx.Rollback(cKey)

	_, err = DBtx.Exec(`DO $$
					   	BEGIN

						   	UPDATE storage_quota AS q
						   	SET last_updated = t.valid_until
						   	FROM (SELECT q.quotaid, tmp.valid_until
								  FROM (SELECT *
									    FROM storage_quota
									    WHERE valid_until < NOW())
								  AS tmp
								  JOIN storage_quota AS q
								  ON ((q.uid, q.storageid) = (tmp.uid, tmp.storageid)
									  OR (q.groupid, q.storageid) = (tmp.groupid, tmp.storageid))
									  AND q.valid_until is null)
							AS t
							WHERE q.quotaid = t.quotaid;

						   	DELETE FROM storage_quota
						   	WHERE valid_until < NOW();

					   	END $$;`)
	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Error(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
		return
	}

	DBtx.Commit(cKey)

	log.WithFields(QueryFields(r, startTime)).Info("Success!")
	fmt.Fprintf(w, "{ \"ferry_status\": \"success\" }")
}

func cleanCondorQuotas(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

	DBtx, cKey, err := LoadTransaction(r, DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error starting DB transaction: " + err.Error())
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error starting database transaction.\" }")
		return
	}
	defer DBtx.Rollback(cKey)

	_, err = DBtx.Exec(`DO $$
					   	BEGIN

						   	UPDATE compute_batch AS q
						   	SET last_updated = t.valid_until
						   	FROM (SELECT q.batchid, tmp.valid_until
								  FROM (SELECT *
									    FROM compute_batch
									    WHERE valid_until < NOW())
								  AS tmp
								  JOIN compute_batch AS q
								  ON q.name = tmp.name
								  AND q.valid_until is null)
							AS t
							WHERE q.batchid = t.batchid;

						   	DELETE FROM compute_batch
						   	WHERE valid_until < NOW();

					   	END $$;`)
	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Error(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
		return
	}

	DBtx.Commit(cKey)

	log.WithFields(QueryFields(r, startTime)).Info("Success!")
	fmt.Fprintf(w, "{ \"ferry_status\": \"success\" }")
}

func setStorageQuota(c APIContext, i Input) (interface{}, []APIError) {
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
									 where storageid = $1 and `+column+` = $2 and
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

	c.DBtx.Exec(`insert into storage_quota (storageid, `+column+`, unitid, value, unit, valid_until, path, last_updated)
				values ($1, $2, $3, $4, $5, $6, $7, NOW())
				on conflict (storageid, `+column+`) where valid_until is `+tmpNull+`null
				do update set value = $4, unit = $5, valid_until = $6, path = $7, last_updated = NOW()`,
		vStorageid, vDataid, vUnitid, quota, unit, i[ExpirationDate], vPath)
	if !i[ExpirationDate].Valid {
		c.DBtx.Exec(`delete from storage_quota where storageid = $1 and `+column+` = $2 and valid_until is not null`, vStorageid, vDataid)
	}

	if c.DBtx.Error() != nil {
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	return nil, nil
}
