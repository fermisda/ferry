package main

import (
	"database/sql"
	"errors"
	"fmt"
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
		RolePublic,
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
		RoleWrite,
	}
	c.Add("setStorageQuota", &setStorageQuota)

	getGroupGID := BaseAPI{
		InputModel{
			Parameter{GroupName, true},
		},
		getGroupGID,
		RoleRead,
	}
	c.Add("getGroupGID", &getGroupGID)

	getGroupFile := BaseAPI{
		InputModel{
			Parameter{UnitName, false},
			Parameter{ResourceName, false},
			Parameter{LastUpdated, false},
		},
		getGroupFile,
		RoleRead,
	}
	c.Add("getGroupFile", &getGroupFile)

	getGridMapFile := BaseAPI{
		InputModel{
			Parameter{UnitName, false},
			Parameter{ResourceName, false},
			Parameter{LastUpdated, false},
			Parameter{JWT, false},
			Parameter{Status, false},
		},
		getGridMapFile,
		RoleRead,
	}
	c.Add("getGridMapFile", &getGridMapFile)

	getGridMapFileByVO := BaseAPI{
		InputModel{
			Parameter{UnitName, false},
			Parameter{LastUpdated, false},
			Parameter{JWT, false},
		},
		getGridMapFileByVO,
		RoleRead,
	}
	c.Add("getGridMapFileByVO", &getGridMapFileByVO)

	getVORoleMapFile := BaseAPI{
		InputModel{
			Parameter{ResourceName, false},
			Parameter{LastUpdated, false},
		},
		getVORoleMapFile,
		RoleRead,
	}
	c.Add("getVORoleMapFile", &getVORoleMapFile)

	getGroupName := BaseAPI{
		InputModel{
			Parameter{GID, true},
		},
		getGroupName,
		RoleRead,
	}
	c.Add("getGroupName", &getGroupName)

	lookupCertificateDN := BaseAPI{
		InputModel{
			Parameter{DN, true},
		},
		lookupCertificateDN,
		RoleWrite,
	}
	c.Add("lookupCertificateDN", &lookupCertificateDN)

	getMappedGidFile := BaseAPI{
		InputModel{},
		getMappedGidFile,
		RoleRead,
	}
	c.Add("getMappedGidFile", &getMappedGidFile)

	getStorageAuthzDBFile := BaseAPI{
		InputModel{
			Parameter{PasswdMode, false},
			Parameter{LastUpdated, false},
		},
		getStorageAuthzDBFile,
		RoleRead,
	}
	c.Add("getStorageAuthzDBFile", &getStorageAuthzDBFile)

	getAffiliationMembersRoles := BaseAPI{
		InputModel{
			Parameter{UnitName, false},
			Parameter{Role, false},
		},
		getAffiliationMembersRoles,
		RoleRead,
	}
	c.Add("getAffiliationMembersRoles", &getAffiliationMembersRoles)

	createComputeResource := BaseAPI{
		InputModel{
			Parameter{ResourceName, true},
			Parameter{ResourceType, true},
			Parameter{HomeDir, true},
			Parameter{Shell, false},
			Parameter{UnitName, false},
		},
		createComputeResource,
		RoleWrite,
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
		RoleWrite,
	}
	c.Add("setComputeResourceInfo", &setComputeResourceInfo)

	createStorageResource := BaseAPI{
		InputModel{
			Parameter{ResourceName, true},
			Parameter{ResourceType, true},
			Parameter{Quota, false},
			Parameter{QuotaUnit, false},
			Parameter{Path, false},
		},
		createStorageResource,
		RoleWrite,
	}
	c.Add("createStorageResource", &createStorageResource)

	setStorageResourceInfo := BaseAPI{
		InputModel{
			Parameter{ResourceName, true},
			Parameter{ResourceType, false},
			Parameter{Quota, false},
			Parameter{QuotaUnit, false},
			Parameter{Path, false},
		},
		setStorageResourceInfo,
		RoleWrite,
	}
	c.Add("setStorageResourceInfo", &setStorageResourceInfo)

	getStorageResourceInfo := BaseAPI{
		InputModel{
			Parameter{ResourceName, false},
		},
		getStorageResourceInfo,
		RoleRead,
	}
	c.Add("getStorageResourceInfo", &getStorageResourceInfo)

	getAllComputeResources := BaseAPI{
		InputModel{
			Parameter{ResourceType, false},
			Parameter{LastUpdated, false},
		},
		getAllComputeResources,
		RoleRead,
	}
	c.Add("getAllComputeResources", &getAllComputeResources)

	getVOUserMap := BaseAPI{
		InputModel{
			Parameter{UserName, false},
			Parameter{UnitName, false},
			Parameter{FQAN, false},
		},
		getVOUserMap,
		RoleRead,
	}
	c.Add("getVOUserMap", &getVOUserMap)

	getPasswdFile := BaseAPI{
		InputModel{
			Parameter{Status, false},
			Parameter{UnitName, false},
			Parameter{ResourceName, false},
			Parameter{LastUpdated, false},
		},
		getPasswdFile,
		RoleRead,
	}
	c.Add("getPasswdFile", &getPasswdFile)

	ping := BaseAPI{
		nil,
		ping,
		RolePublic,
	}
	c.Add("ping", &ping)

	cleanStorageQuotas := BaseAPI{
		nil,
		cleanStorageQuotas,
		RoleWrite,
	}
	c.Add("cleanStorageQuotas", &cleanStorageQuotas)

	cleanCondorQuotas := BaseAPI{
		nil,
		cleanCondorQuotas,
		RoleWrite,
	}
	c.Add("cleanCondorQuotas", &cleanCondorQuotas)
}

// getPasswdFile godoc
// @Summary      Returns the contents for a passwd file with all the members of an affiliation unit.
// @Description  Returns the contents for a passwd file with all the members of an affiliation unit.
// @Tags         Authorization Queries
// @Accept       html
// @Produce      json
// @Param        lastupdated    query     string  false  "limit results to records  updated since"  Format(date)
// @Param        resourcename   query     string  false  "compute resource to return passwd file data for"
// @Param        status         query     boolean false  "return only those with the specified status, default all"  Format(true/false)
// @Param        unitname       query     string  false  "affiliation to return passwd file data for""
// @Success      200  {object}  miscUserPasswdGrps
// @Failure      400  {object}  jsonOutput
// @Failure      401  {object}  jsonOutput
// @Router /getPasswdFile [get]
func getPasswdFile(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	unitid := NewNullAttribute(UnitID)
	compid := NewNullAttribute(ResourceID)
	err := c.DBtx.QueryRow(`select (select unitid from affiliation_units where name = $1),
								   (select compid from compute_resources where name = $2)`,
		i[UnitName], i[ResourceName]).Scan(&unitid, &compid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
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

	rows, err := c.DBtx.Query(`select au.name, cr.name, uname, uid, gid, full_name, home_dir, shell, cag.last_updated
							   from compute_access_group as cag
                               left join compute_access as ca using(compid, uid)
                               join groups as g using(groupid)
                               join compute_resources as cr using(compid)
                        	   left join affiliation_units as au using(unitid)
                               join users as u using(uid)
							   where cag.is_primary = true
							   and (unitid = $1 or $1 is null)
							   and (compid = $2 or $2 is null)
							   and (ca.last_updated>=$3 or u.last_updated>=$3
								or au.last_updated>=$3 or cr.last_updated>=$3
								or g.last_updated>=$3 or $3 is null)
							   and (u.status = $4 or $4 is null)
							   order by au.name, cr.name`, unitid, compid, i[LastUpdated], i[Status])
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	const GECOS Attribute = "gecos"
	const Resources Attribute = "resources"
	type jsonmap map[Attribute]interface{}

	out := make(map[string]jsonmap)

	lastTime := int64(0)
	prevUname := NewNullAttribute(UnitName)
	prevRname := NewNullAttribute(ResourceName)
	tmpResources := make(map[string][]jsonmap, 0)
	tmpUsers := make([]jsonmap, 0)
	for rows.Next() {
		row := NewMapNullAttribute(UnitName, ResourceName, UserName, UID, GID, FullName, HomeDir, Shell, LastUpdated)
		rows.Scan(row[UnitName], row[ResourceName], row[UserName], row[UID], row[GID], row[FullName], row[HomeDir], row[Shell], row[LastUpdated])

		if !row[ResourceName].Valid {
			continue
		}

		*row[UnitName] = row[UnitName].Default("null")

		if !prevRname.Valid {
			prevRname = *row[ResourceName]
		}
		if !prevUname.Valid {
			prevUname = *row[UnitName]
		}

		if row[ResourceName].Valid {
			if prevRname != *row[ResourceName] {
				tmpResources[prevRname.Data.(string)] = tmpUsers
				tmpUsers = make([]jsonmap, 0)
				prevRname = *row[ResourceName]
			}
			if prevUname != *row[UnitName] {
				out[prevUname.Data.(string)] = jsonmap{
					Resources:   tmpResources,
					LastUpdated: lastTime,
				}
				tmpResources = make(map[string][]jsonmap, 0)
				lastTime = 0
				prevUname = *row[UnitName]

			}
			if row[LastUpdated].Valid {
				if lastTime == 0 || (row[LastUpdated].Data.(time.Time).Unix() > lastTime) {
					lastTime = row[LastUpdated].Data.(time.Time).Unix()
				}
			}
			tmpUsers = append(tmpUsers, jsonmap{
				UserName: row[UserName].Data,
				UID:      row[UID].Data,
				GID:      row[GID].Data,
				GECOS:    row[FullName].Data,
				HomeDir:  row[HomeDir].Data,
				Shell:    row[Shell].Data,
			})
		}
	}
	if prevUname.Valid || prevUname.AbsoluteNull {
		tmpResources[prevRname.Data.(string)] = tmpUsers
		out[prevUname.Data.(string)] = jsonmap{
			Resources:   tmpResources,
			LastUpdated: lastTime,
		}
	}

	return out, nil
}

// getGroupFile godoc
// @Summary      Returns the contents for a group file for a compute resource assigned to an affiliation unit.
// @Description  Returns the contents for a group file for a compute resource assigned to an affiliation unit.
// @Tags         Authorization Queries
// @Accept       html
// @Produce      json
// @Param        lastupdated    query     string  false  "limit results to records  updated since"  Format(date)
// @Param        resourcename   query     string  false  "compute resource to return group file data for"
// @Param        unitname       query     string  false  "affiliation to return group file data for""
// @Success      200  {object}  miscGroupFile
// @Failure      400  {object}  jsonOutput
// @Failure      401  {object}  jsonOutput
// @Router /getGroupFile [get]
func getGroupFile(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	unitid := NewNullAttribute(UnitID)
	compid := NewNullAttribute(ResourceID)
	err := c.DBtx.QueryRow(`select (select unitid from affiliation_units where name = $1),
								   (select compid from compute_resources where name = $2)`,
		i[UnitName], i[ResourceName]).Scan(&unitid, &compid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
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

	rows, err := c.DBtx.Query(`select g.name, gid, uname, is_primary, cg.last_updated
	                                from compute_access_group cg
									join compute_resources using (compid)
									join groups g using(groupid)
									join users using(uid)
								   where (unitid = $1 or $1 is null) and (compid = $2 or $2 is null)
										  and (g.type = 'UnixGroup') and (cg.last_updated>=$3 or $3 is null)
								   order by name, uname`,
		unitid, compid, i[LastUpdated])
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	type jsongroup map[Attribute]interface{}
	out := make([]jsongroup, 0)
	entry := make(jsongroup)

	const Users Attribute = "users"
	users := make([]interface{}, 0)

	lastTime := NewNullAttribute(LastUpdated)
	prevGname := NewNullAttribute(GroupName)
	prevUname := NewNullAttribute(UserName)
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
				prevUname.Data = " "
			}
			if !row[Primary].Data.(bool) && prevUname != *row[UserName] {
				users = append(users, row[UserName].Data)
				prevUname = *row[UserName]
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

// getGridMapFile godoc
// @Summary      Returns the contents for a gridmap file for a specific experiment and/or a group.
// @Description  Returns the contents for a gridmap file for a specific experiment and/or a group.
// @Tags         Authorization Queries
// @Accept       html
// @Produce      json
// @Param        jwt            query     string  false  "When exists, output the new token supporting format"
// @Param        lastupdated    query     string  false  "limit results to records  updated since"  Format(date)
// @Param        resourcename   query     string  false  "compute resource to return gridmap file data for"
// @Param        unitname       query     string  false  "affiliation to return gridmap file data for"
// @Param        status         query     bool    false  "when set, limits output to the chosen status"
// @Success      200  {object}  miscGridMapFile
// @Failure      400  {object}  jsonOutput
// @Failure      401  {object}  jsonOutput
// @Router /getGridMapFile [get]
func getGridMapFile(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	// Call the new JWT based code which will replace the older method -- at some point.
	if i[JWT].Valid {
		return getJWTGridMapFile(c, i)
	}

	unitid := NewNullAttribute(UnitID)
	compid := NewNullAttribute(ResourceID)
	err := c.DBtx.QueryRow(`select (select unitid from affiliation_units where name = $1),
								   (select compid from compute_resources where name = $2)`,
		i[UnitName], i[ResourceName]).Scan(&unitid, &compid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
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

	rows, err := c.DBtx.Query(`select distinct dn, uname, token_subject, status
								from affiliation_unit_user_certificate as ac
								left join user_certificates as uc using(dnid)
								left join users as us using(uid)
								left join compute_access as ca using(uid)
							   where (unitid = $1 or $1 is null)
							     and (compid = $2 or $2 is null)
								 and (ac.last_updated>=$3 or uc.last_updated>=$3 or us.last_updated>=$3 or $3 is null)
								 and (us.status = $4 or $4 is null)`,
		unitid, compid, i[LastUpdated], i[Status])
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	type jsondnmap map[Attribute]interface{}
	var out []jsondnmap

	for rows.Next() {
		row := NewMapNullAttribute(DN, UserName, TokenSubject, Status)
		rows.Scan(row[DN], row[UserName], row[TokenSubject], row[Status])
		if row[DN].Valid {
			out = append(out, jsondnmap{
				DN:           row[DN].Data,
				UserName:     row[UserName].Data,
				TokenSubject: row[TokenSubject].Data,
				Status:       row[Status].Data,
			})
		}
	}

	return out, nil
}

// Replacement for getGridMapFile.  When it will replace it?  Now there's the question.
func getJWTGridMapFile(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	unitid := NewNullAttribute(UnitID)
	compid := NewNullAttribute(ResourceID)
	err := c.DBtx.QueryRow(`select (select unitid from affiliation_units where name = $1),
								   (select compid from compute_resources where name = $2)`,
		i[UnitName], i[ResourceName]).Scan(&unitid, &compid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
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

	rows, err := c.DBtx.Query(`select distinct dn as subject, 'dn' as type, uname, status
							   from affiliation_unit_user_certificate as ac
								 left join user_certificates as uc using(dnid)
								 left join users as us using(uid)
								 left join compute_access as ca using(uid)
							   where dn not like '/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=%/CN=UID:%'
							     and (unitid = $1 or $1 is null)
							     and (compid = $2 or $2 is null)
								 and (ac.last_updated>=$3 or uc.last_updated>=$3 or us.last_updated>=$3 or $3 is null)
								 and (us.status = $4 or $4 is null)
							   UNION
							   select distinct cast(token_subject as text) as subject, 'jwt' as type, uname, status
							   from user_affiliation_units as uac
								 left join users as us using(uid)
								 left join compute_access as ca using(uid)
							   where (unitid = $1 or $1 is null)
							     and (compid = $2 or $2 is null)
								 and (uac.last_updated>=$3 or us.last_updated>=$3 or $3 is null)
								 and (us.status = $4 or $4 is null)`,
		unitid, compid, i[LastUpdated], i[Status])
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	type jsondnmap map[Attribute]interface{}
	var out []jsondnmap

	for rows.Next() {
		row := NewMapNullAttribute(Subject, SubjectType, UserName, Status)
		rows.Scan(row[Subject], row[SubjectType], row[UserName], row[Status])
		out = append(out, jsondnmap{
			Subject:     row[Subject].Data,
			SubjectType: row[SubjectType].Data,
			UserName:    row[UserName].Data,
			Status:      row[Status].Data,
		})
	}

	return out, nil
}

// getGridMapFileByVO godoc
// @Summary      Returns the contents for a gridmap file for a specific experiment and/or a group.
// @Description  Returns the contents for a gridmap file for a specific experiment and/or a group.
// @Tags         Authorization Queries
// @Accept       html
// @Produce      json
// @Param        jwt            query     string  false  "When exists, uses the new token supporting format"
// @Param        lastupdated    query     string  false  "limit results to records  updated since"  Format(date)
// @Param        unitname       query     string  false  "affiliation to return gridmap file data for""
// @Success      200  {object}  miscGridMapFileByVO
// @Failure      400  {object}  jsonOutput
// @Failure      401  {object}  jsonOutput
// @Router /getGridMapFileByVO [get]
func getGridMapFileByVO(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	// Call the new JWT based code which will replace the older method -- at some point.
	if i[JWT].Valid {
		return getJWTGridMapFileByVO(c, i)
	}

	unitid := NewNullAttribute(UnitID)
	err := c.DBtx.QueryRow(`select unitid from affiliation_units where name = $1`, i[UnitName]).Scan(&unitid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !unitid.Valid && i[UnitName].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
		return nil, apiErr
	}

	rows, err := c.DBtx.Query(`select name, dn, uname, token_subject, status
								from  affiliation_unit_user_certificate as ac
								left join user_certificates as uc using(dnid)
								left join users as us using(uid)
								left join affiliation_units as au using(unitid)
								where (unitid = $1 or $1 is null) and (ac.last_updated >= $2 or uc.last_updated >= $2 or
									   us.last_updated >= $2 or au.last_updated >= $2 or $2 is null)`,
		unitid, i[LastUpdated])
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	type jsondnmap map[Attribute]interface{}
	out := make(map[string][]jsondnmap)

	for rows.Next() {
		row := NewMapNullAttribute(UnitName, DN, UserName, TokenSubject, Status)
		rows.Scan(row[UnitName], row[DN], row[UserName], row[TokenSubject], row[Status])
		if row[DN].Valid {
			out[row[UnitName].Data.(string)] = append(out[row[UnitName].Data.(string)], jsondnmap{
				DN:           row[DN].Data,
				UserName:     row[UserName].Data,
				TokenSubject: row[TokenSubject].Data,
				Status:       row[Status].Data,
			})
		}
	}

	return out, nil
}

// Replacement for getGridMapFileByVO.  When it will replace it?  Now there's the question.
func getJWTGridMapFileByVO(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	unitid := NewNullAttribute(UnitID)
	err := c.DBtx.QueryRow(`select unitid from affiliation_units where name = $1`, i[UnitName]).Scan(&unitid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !unitid.Valid && i[UnitName].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
		return nil, apiErr
	}

	rows, err := c.DBtx.Query(`select name, dn as subject, 'dn' as type, uname, status
							   from affiliation_unit_user_certificate as ac
								 left join user_certificates as uc using(dnid)
								 left join users as us using(uid)
								 left join affiliation_units as au using(unitid)
							   where dn not like '/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=%/CN=UID:%'
							      and (unitid = $1 or $1 is null)
								  and (ac.last_updated >= $2 or uc.last_updated >= $2
								       or us.last_updated >= $2 or au.last_updated >= $2 or $2 is null)
								  and (us.status = $3 or $3 is null)
							   UNION
							   select name, token_subject as subject, 'jwt' as type, uname, status
							   from user_affiliation_units as uac
								  left join users as us using(uid)
								  left join affiliation_units as au using(unitid)
							   where (unitid = $1 or $1 is null)
								  and (uac.last_updated>=$2 or us.last_updated>=$2 or $2 is null)
								  and (us.status = $3 or $3 is null)`,
		unitid, i[LastUpdated], i[Status])
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	type jsondnmap map[Attribute]interface{}
	out := make(map[string][]jsondnmap)

	for rows.Next() {
		row := NewMapNullAttribute(UnitName, Subject, SubjectType, UserName, Status)
		rows.Scan(row[UnitName], row[Subject], row[SubjectType], row[UserName], row[Status])
		out[row[UnitName].Data.(string)] = append(out[row[UnitName].Data.(string)], jsondnmap{
			Subject:     row[Subject].Data,
			SubjectType: row[SubjectType].Data,
			UserName:    row[UserName].Data,
			Status:      row[Status].Data,
		})
	}

	return out, nil
}

// getVORoleMapFile godoc
// @Summary      Returns the contents for a grid-vorolemap file for a specific experiment and/or a group.
// @Description  Returns the contents for a grid-vorolemap file for a specific experiment and/or a group.
// @Tags         Authorization Queries
// @Accept       html
// @Produce      json
// @Param        lastupdated    query     string  false  "limit results to records  updated since"  Format(date)
// @Param        resourcename   query     string  false  "compute resource to return gridmap file data for"
// @Success      200  {object}  miscVORoleMapFile
// @Failure      400  {object}  jsonOutput
// @Failure      401  {object}  jsonOutput
// @Router /getVORoleMapFile [get]
func getVORoleMapFile(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	compid := NewNullAttribute(ResourceID)
	err := c.DBtx.QueryRow(`select compid from compute_resources where name = $1`, i[ResourceName]).Scan(&compid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !compid.Valid && i[ResourceName].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, ResourceName))
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
		log.WithFields(QueryFields(c)).Error(err)
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
				FQAN:     row[FQAN].Data,
				UserName: row[UserName].Data,
				UnitName: row[UnitName].Data,
			})
		}
	}

	return out, nil
}

// getGroupGID godoc
// @Summary      Returns the groupid (intername FERRY identifier).
// @Description  Returns the groupid (intername FERRY identifier).
// @Tags         Basic Queries
// @Accept       html
// @Produce      json
// @Param        groupname   query     string  true  "name of the group"
// @Success      200  {object}  miscGroupGID
// @Failure      400  {object}  jsonOutput
// @Failure      401  {object}  jsonOutput
// @Router /getGroupGID [get]
func getGroupGID(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	gid := NewNullAttribute(GID)
	groupid := NewNullAttribute(GroupID)

	err := c.DBtx.QueryRow(`select groupid, gid from groups where name = $1 and type = 'UnixGroup'`, i[GroupName]).Scan(&groupid, &gid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
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

// getGroupName godoc
// @Summary      Returns the groupname.
// @Description  Returns the groupname.
// @Tags         Basic Queries
// @Accept       html
// @Produce      json
// @Param        gid   query     int  true  "GID of the group"
// @Success      200  {object}  miscGroupName
// @Failure      400  {object}  jsonOutput
// @Failure      401  {object}  jsonOutput
// @Router /getGroupName [get]
func getGroupName(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	groupname := NewNullAttribute(GroupName)

	err := c.DBtx.QueryRow(`select name from groups where gid=$1`, i[GID]).Scan(&groupname)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
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

// lookupCertificateDN godoc
// @Summary      Returns the uid and the username who is assigned to a certificate DN.
// @Description  Returns the uid and the username who is assigned to a certificate DN.
// @Tags         Basic Queries
// @Accept       html
// @Produce      json
// @Param        dn   query     string  true  "certificate DN to look up"
// @Success      200  {object}  miscDNowner
// @Failure      400  {object}  jsonOutput
// @Failure      401  {object}  jsonOutput
// @Router /lookupCertificateDN [get]
func lookupCertificateDN(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	dnid := NewNullAttribute(DNID)
	err := c.DBtx.QueryRow(`select dnid from user_certificates where dn = $1`, i[DN]).Scan(&dnid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
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
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	type jsonuser map[Attribute]interface{}
	out := make(jsonuser)

	if row[UID].Valid {
		out = jsonuser{
			UID:      row[UID].Data,
			UserName: row[UserName].Data,
		}
	}

	return out, nil
}

// getMappedGidFile godoc
// @Summary      Returns the contents of a file that lists all the fqans and the usernames and gids these fqans are mapped into.
// @Description  Returns the contents of a file that lists all the fqans and the usernames and gids these fqans are mapped into.
// @Description  This method is primarily needed by storage access. It defines which gid a role should be mapped into for file
// @Description  access. The mapped username and groupname are kept in Ferry for each fqan.
// @Tags         Authorization Queries
// @Accept       html
// @Produce      json
// @Success      200  {object}  miscMappedGidFile
// @Failure      400  {object}  jsonOutput
// @Failure      401  {object}  jsonOutput
// @Router /getMappedGidFile [get]
func getMappedGidFile(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	rows, err := c.DBtx.Query(`select fqan, uname, gid from grid_fqan as gf
							   left join groups as g on g.groupid = gf.mapped_group
							   left join users as u on u.uid = gf.mapped_user`)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	type jsonmapping map[Attribute]interface{}
	var out []jsonmapping

	const MappedUname Attribute = "mapped_uname"
	const MappedGID Attribute = "mapped_gid"

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
					FQAN:        row[FQAN].Data,
					MappedUname: row[UserName].Data,
					MappedGID:   row[GID].Data,
				})
			}
		}
	}

	return out, nil
}

// getStorageAuthzDBFile godoc
// @Summary      Returns the list of authorized users for the dCache server.
// @Description  Returns the list of authorized users for the dCache server.  There are two different JSON outputs provided based
// @Description  on the the parameter passwdmode.  (Now how do you show that in swagger?)
// @Tags         Authorization Queries
// @Accept       html
// @Produce      json
// @Param        lastupdated    query     string  false  "limit results to records  updated since"  Format(date)
// @Param        passwdmode     query     string  false  "Changes the JSON struct output.  Why?  I have no idea."
// @Failure      400  {object}  jsonOutput
// @Failure      401  {object}  jsonOutput
// @Router /getStorageAuthzDBFile [get]
func getStorageAuthzDBFile(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	rows, err := c.DBtx.Query(`select full_name, uname, uid, gid, ug.last_updated
								from users
								join user_group as ug using(uid)
								join groups using(groupid)
                               where type = 'UnixGroup' and (ug.last_updated>=$1 or $1 is null)
							   order by uname`, i[LastUpdated])
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
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
				tmpMap["all"] = append(tmpMap["all"], jsonmap{
					UserName: row[UserName].Data,
					UID:      row[UID].Data,
					GID:      row[GID].Data,
					GECOS:    row[FullName].Data,
					HomeDir:  "/home/" + row[UserName].Data.(string),
					Shell:    "/sbin/nologin",
				})
				prevUname = *row[UserName]
			}
		}
		out["fermilab"] = jsonmap{
			Resources:   tmpMap,
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

// getAffiliationMembersRoles godoc
// @Summary      Returns  the list of authorized users for POMS service.
// @Description  Returns  the list of authorized users for POMS service.
// @Tags         Authorization Queries
// @Accept       html
// @Produce      json
// @Param        role           query     string  false  "restrict results to production, analysis or ..."
// @Param        unitname       query     string  false  "affiliation to restrict authorization list to"
// @Success      200  {object}  miscAffMembRoles
// @Failure      400  {object}  jsonOutput
// @Failure      401  {object}  jsonOutput
// @Router /getAffiliationMembersRoles [get]
func getAffiliationMembersRoles(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	role := i[Role].Default("%")

	unitid := NewNullAttribute(UnitID)
	err := c.DBtx.QueryRow(`select unitid from affiliation_units where name = $1`, i[UnitName]).Scan(&unitid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !unitid.Valid && i[UnitName].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
		return nil, apiErr
	}

	rows, err := DBptr.Query(`select name, fqan, uname, full_name, token_subject, uid
								from grid_access
								join grid_fqan using(fqanid)
								join users using(uid)
								left join affiliation_units using(unitid)
							  where (unitid = $1 or $1 is null) and (lower(fqan) like lower($2))`,
		unitid, "%/role="+role.Data.(string)+"/%")
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	type jsonentry map[Attribute]interface{}
	out := make(map[string][]jsonentry)

	for rows.Next() {
		row := NewMapNullAttribute(UnitName, FQAN, UserName, FullName, TokenSubject, UID)
		rows.Scan(row[UnitName], row[FQAN], row[UserName], row[FullName], row[TokenSubject], row[UID])

		if row[FQAN].Valid {
			out[row[UnitName].Data.(string)] = append(out[row[UnitName].Data.(string)], jsonentry{
				FQAN:         row[FQAN].Data,
				UserName:     row[UserName].Data,
				FullName:     row[FullName].Data,
				TokenSubject: row[TokenSubject].Data,
				UID:          row[UID].Data,
			})
		}
	}

	return out, nil
}

// createComputeResource godoc
// @Summary      Creates a compute resource in Ferry's database.
// @Description  Creates a compute resource in Ferry's database.
// @Tags         Compute and Storage Resources
// @Accept       html
// @Produce      json
// @Param        homedir        query     string  true  "home directory associated with the resource"
// @Param        resourcename   query     string  true  "compute resource to return passwd file data for"
// @Param        resourcetype   query     string  true  "interactive or batch"
// @Param        shell          query     string  false "default shell for the resource"
// @Param        unitname       query     string  false "affiliation to relate resource to, if any"
// @Success      200  {object}  jsonOutput
// @Failure      400  {object}  jsonOutput
// @Failure      401  {object}  jsonOutput
// @Router /createComputeResource [put]
func createComputeResource(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	shell := i[Shell].Default("/sbin/nologin")

	unitid := NewNullAttribute(UnitID)
	compid := NewNullAttribute(ResourceID)
	err := c.DBtx.QueryRow(`select (select unitid from affiliation_units where name = $1),
								   (select compid from compute_resources where name = $2)`,
		i[UnitName], i[ResourceName]).Scan(&unitid, &compid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
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
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, nil
}

// setComputeResourceInfo godoc
// @Summary      Modifies the settings for a specific compute resource.
// @Description  Modifies the settings for a specific compute resource.
// @Tags         Compute and Storage Resources
// @Accept       html
// @Produce      json
// @Param        homedir        query     string  false  "home directory associated with the resource"
// @Param        resourcename   query     string  false  "compute resource to return passwd file data for"
// @Param        resourcetype   query     string  false  "interactive or batch"
// @Param        shell          query     string  false "default shell for the resource"
// @Param        unitname       query     string  false "affiliation to relate resource to, if any"
// @Success      200  {object}  jsonOutput
// @Failure      400  {object}  jsonOutput
// @Failure      401  {object}  jsonOutput
// @Router /setComputeResourceInfo [post]
func setComputeResourceInfo(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	unitid := NewNullAttribute(UnitID)
	compid := NewNullAttribute(ResourceID)
	err := c.DBtx.QueryRow(`select (select unitid from affiliation_units where name = $1),
								   (select compid from compute_resources where name = $2)`,
		i[UnitName], i[ResourceName]).Scan(&unitid, &compid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
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
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, nil
}

// createStorageResource godoc
// @Summary      Creates a storage resourse in Ferry's database.
// @Description  Creates a storage resourse in Ferry's database.
// @Tags         Compute and Storage Resources
// @Accept       html
// @Produce      json
// @Param        path           query     string  false  "the default path for the resource"
// @Param        quota          query     string  false  "the default quota for the resource"
// @Param        quotaunit      query     string  false  "the unit quota is given in ... B,KB,KIB,MB,MIB,GB,GIB,TB,TIB"
// @Param        resourcename   query     string  true   "the name of the resource"
// @Param        resourcetype   query     string  true   "nfs or eos"
// @Success      200  {object}  jsonOutput
// @Failure      400  {object}  jsonOutput
// @Failure      401  {object}  jsonOutput
// @Router /createStorageResource [put]
func createStorageResource(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	storageid := NewNullAttribute(ResourceID)
	err := c.DBtx.QueryRow(`select storageid from storage_resources where name = $1`,
		i[ResourceName]).Scan(&storageid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if storageid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDuplicateData, ResourceName))
	}
	if i[ResourceType].AbsoluteNull {
		apiErr = append(apiErr, DefaultAPIError(ErrorInvalidData, ResourceType))
	}
	if i[QuotaUnit].Valid {
		if checkUnits(i[QuotaUnit].Data.(string)) {
			unit := NewNullAttribute(QuotaUnit)
			unit.Scan(strings.ToUpper(i[QuotaUnit].Data.(string)))
			i[QuotaUnit] = unit
		} else {
			apiErr = append(apiErr, APIError{fmt.Errorf("allowed quotaunit values are B,KB,KIB,MB,MIB,GB,GIB,TB,TIB"), ErrorAPIRequirement})
		}
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
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, nil
}

// getStorageResourceInfo godoc
// @Summary      Returns the contents for a group file for a compute resource assigned to an affiliation unit.
// @Description  Returns the contents for a group file for a compute resource assigned to an affiliation unit.
// @Tags         Compute and Storage Resources
// @Accept       html
// @Produce      json
// @Param        resourcename   query     string  false  "resource to return information for"
// @Success      200  {object}  miscStorageResourceInfo
// @Failure      400  {object}  jsonOutput
// @Failure      401  {object}  jsonOutput
// @Router /getStorageResourceInfo [get]
func getStorageResourceInfo(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	storageid := NewNullAttribute(ResourceID)
	err := c.DBtx.QueryRow(`select storageid from storage_resources where name = $1`,
		i[ResourceName]).Scan(&storageid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !storageid.Valid && i[ResourceName].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, ResourceName))
		return nil, apiErr
	}

	rows, err := c.DBtx.Query(`select name, default_path, default_quota, default_unit, type from storage_resources
							   where storageid = $1 or $1 is null order by name`, storageid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	type jsonresource map[Attribute]interface{}
	out := make([]jsonresource, 0)

	for rows.Next() {
		row := NewMapNullAttribute(ResourceName, Path, Quota, QuotaUnit, ResourceType)
		rows.Scan(row[ResourceName], row[Path], row[Quota], row[QuotaUnit], row[ResourceType])

		if row[ResourceName].Valid {
			out = append(out, jsonresource{
				ResourceName: row[ResourceName].Data,
				Path:         row[Path].Data,
				Quota:        row[Quota].Data,
				QuotaUnit:    row[QuotaUnit].Data,
				ResourceType: row[ResourceType].Data,
			})
		}
	}

	return out, nil
}

// setStorageResourceInfo godoc
// @Summary      Modify the settings for a storage resource in the database.
// @Description  Modify the settings for a storage resource in the database.
// @Tags         Compute and Storage Resources
// @Accept       html
// @Produce      json
// @Param        path           query     string  false  "the default path for the resource"
// @Param        quota          query     string  false  "the default quota for the resource"
// @Param        quotaunit      query     string  false  "the unit quota is given in ... B,KB,KIB,MB,MIB,GB,GIB,TB,TIB"
// @Param        resourcename   query     string  true   "the name of the resource to be modified"
// @Param        resourcetype   query     string  true   "nfs or eos"
// @Success      200  {object}  jsonOutput
// @Failure      400  {object}  jsonOutput
// @Failure      401  {object}  jsonOutput
// @Router /setStorageResourceInfo [post]
func setStorageResourceInfo(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	storageid := NewNullAttribute(ResourceID)
	err := c.DBtx.QueryRow(`select storageid from storage_resources where name = $1`,
		i[ResourceName]).Scan(&storageid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !storageid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, ResourceName))
	}
	if i[ResourceType].AbsoluteNull {
		apiErr = append(apiErr, DefaultAPIError(ErrorInvalidData, ResourceType))
	}
	if i[QuotaUnit].Valid {
		if checkUnits(i[QuotaUnit].Data.(string)) {
			unit := NewNullAttribute(QuotaUnit)
			unit.Scan(strings.ToUpper(i[QuotaUnit].Data.(string)))
			i[QuotaUnit] = unit
		} else {
			apiErr = append(apiErr, APIError{fmt.Errorf("allowed quotaunit values are B,KB,KIB,MB,MIB,GB,GIB,TB,TIB"), ErrorAPIRequirement})
		}
	}
	if i[Quota].Valid != i[QuotaUnit].Valid {
		apiErr = append(apiErr, APIError{fmt.Errorf("quota requires quotaunit"), ErrorAPIRequirement})
	}
	if !i[ResourceType].Valid && !i[Quota].Valid && !i[QuotaUnit].Valid && !i[Path].Valid {
		apiErr = append(apiErr, APIError{errors.New("not enough arguments"), ErrorAPIRequirement})
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`update storage_resources
						  set default_path = coalesce($1, default_path),
							  default_quota = coalesce($2, default_quota),
							  default_unit = coalesce($3, default_unit),
							  type = coalesce($4, type),
							  last_updated = NOW()
						  where name = $5`, i[Path], i[Quota], i[QuotaUnit], i[ResourceType], i[ResourceName])

	return nil, nil
}

// getAllComputeResources godoc
// @Summary      Returns compute resouce settings and affiliations.
// @Description  Returns compute resouce settings and affiliations.
// @Tags         Compute and Storage Resources
// @Accept       html
// @Produce      json
// @Param        lastupdated    query     string  false  "limit results to records  updated since"  Format(date)
// @Param        resourcetype   query     string  false  "limit results to a specific type of resource"
// @Success      200  {object}  miscComputeResources
// @Failure      400  {object}  jsonOutput
// @Failure      401  {object}  jsonOutput
// @Router /getAllComputeResources [get]
func getAllComputeResources(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	resourceType := NewNullAttribute(ResourceType)

	if i[ResourceType].Valid {
		err := c.DBtx.QueryRow(`select type from compute_resources where type = $1`, i[ResourceType]).Scan(&resourceType)
		if err != nil && err != sql.ErrNoRows {
			log.WithFields(QueryFields(c)).Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
			return nil, apiErr
		} else if err == sql.ErrNoRows {
			apiErr = append(apiErr, DefaultAPIError(ErrorInvalidData, ResourceType))
			return nil, apiErr
		}
	}

	rows, err := DBptr.Query(`select cr.name, default_shell, default_home_dir, cr.type, au.name
							  from compute_resources as cr
							  left join affiliation_units as au using(unitid)
							  where (cr.last_updated>=$1 or $1 is null)
							    and (cr.type=$2 or $2 is null);`, i[LastUpdated], resourceType)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	type jsonresource map[Attribute]interface{}

	out := make([]jsonresource, 0)

	for rows.Next() {
		row := NewMapNullAttribute(ResourceName, Shell, HomeDir, ResourceType, UnitName)
		rows.Scan(row[ResourceName], row[Shell], row[HomeDir], row[ResourceType], row[UnitName])
		out = append(out, jsonresource{
			ResourceName: row[ResourceName].Data,
			Shell:        row[Shell].Data,
			HomeDir:      row[HomeDir].Data,
			ResourceType: row[ResourceType].Data,
			UnitName:     row[UnitName].Data,
		})
	}

	return out, nil
}

// ping godoc
// @Summary      Dive! Dive! Dive!
// @Description  Run Silent, Run Deep.
// @Tags         Basic Queries
// @Accept       html
// @Produce      json
// @Success      200  {object}  jsonOutput
// @Failure      400  {object}  jsonOutput
// @Failure      401  {object}  jsonOutput
// @Router /ping [get]
func ping(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError
	const ReleaseVersion Attribute = "releaseversion"
	const BuildDate Attribute = "builddate"
	const Server Attribute = "server"

	rows, err := DBptr.Query(`select now();`)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	return map[Attribute]interface{}{ReleaseVersion: release_ver, BuildDate: build_date, Server: serverRole}, nil
}

// getVOUserMap godoc
// @Summary      Returns the contents for a grid-vorolemap file.
// @Description  Returns the contents for a grid-vorolemap file.
// @Tags         Authorization Queries
// @Accept       html
// @Produce      json
// @Param        fqan       query     string  false  "restrict returned data to a specific fqan"
// @Param        unitname   query     string  false  "restrict returned data to a specific affiliatiion"
// @Param        username   query     string  false  "restruct returned data to a specific user"
// @Success      200  {object}  miscVOUserMap
// @Failure      400  {object}  jsonOutput
// @Failure      401  {object}  jsonOutput
// @Router /getVOUserMap [get]
func getVOUserMap(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	var validUser, validrUnit, validFQAN sql.NullBool
	err := c.DBtx.QueryRow(`select ($1 in (select uname from users)),
								   ($2 in (select name from affiliation_units)),
								   ($3 in (select fqan from grid_fqan))`,
		i[UserName], i[UnitName], i[FQAN]).Scan(&validUser, &validrUnit, &validFQAN)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !validUser.Bool && i[UserName].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
	}
	if !validrUnit.Bool && i[UnitName].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
	}
	if !validFQAN.Bool && i[FQAN].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, FQAN))
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	user := i[UserName].Default("%")
	unit := i[UnitName].Default("%")
	fqan := i[FQAN].Default("%")

	rows, err := DBptr.Query(`select distinct
								u1.uname as username,
								au.name as unitname,
								gf.fqan as fqan,
								coalesce(u2.uname, u1.uname) as mapped_user
							  from
								grid_access g
								join users u1 using(uid)
								join grid_fqan gf using(fqanid)
								join affiliation_units au using(unitid)
								left outer join users u2 on gf.mapped_user = u2.uid
							  where
								u1.uname like $1
								and au.name like $2
								and gf.fqan like $3
							  order by u1.uname`, user, unit, fqan)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	out := make(map[string]map[string]map[string]interface{})

	for rows.Next() {
		row := NewMapNullAttribute(UserName, UnitName, FQAN, UserAttribute)
		rows.Scan(row[UserName], row[UnitName], row[FQAN], row[UserAttribute])

		if row[UserName].Valid {
			if _, ok := out[row[UserName].Data.(string)]; !ok {
				out[row[UserName].Data.(string)] = make(map[string]map[string]interface{})
			}
			if _, ok := out[row[UserName].Data.(string)][row[UnitName].Data.(string)]; !ok {
				out[row[UserName].Data.(string)][row[UnitName].Data.(string)] = make(map[string]interface{})
			}
			out[row[UserName].Data.(string)][row[UnitName].Data.(string)][row[FQAN].Data.(string)] = row[UserAttribute].Data
		}
	}

	return out, nil
}

// cleanStorageQuotas godoc
// @Summary      Cleans expired temporary quotas and bump their permanent counterparts last updated date.
// @Description  Cleans expired temporary quotas and bump their permanent counterparts last updated date.
// @Tags         Compute and Storage Resources
// @Accept       html
// @Produce      json
// @Success      200  {object}  jsonOutput
// @Failure      400  {object}  jsonOutput
// @Failure      401  {object}  jsonOutput
// @Router /cleanStorageQuotas [post]
func cleanStorageQuotas(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	_, err := c.DBtx.Exec(`DO $$
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
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, nil
}

// cleanCondorQuotas godoc
// @Summary      Cleans expired temporary quotas and bump their permanent counterparts last updated date.
// @Description  Cleans expired temporary quotas and bump their permanent counterparts last updated date.
// @Tags         Compute and Storage Resources
// @Accept       html
// @Produce      json
// @Success      200  {object}  jsonOutput
// @Failure      400  {object}  jsonOutput
// @Failure      401  {object}  jsonOutput
// @Router /cleanCondorQuotas [post]
func cleanCondorQuotas(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	_, err := c.DBtx.Exec(`DO $$
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
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, nil
}

// setStorageQuota godoc
// @Summary      Sets the storage quota assigned for a user or group when “groupaccount” is true.
// @Description  Sets the storage quota assigned for a user or group when “groupaccount” is true.
// @Tags         Users
// @Accept       html
// @Produce      json
// @Param        expirationdate query     string  false  "date quota expires"  Format(date)
// @Param        groupaccount   query     bool    false  "set to true if this is a user account"
// @Param        groupname      query     string  false  "** required for group accounts"
// @Param        path           query     string  false  "** required for user accounts"
// @Param        quota          query     int     true   "quota limit -- value adjusted by quotaunit"
// @Param        quotaunit      query     string  true   "allowed quotaunit values are B,KB,KIB,MB,MIB,GB,GIB,TB,TIB"
// @Param        resourcename   query     string  true   "resource to apply quota on"
// @Param        unitname       query     string  true   "affiliation to apply quota on"
// @Param        username       query     string  false  "** required for user accounts"
// @Success      200  {object}  jsonOutput
// @Failure      400  {object}  jsonOutput
// @Failure      401  {object}  jsonOutput
// @Router /setStorageQuota [put]
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
		log.WithFields(QueryFields(c)).Error(queryerr)
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
		if queryerr == sql.ErrNoRows {
			if !i[ExpirationDate].Valid { // New permanent quota
				queryerr = c.DBtx.tx.QueryRow(`select concat(default_path, '/', $1::varchar) from storage_resources where storageid = $2`,
					i[dataAttr], vStorageid).Scan(&vPath)
			} else {
				apiErr = append(apiErr, APIError{errors.New("no permanent quota"), ErrorAPIRequirement})
				return nil, apiErr
			}
		}
		if queryerr != nil && queryerr != sql.ErrNoRows {
			log.WithFields(QueryFields(c)).Error(queryerr)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
			return nil, apiErr
		}
	}
	if !vPath.Valid && !gAccount.Data.(bool) {
		apiErr = append(apiErr, APIError{errors.New("null path for user quota"), ErrorAPIRequirement})
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
