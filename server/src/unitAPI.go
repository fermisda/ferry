package main

import (
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"

	_ "github.com/lib/pq"
	log "github.com/sirupsen/logrus"
)

// IncludeUnitAPIs includes all APIs described in this file in an APICollection
func IncludeUnitAPIs(c *APICollection) {

	setAffiliationUnitInfo := BaseAPI{
		InputModel{
			Parameter{UnitName, true},
			Parameter{VOMSURL, false},
			Parameter{AlternativeName, false},
			Parameter{UnitType, false},
		},
		setAffiliationUnitInfo,
		RoleWrite,
	}
	c.Add("setAffiliationUnitInfo", &setAffiliationUnitInfo)

	getAffiliationUnitMembers := BaseAPI{
		InputModel{
			Parameter{UnitName, true},
			Parameter{LastUpdated, false},
		},
		getAffiliationUnitMembers,
		RoleRead,
	}
	c.Add("getAffiliationUnitMembers", &getAffiliationUnitMembers)

	getGroupsInAffiliationUnit := BaseAPI{
		InputModel{
			Parameter{UnitName, false},
			Parameter{GroupType, false},
			Parameter{LastUpdated, false},
		},
		getGroupsInAffiliationUnit,
		RoleRead,
	}
	c.Add("getGroupsInAffiliationUnit", &getGroupsInAffiliationUnit)

	getGroupLeadersinAffiliationUnit := BaseAPI{
		InputModel{
			Parameter{UnitName, true},
		},
		getGroupLeadersinAffiliationUnit,
		RoleRead,
	}
	c.Add("getGroupLeadersinAffiliationUnit", &getGroupLeadersinAffiliationUnit)

	getAffiliationUnitComputeResources := BaseAPI{
		InputModel{
			Parameter{UnitName, true},
			Parameter{LastUpdated, false},
		},
		getAffiliationUnitComputeResources,
		RoleRead,
	}
	c.Add("getAffiliationUnitComputeResources", &getAffiliationUnitComputeResources)

	createAffiliationUnit := BaseAPI{
		InputModel{
			Parameter{UnitName, true},
			Parameter{VOMSURL, false},
			Parameter{AlternativeName, false},
			Parameter{UnitType, false},
		},
		createAffiliationUnit,
		RoleWrite,
	}
	c.Add("createAffiliationUnit", &createAffiliationUnit)

	removeAffiliationUnit := BaseAPI{
		InputModel{
			Parameter{UnitName, true},
		},
		removeAffiliationUnit,
		RoleWrite,
	}
	c.Add("removeAffiliationUnit", &removeAffiliationUnit)

	createFQAN := BaseAPI{
		InputModel{
			Parameter{FQAN, true},
			Parameter{GroupName, true},
			Parameter{UserName, false},
			Parameter{UnitName, false},
		},
		createFQAN,
		RoleWrite,
	}
	c.Add("createFQAN", &createFQAN)

	setFQANMappings := BaseAPI{
		InputModel{
			Parameter{FQAN, true},
			Parameter{GroupName, false},
			Parameter{UserName, false},
		},
		setFQANMappings,
		RoleWrite,
	}
	c.Add("setFQANMappings", &setFQANMappings)

	removeFQAN := BaseAPI{
		InputModel{
			Parameter{FQAN, true},
		},
		removeFQAN,
		RoleWrite,
	}
	c.Add("removeFQAN", &removeFQAN)

	getAllAffiliationUnits := BaseAPI{
		InputModel{
			Parameter{VOName, false},
			Parameter{LastUpdated, false},
		},
		getAllAffiliationUnits,
		RoleRead,
	}
	c.Add("getAllAffiliationUnits", &getAllAffiliationUnits)
}

func createAffiliationUnit(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError
	unitid := NewNullAttribute(UnitID)

	//only the unit name is actually required; the others can be empty
	if !i[UnitName].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorAPIRequirement, UnitName))
		return nil, apiErr
	}

	err := c.DBtx.QueryRow(`select unitid from affiliation_units where name=$1`,
		i[UnitName]).Scan(&unitid)

	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	//if unitid is valid the entry is a duplicate, so skip this and make sure it has a voms_url
	if !unitid.Valid {
		_, err = c.DBtx.Exec(`insert into affiliation_units (name, alternative_name, type, last_updated) values ($1, $2, $3, NOW())`, i[UnitName], i[AlternativeName], i[UnitType])
		if err != nil {
			log.WithFields(QueryFields(c)).Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
			return nil, apiErr
		}
	}

	// Add a VOMSURL if it does not already exist.
	if i[VOMSURL].Valid {
		_, vomserr := c.DBtx.Exec(`insert into voms_url (unitid, url) values ((select unitid from affiliation_units where name = $1), $2)
										on conflict (unitid, url) do nothing`, i[UnitName], i[VOMSURL])
		if vomserr != nil {
			log.WithFields(QueryFields(c)).Error(vomserr)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
			return nil, apiErr
		}
	}
	return nil, nil
}

func removeAffiliationUnit(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	unitid := NewNullAttribute(UnitID)

	err := c.DBtx.QueryRow(`select unitid from affiliation_units where name=$1`, i[UnitName]).Scan(&unitid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	if !unitid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`delete from voms_url where unitid = $1`, unitid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`delete from affiliation_units where unitid = $1`, unitid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		if strings.Contains(err.Error(), "violates foreign key constraint") {
			apiErr = append(apiErr, APIError{errors.New("all associations with this affiliation unit shall be removed before it can be deleted"), ErrorAPIRequirement})
		} else {
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		}
		return nil, apiErr
	}

	return nil, nil
}

func setAffiliationUnitInfo(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	if !i[UnitName].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
		return nil, apiErr
	}

	if !i[UnitType].Valid && !i[VOMSURL].Valid && !i[AlternativeName].Valid && !i[UnitType].AbsoluteNull && !i[AlternativeName].AbsoluteNull {
		apiErr = append(apiErr, APIError{errors.New("not enough arguments"), ErrorAPIRequirement})
		return nil, apiErr
	}
	tmpID := NewNullAttribute(UnitID)
	tmpvoms := NewNullAttribute(VOMSURL)
	tmpaltName := NewNullAttribute(AlternativeName)
	tmpType := NewNullAttribute(UnitType)

	queryerr := c.DBtx.QueryRow(`select au.unitid, vu.url, au.alternative_name, au.type from affiliation_units as au
								left join voms_url as vu on au.unitid = vu.unitid where name=$1`,
		i[UnitName]).Scan(&tmpID, &tmpvoms, &tmpaltName, &tmpType)

	if queryerr == sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(queryerr)
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
		return nil, apiErr
	} else if queryerr != nil {
		log.WithFields(QueryFields(c)).Error(queryerr)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	if i[UnitType].Valid || i[AlternativeName].Valid || i[UnitType].AbsoluteNull || i[AlternativeName].AbsoluteNull {
		_, queryerr = c.DBtx.Exec(`update affiliation_units set alternative_name = $1, type = $2, last_updated = NOW() where unitid = $3`,
			i[AlternativeName].Default(tmpaltName), i[UnitType].Default(tmpType), tmpID)

		if queryerr != nil {
			log.WithFields(QueryFields(c)).Error(queryerr)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
			return nil, apiErr
		}
	}
	if i[VOMSURL].Valid {
		_, queryerr = c.DBtx.Exec(`update voms_url set url = $1, last_updated = NOW() where unitid = $2`, i[VOMSURL].Default(tmpvoms), tmpID)

		if queryerr != nil {
			log.WithFields(QueryFields(c)).Error(queryerr)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
			return nil, apiErr
		}
	}
	return nil, nil
}

func getAffiliationUnitMembers(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	unitid := NewNullAttribute(UnitID)

	checkerr := c.DBtx.QueryRow(`select unitid from affiliation_units where name=$1`, i[UnitName]).Scan(&unitid)
	if checkerr != nil && checkerr != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(checkerr)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	if !unitid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
		return nil, apiErr
	}

	rows, checkerr := c.DBtx.Query(`select DISTINCT ug.uid, users.uname from user_group as ug join affiliation_unit_group as aug on aug.groupid = ug.groupid join users on ug.uid = users.uid where aug.unitid=$1 and (ug.last_updated>=$2 or $2 is null) order by ug.uid`,
		unitid, i[LastUpdated])
	if checkerr != nil {
		log.WithFields(QueryFields(c)).Error(checkerr)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	type jsonentry map[Attribute]interface{}
	out := make([]jsonentry, 0)

	for rows.Next() {
		row := NewMapNullAttribute(UID, UserName)
		rows.Scan(row[UID], row[UserName])

		if row[UID].Valid {
			out = append(out, jsonentry{
				UID:      row[UID].Data,
				UserName: row[UserName].Data,
			})
		}
	}
	return out, nil
}

func getGroupsInAffiliationUnit(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	unitid := NewNullAttribute(UnitID)
	grouptype := NewNullAttribute(GroupType)

	checkerr := c.DBtx.QueryRow(`select (select unitid from affiliation_units where name=$1),
										(select distinct type from groups where type = $2) `, i[UnitName], i[GroupType]).Scan(&unitid, &grouptype)
	if checkerr != nil && checkerr != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(checkerr)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	if i[UnitName].Data != nil && !unitid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
		return nil, apiErr
	}
	if grouptype.Data != "" && !grouptype.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, GroupType))
		return nil, apiErr
	}

	rows, checkerr := c.DBtx.Query(`select au.name, gid, groups.name, groups.type, aug.is_primary
									from affiliation_unit_group as aug
									join groups on aug.groupid = groups.groupid
									join affiliation_units au on aug.unitid = au.unitid
									where (au.name=$1 or $1 is null)
										and (aug.last_updated>=$2 or $2 is null)
										and (groups.type = $3 or $3 is null)
									order by au.name, groups.name`,
		i[UnitName], i[LastUpdated], i[GroupType])
	if checkerr != nil {
		log.WithFields(QueryFields(c)).Error(checkerr)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	defer rows.Close()
	type jsonentry map[Attribute]interface{}
	out := make([]jsonentry, 0)

	for rows.Next() {
		row := NewMapNullAttribute(UnitName, GID, GroupName, GroupType, Primary)
		rows.Scan(row[UnitName], row[GID], row[GroupName], row[GroupType], row[Primary])
		entry := jsonentry{
			UnitName:  row[UnitName].Data,
			GID:       row[GID].Data,
			GroupName: row[GroupName].Data,
			GroupType: row[GroupType].Data,
			Primary:   row[Primary].Data,
		}
		out = append(out, entry)
	}
	return out, nil
}

func getGroupLeadersinAffiliationUnit(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	unitid := NewNullAttribute(UnitID)

	err := c.DBtx.QueryRow(`select unitid from affiliation_units where name=$1`, i[UnitName]).Scan(&unitid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !unitid.Valid && i[UnitName].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
		return nil, apiErr
	}

	rows, checkerr := DBptr.Query(`select name, type, uid, uname from user_group
								   join users using(uid)
								   join groups using(groupid)
								   where is_leader = TRUE and
								   user_group.groupid in
										(select groupid from affiliation_unit_group where unitid = $1)
								   order by groups.name, groups.type`,
		unitid)

	if checkerr != nil && checkerr != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(checkerr)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	defer rows.Close()
	type jsonout map[Attribute]interface{}
	type jsonlist []interface{}

	out := make([]jsonout, 0)
	prevGroupName := NewNullAttribute(GroupName)
	prevGroupType := NewNullAttribute(GroupType)
	var uids, unames jsonlist

	for rows.Next() {
		row := NewMapNullAttribute(GroupName, GroupType, UID, UserName)
		rows.Scan(row[GroupName], row[GroupType], row[UID], row[UserName])
		if row[GroupName].Valid {
			if prevGroupName != *row[GroupName] {
				if prevGroupName.Valid {
					out = append(out, jsonout{
						GroupName: prevGroupName.Data,
						GroupType: prevGroupType.Data,
						UID:       uids,
						UserName:  unames,
					})
				}
				uids = jsonlist{row[UID].Data}
				unames = jsonlist{row[UserName].Data}
				prevGroupName = *row[GroupName]
				prevGroupType = *row[GroupType]
			} else {
				uids = append(uids, row[UID].Data)
				unames = append(unames, row[UserName].Data)
			}
		}
	}

	return out, nil
}

func getAffiliationUnitComputeResources(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	unitid := NewNullAttribute(UnitID)

	err := c.DBtx.QueryRow(`select unitid from affiliation_units where name=$1`, i[UnitName]).Scan(&unitid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !unitid.Valid && i[UnitName].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
		return nil, apiErr
	}

	rows, err := c.DBtx.Query(`select name, type, default_shell, default_home_dir from compute_resources
							   where unitid = $1 and (last_updated>=$2 or $2 is null) order by name`, unitid, i[LastUpdated])
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	type jsonout map[Attribute]interface{}
	out := make([]jsonout, 0)
	for rows.Next() {
		row := NewMapNullAttribute(ResourceName, ResourceType, Shell, HomeDir)
		rows.Scan(row[ResourceName], row[ResourceType], row[Shell], row[HomeDir])
		if row[ResourceName].Valid && row[ResourceType].Valid && row[Shell].Valid && row[HomeDir].Valid {
			out = append(out, jsonout{
				ResourceName: row[ResourceName].Data,
				ResourceType: row[ResourceType].Data,
				Shell:        row[Shell].Data,
				HomeDir:      row[HomeDir].Data,
			})
		}
	}
	return out, nil
}

func createFQAN(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	groupid := NewNullAttribute(GroupID)
	uid := NewNullAttribute(UID)
	unitid := NewNullAttribute(UnitID)

	err := c.DBtx.QueryRow(`select (select groupid from groups where name = $1 and type = 'UnixGroup'),
								   (select uid from users where uname = $2),
								   (select unitid from affiliation_units where name = $3)`,
		i[GroupName], i[UserName], i[UnitName]).Scan(&groupid, &uid, &unitid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !groupid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, GroupName))
	}
	if !uid.Valid && i[UserName].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
	}
	if !unitid.Valid {
		if i[UnitName].Valid {
			apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
		}
	} else {
		if ok, _ := regexp.MatchString(fmt.Sprintf(`^\/(fermilab\/)?%s\/.*`, i[UnitName].Data.(string)), i[FQAN].Data.(string)); !ok {
			apiErr = append(apiErr, APIError{errors.New("invalid FQAN"), ErrorAPIRequirement})
		}
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	var duplicateFQAN bool
	err = c.DBtx.QueryRow(`select (($1, coalesce($2, -1))     in (select fqan, coalesce(unitid, -1) from grid_fqan)) or
								  (($1, $3, coalesce($4, -1)) in (select fqan, mapped_group, coalesce(mapped_user, -1) from grid_fqan))`,
		i[FQAN], unitid, groupid, uid).Scan(&duplicateFQAN)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	if duplicateFQAN {
		apiErr = append(apiErr, APIError{errors.New("specified FQAN mapping already exist"), ErrorDuplicateData})
		return nil, apiErr
	}

	if uid.Valid {
		var userInGroup bool
		err = c.DBtx.QueryRow(`select ($1, $2) in (select uid, groupid from user_group)`, uid, groupid).Scan(&userInGroup)
		if err != nil {
			log.WithFields(QueryFields(c)).Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
			return nil, apiErr
		}
		if !userInGroup {
			apiErr = append(apiErr, APIError{errors.New("user not a member of this group"), ErrorAPIRequirement})
			return nil, apiErr

		}
		if unitid.Valid {
			var groupInUnit bool
			err = c.DBtx.QueryRow(`select ($1, $2) in (select unitid, groupid from affiliation_unit_group)`, unitid, groupid).Scan(&groupInUnit)
			if err != nil {
				log.WithFields(QueryFields(c)).Error(err)
				apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
				return nil, apiErr
			}
			if !groupInUnit {
				apiErr = append(apiErr, APIError{errors.New("group not in this experiment"), ErrorAPIRequirement})
				return nil, apiErr
			}
		}
	}

	_, err = c.DBtx.Exec(`insert into grid_fqan (fqan, unitid, mapped_user, mapped_group, last_updated) values ($1, $2, $3, $4, NOW())`, i[FQAN], unitid, uid, groupid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, nil
}

func removeFQAN(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	fqanid := NewNullAttribute(FQANID)

	err := c.DBtx.QueryRow(`select fqanid from grid_fqan where fqan = $1`, i[FQAN]).Scan(&fqanid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !fqanid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, FQAN))
		return nil, apiErr
	}

	_, err = c.DBtx.Exec("delete from grid_fqan where fqanid = $1", fqanid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		if strings.Contains(err.Error(), "violates foreign key constraint") {
			apiErr = append(apiErr, APIError{errors.New("all user associations with this fqan shall be removed before it can be deleted"), ErrorAPIRequirement})
		} else {
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		}
		return nil, apiErr
	}

	return nil, nil
}

func setFQANMappings(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	groupid := NewNullAttribute(GroupID)
	uid := NewNullAttribute(UID)

	var validFQAN bool

	err := c.DBtx.QueryRow(`select (select $1 in (select fqan from grid_fqan)),
								   (select groupid from groups where name = $2 and type = 'UnixGroup'),
								   (select uid from users where uname = $3)`,
		i[FQAN], i[GroupName], i[UserName]).Scan(&validFQAN, &groupid, &uid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !validFQAN {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, FQAN))
	}
	if !i[UserName].Valid && !i[UserName].AbsoluteNull && !i[GroupName].Valid && !i[GroupName].AbsoluteNull {
		apiErr = append(apiErr, APIError{errors.New("no username or groupname specified in http query"), ErrorAPIRequirement})
		return nil, apiErr
	}
	if i[GroupName].AbsoluteNull {
		apiErr = append(apiErr, DefaultAPIError(ErrorInvalidData, GroupName))
	}
	if !groupid.Valid && i[GroupName].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, GroupName))
	}
	if !uid.Valid && i[UserName].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`update grid_fqan set
							mapped_user = case when $1 then $2 else coalesce($2, mapped_user) end,
							mapped_group = coalesce($3, mapped_group),
							last_updated = NOW()
						  where fqan = $4`,
		i[UserName].AbsoluteNull, uid, groupid, i[FQAN])
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, nil
}

func getAllAffiliationUnits(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	var countURLs int

	err := c.DBtx.QueryRow(`select count(*) from voms_url
							where url like concat('%voms/', $1::text) or url like concat('%voms/', $1::text, '/%')`,
		i[VOName]).Scan(&countURLs)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if countURLs < 1 && i[VOName].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, VOName))
		return nil, apiErr
	}

	rows, err := DBptr.Query(`select name, url from affiliation_units au left join voms_url using(unitid)
							  where url is not null and (url like concat('%voms/', $1::text) or url like concat('%voms/', $1::text, '/%') or $1 is null)
							  and (au.last_updated>=$2 or $2 is null)`,
		i[VOName], i[LastUpdated])
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	type jsonout map[Attribute]interface{}
	out := make([]jsonout, 0)

	for rows.Next() {
		row := NewMapNullAttribute(UnitName, VOMSURL)
		rows.Scan(row[UnitName], row[VOMSURL])
		out = append(out, jsonout{
			UnitName: row[UnitName].Data,
			VOMSURL:  row[VOMSURL].Data,
		})
	}

	return out, nil
}
