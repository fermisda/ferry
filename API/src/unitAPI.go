package main

import (
	"errors"
	"regexp"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
	_ "github.com/lib/pq"
	log "github.com/sirupsen/logrus"
)

// IncludeUnitAPIs includes all APIs described in this file in an APICollection
func IncludeUnitAPIs(c *APICollection) {

	setAffiliationUnitInfo := BaseAPI {
		InputModel {
			Parameter{UnitName, true},
			Parameter{VOMSURL, false},
			Parameter{AlternativeName, false},
			Parameter{UnitType, false},
		},
		setAffiliationUnitInfo,
	}
	c.Add("setAffiliationUnitInfo", &setAffiliationUnitInfo)

	getAffiliationUnitMembers := BaseAPI {
		InputModel {
			Parameter{UnitName, true},
			Parameter{LastUpdated, false},
		},
		getAffiliationUnitMembers,
	}
	c.Add("getAffiliationUnitMembers", &getAffiliationUnitMembers)

	getGroupsInAffiliationUnit := BaseAPI {
		InputModel {
			Parameter{UnitName, true},
		},
		getGroupsInAffiliationUnit,
	}
	c.Add("getGroupsInAffiliationUnit", &getGroupsInAffiliationUnit)

	getGroupLeadersinAffiliationUnit := BaseAPI {
		InputModel {
			Parameter{UnitName, true},
		},
		getGroupLeadersinAffiliationUnit,
	}
	c.Add("getGroupLeadersinAffiliationUnit", &getGroupLeadersinAffiliationUnit)

	getAffiliationUnitComputeResources := BaseAPI {
		InputModel {
			Parameter{UnitName, true},
			Parameter{LastUpdated, false},
		},
		getAffiliationUnitComputeResources,
	}
	c.Add("getAffiliationUnitComputeResources", &getAffiliationUnitComputeResources)

	createAffiliationUnit := BaseAPI {
		InputModel {
			Parameter{UnitName, true},
			Parameter{VOMSURL, false},
			Parameter{AlternativeName, false},
			Parameter{UnitType, false},
		},
		createAffiliationUnit,
	}
	c.Add("createAffiliationUnit", &createAffiliationUnit)

	createFQAN := BaseAPI {
		InputModel {
			Parameter{FQAN, true},
			Parameter{GroupName, true},
			Parameter{UserName, false},
			Parameter{UnitName, false},
		},
		createFQAN,
	}
	c.Add("createFQAN", &createFQAN)

	setFQANMappings := BaseAPI {
		InputModel {
			Parameter{FQAN, true},
			Parameter{GroupName, false},
			Parameter{UserName, false},
		},
		setFQANMappings,
	}
	c.Add("setFQANMappings", &setFQANMappings)
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
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	//if unitid is valid, the entry is a duplicate
	if unitid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDuplicateData, UnitName))
		return nil, apiErr
	}
	
	_, err = c.DBtx.Exec(`insert into affiliation_units (name, alternative_name, type, last_updated) values ($1, $2, $3, NOW())`, i[UnitName], i[AlternativeName], i[UnitType])
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))	
		return nil, apiErr
	} 
	if i[VOMSURL].Valid { //do only after unitid has been created by initial insert
		_, vomserr := c.DBtx.Exec(`insert into voms_url (unitid, url) values ((select unitid from affiliation_units where name = $1), $2)`, i[UnitName], i[VOMSURL])
		if vomserr != nil {
			log.WithFields(QueryFields(c.R, c.StartTime)).Error(vomserr)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))	
			return nil, apiErr
		}
	}
	return nil, nil
}

func removeAffiliationUnit(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	unitName := strings.TrimSpace(q.Get("unitname"))
	if unitName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No experiment specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No experiment name specified.\" }")
		return
	}
	//requires auth
	authorized, authout := authorize(r)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, "{ \"ferry_error\": \""+authout+"not authorized.\" }")
		return
	}
	//check if it is really there already
	// check if it already exists
	var unitId int
	checkerr := DBptr.QueryRow(`select unitid from affiliation_units where name=$1`, unitName).Scan(&unitId)
	log.WithFields(QueryFields(r, startTime)).Info("unitID = " + strconv.Itoa(unitId))
	switch {
	case checkerr == sql.ErrNoRows:
		// OK, it doesn't exist, let's add it now.
		log.WithFields(QueryFields(r, startTime)).Error("Cannot delete affiliation unit " + unitName + "; unit does not exist.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"Unit %s does not exist.\" }", unitName)
		return
	case checkerr != nil:
		//other weird error
		w.WriteHeader(http.StatusNotFound)
		log.WithFields(QueryFields(r, startTime)).Error("Cannot remove affiliation unit " + unitName + ": " + checkerr.Error())
		fmt.Fprintf(w, "{ \"ferry_error\": \"Database error; check logs.\" }")
		return
	default:

		DBtx, cKey, err := LoadTransaction(r, DBptr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Error starting DB transaction: " + err.Error())
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w, "{ \"ferry_error\": \"Error starting database transaction.\" }")
			return
		}
		defer DBtx.Rollback(cKey)
		// string for the remove statement

		removestr := fmt.Sprintf(`do $$ declare v_unitid int = %d ; begin delete from voms_url where unitid=v_unitid; delete from affiliation_units where unitid=v_unitid ; end $$;`, unitId)
		//create prepared statement
		_, err = DBtx.Exec(removestr)

		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Error deleting " + unitName + " to affiliation_units: " + err.Error())
			if strings.Contains(err.Error(), "fk_affiliation_unit_user_certificate_affiliation_units") {
				fmt.Fprintf(w, "{ \"ferry_error\": \"There are still user certificates associated with this unit.\" }")
			} else if strings.Contains(err.Error(), "fk_compute_resource_affiliation_units") {
				fmt.Fprintf(w, "{ \"ferry_error\": \"There are still compute resources associated with this unit.\" }")
			} else if strings.Contains(err.Error(), "fk_experiment_group_affiliation_units") {
				fmt.Fprintf(w, "{ \"ferry_error\": \"There are still groups associated with this unit\" }")
			} else if strings.Contains(err.Error(), "fk_grid_fqan_affiliation_units") {
				fmt.Fprintf(w, "{ \"ferry_error\": \"There are still FQANs associated with this unit.\" }")
			} else {
				fmt.Fprintf(w, "{ \"ferry_error\": \"Error executing DB deletion.\" }")
			}
		} else {
			// error is nil, so it's a success. Commit the transaction and return success.
			if cKey != 0 {
				DBtx.Commit(cKey)
			}
			log.WithFields(QueryFields(r, startTime)).Info("Successfully added " + unitName + " to affiliation_units.")
			fmt.Fprintf(w, "{ \"ferry_status\": \"success\" }")
		}
		return
	}
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
	tmpID 	   := NewNullAttribute(UnitID)
	tmpvoms    := NewNullAttribute(VOMSURL)
	tmpaltName := NewNullAttribute(AlternativeName)
	tmpType    := NewNullAttribute(UnitType)

	queryerr := c.DBtx.QueryRow(`select au.unitid, vu.url, au.alternative_name, au.type from affiliation_units as au
								left join voms_url as vu on au.unitid = vu.unitid where name=$1`,
		i[UnitName]).Scan(&tmpID, &tmpvoms, &tmpaltName, &tmpType)

	if queryerr == sql.ErrNoRows {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(queryerr)
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
		return nil, apiErr
	} else if queryerr != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(queryerr)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	if i[UnitType].Valid || i[AlternativeName].Valid || i[UnitType].AbsoluteNull || i[AlternativeName].AbsoluteNull {
		_, queryerr = c.DBtx.Exec(`update affiliation_units set alternative_name = $1, type = $2, last_updated = NOW() where unitid = $3`,
			i[AlternativeName].Default(tmpaltName), i[UnitType].Default(tmpType), tmpID)

		if queryerr != nil {
			log.WithFields(QueryFields(c.R, c.StartTime)).Error(queryerr)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
			return nil, apiErr
		}
	}
	if i[VOMSURL].Valid {
		_, queryerr = c.DBtx.Exec(`update voms_url set url = $1, last_updated = NOW() where unitid = $2`, i[VOMSURL].Default(tmpvoms), tmpID)

		if queryerr != nil {
			log.WithFields(QueryFields(c.R, c.StartTime)).Error(queryerr)
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
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(checkerr)
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
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(checkerr)
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
				UID:		row[UID].Data,
				UserName:	row[UserName].Data,
			})		
		}
	}
	return out, nil
}

func getGroupsInAffiliationUnit(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	unitid := NewNullAttribute(UnitID)

	checkerr := c.DBtx.QueryRow(`select unitid from affiliation_units where name=$1`, i[UnitName]).Scan(&unitid)
	if checkerr != nil && checkerr != sql.ErrNoRows {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(checkerr)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	if !unitid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
		return nil, apiErr
	}
	
	rows, checkerr := c.DBtx.Query(`select gid, groups.name, groups.type, aug.is_primary 
	from affiliation_unit_group as aug
	join groups on aug.groupid = groups.groupid
	where aug.unitid=$1 and (aug.last_updated>=$2 or $2 is null)`,
	unitid, i[LastUpdated])
	if checkerr != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(checkerr)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	
	defer rows.Close()
	type jsonentry map[Attribute] interface{}
	out := make([]jsonentry, 0)
	
	for rows.Next() {
		row := NewMapNullAttribute(GID, GroupName, GroupType, Primary)
		rows.Scan(row[GID], row[GroupName], row[GroupType], row[Primary])
		entry := jsonentry {
			GID:		row[GID].Data,
			GroupName:	row[GroupName].Data,
			GroupType:  row[GroupType].Data,
			Primary:	row[Primary].Data, 
		}
		out = append(out, entry)
	}
	return out, nil
}


func getGroupLeadersinAffiliationUnit(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	unitid	:= NewNullAttribute(UnitID)

	err := c.DBtx.QueryRow(`select unitid from affiliation_units where name=$1`, i[UnitName]).Scan(&unitid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
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
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(checkerr)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	defer rows.Close()
	type jsonout  map[Attribute]interface{}
	type jsonlist []interface{}
	
	out := make([]jsonout, 0)
	prevGroupName := NewNullAttribute(GroupName)
	prevGroupType := NewNullAttribute(GroupType)
	var uids, unames jsonlist


	for rows.Next() {
		row := NewMapNullAttribute(GroupName, GroupType, UID, UserName)
		rows.Scan(row[GroupName], row[GroupType], row[UID], row[UserName])
		if row[GroupName].Valid {
			if prevGroupName != *row[GroupName]{
				if prevGroupName.Valid {
					out = append(out, jsonout {
						GroupName:	prevGroupName.Data,
						GroupType:	prevGroupType.Data,
						UID:		uids,
						UserName:   unames,
					})
				}
				uids = jsonlist{row[UID].Data}
				unames = jsonlist{row[UserName].Data}
				prevGroupName = *row[GroupName]
				prevGroupType = *row[GroupType]
			} else {
				uids   = append(uids, row[UID].Data)
				unames = append(unames, row[UserName].Data)
			}
		}
	}

	return out, nil
}

func getAffiliationUnitStorageResources(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	//	q := r.URL.Query()
	//	collabunit := q.Get("unitname")
	NotDoneYet(w, r, startTime)
}

func getAffiliationUnitComputeResources(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	unitid	:= NewNullAttribute(UnitID)

	err := c.DBtx.QueryRow(`select unitid from affiliation_units where name=$1`, i[UnitName]).Scan(&unitid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
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
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	type jsonout map[Attribute] interface{}
	out := make([]jsonout, 0)
	for rows.Next() {
		row := NewMapNullAttribute(ResourceName, ResourceType, Shell, HomeDir)
		rows.Scan(row[ResourceName], row[ResourceType], row[Shell], row[HomeDir])
		if row[ResourceName].Valid && row[ResourceType].Valid && row[Shell].Valid && row[HomeDir].Valid {
			out = append(out, jsonout {
				ResourceName:	row[ResourceName].Data,
				ResourceType:	row[ResourceType].Data,
				Shell:			row[Shell].Data,
				HomeDir:   		row[HomeDir].Data,
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
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
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
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	if duplicateFQAN {
		apiErr = append(apiErr, APIError{errors.New("specified FQAN mapping already exist"), ErrorAPIRequirement})
		return nil, apiErr
	}

	if uid.Valid {
		var userInGroup bool
		err = c.DBtx.QueryRow(`select ($1, $2) in (select uid, groupid from user_group)`, uid, groupid).Scan(&userInGroup)
		if err != nil {
			log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
			return nil, apiErr
		}
		if !userInGroup {
			apiErr = append(apiErr, APIError{errors.New("user not a member of this group"), ErrorAPIRequirement})
			return nil, apiErr

		}
		if unitid.Valid {
			var userInUnit bool
			err = c.DBtx.QueryRow(`select count(*) > 0  from affiliation_unit_user_certificate
								   left join user_certificates using(dnid)
                                   where unitid = $1 and uid = $2`, unitid, uid).Scan(&userInUnit)
			if err != nil {
				log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
				apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
				return nil, apiErr
			}
			if !userInUnit {
				apiErr = append(apiErr, APIError{errors.New("user not a member of this experiment"), ErrorAPIRequirement})
				return nil, apiErr
			}
		}
	}

	_, err = c.DBtx.Exec(`insert into grid_fqan (fqan, unitid, mapped_user, mapped_group, last_updated) values ($1, $2, $3, $4, NOW())`, i[FQAN], unitid, uid, groupid)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, nil
}

func removeFQAN(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

	type jsonstatus struct {
		Status string `json:"ferry_status,omitempty"`
		Error  string `json:"ferry_error,omitempty"`
	}
	var inputErr []jsonstatus

	q := r.URL.Query()
	fqan := strings.TrimSpace(q.Get("fqan"))

	if fqan == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No fqan specified in http query.")
		inputErr = append(inputErr, jsonstatus{"", "No fqan specified."})
	}
	if len(inputErr) > 0 {
		jsonout, err := json.Marshal(inputErr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error(err)
		}
		fmt.Fprintf(w, string(jsonout))
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

	var aRows int64
	var res sql.Result
	res, err = DBtx.Exec("delete from grid_fqan where fqan = $1", fqan)
	if err == nil {
		aRows, _ = res.RowsAffected()
	} else {
		aRows = 0
	}

	var output interface{}
	if aRows == 1 {
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		output = jsonstatus{"success", ""}
		if cKey != 0 {
			DBtx.Commit(cKey)
		} else {
			return
		}
	} else {
		if aRows == 0 && err == nil {
			log.WithFields(QueryFields(r, startTime)).Error("FQAN doesn't exist.")
			output = jsonstatus{"", "FQAN doesn't exist."}
		} else {
			log.WithFields(QueryFields(r, startTime)).Error(err.Error())
			output = jsonstatus{"", err.Error()}
		}
	}

	out, err := json.Marshal(output)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err.Error())
	}
	fmt.Fprintf(w, string(out))
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
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
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
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, nil
}

func getAllAffiliationUnits(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	voname := strings.TrimSpace(q.Get("voname"))

	//	querystr := `select name, voms_url from affiliation_units where voms_url is not null`
	//	if voname != "" {
	//		querystr := `select name, voms_url from affiliation_units where voms_url is not null and voms_url like %$1%`
	//	}
	lastupdate, parserr := stringToParsedTime(strings.TrimSpace(q.Get("last_updated")))
	if parserr != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error parsing provided update time: " + parserr.Error())
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error parsing last_updated time. Check ferry logs. If provided, it should be an integer representing an epoch time.\"}")
		return
	}

	rows, err := DBptr.Query(`select name, url from affiliation_units as au left join voms_url as vu on au.unitid = vu.unitid
							  where url is not null and url like $1 and (au.last_updated>=$2 or $2 is null)`, "%"+voname+"%", lastupdate)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		log.WithFields(QueryFields(r, startTime)).Error("Error in DB query: " + err.Error())
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
		return
	}
	defer rows.Close()

	type jsonout struct {
		Uname string `json:"name"`
		//		Unitid int `json:"unitid"`
		Voms string `json:"voms_url,omitempty"`
	}

	var tmpout jsonout
	var Out []jsonout

	for rows.Next() {
		//	rows.Scan(&tmpout.Uname,&tmpout.Unitid)
		rows.Scan(&tmpout.Uname, &tmpout.Voms)
		Out = append(Out, tmpout)
	}

	var output interface{}
	if len(Out) == 0 {
		type jsonerror struct {
			Error string `json:"ferry_error"`
		}
		var queryErr []jsonerror
		queryErr = append(queryErr, jsonerror{"Query returned no units."})
		log.WithFields(QueryFields(r, startTime)).Error("Query returned no units.")
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
