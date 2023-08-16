
// Code is here in case I ever figure this out.
// ISSUE:  Users are assigned to an experiment through the group but groups can belong to more then one
// experiment.  So, once assigned what experiment does the user really belong to?   The user_groups table
// should possibly have been user_group_experiments table.   Then we would really know what experiment a
// user is in.   Hindsight is so much easier...

// What is here  will work just fine.  The issue is how to remove the user from the group as
// The DB associates the group with many experiments.  i.e.  what experiments is the user really
// part of.  There is no way the code can tell that.


// For main.go
grouter.HandleFunc("/removeUserFromExperiment", APIs["removeUserFromExperiment"].Run)

// For wrapperAPI.go
removeUserFromExperiment := BaseAPI{
	InputModel{
		Parameter{UserName, true},
		Parameter{UnitName, true},
	},
	removeUserFromExperiment,
	RoleWrite,
}
c.Add("removeUserFromExperiment", &removeUserFromExperiment)

// removeUserFromExperiment godoc
// @Summary      Removes a user from an experiment.
// @Description  Removes a user from an experiment; including all relationships to the experiment's resources, groups and FQANs from the specified user. The user will remain a member of any other experiment they are in.  (see banUser to immediately and permanently terminate all the user's FERRY access)
// @Tags         Users
// @Accept       html
// @Produce      json
// @Param        unitname       query     string  true  "name of the experiment to remove the user from"
// @Param        username       query     string  true  "user name of the user to remove from the experiment"
// @Success      200  {object}  jsonOutput
// @Failure      400  {object}  jsonOutput
// @Failure      401  {object}  jsonOutput
// @Router /removeUserFromExperiment [put]
func removeUserFromExperiment(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	unitid := NewNullAttribute(UnitName)
	uid := NewNullAttribute(UID)

	err := c.DBtx.QueryRow("select uid from users where uname = $1", i[UserName]).Scan(&uid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	err = c.DBtx.QueryRow("select unitid from affiliation_units where name = $1", i[UnitName]).Scan(&unitid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
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

	// Remove user from all compute resources for this experiment
	_, err = c.DBtx.Exec(`delete from compute_access_group
	                      where uid=$1
	                        and compid in (select compid
					                       from compute_resources
					                       where unitid=$2)`, uid, unitid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	_, err = c.DBtx.Exec(`delete from compute_access
	                      where uid=$1
	                        and compid in (select compid
					                       from compute_resources
					                       where unitid=$2)`, uid, unitid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	// Remove the FQANs assigned to the user for this exp
	_, err = c.DBtx.Exec(`delete from grid_access
						  where uid = $1
	  						and fqanid in (select fqanid
										   from grid_fqan
										   where unitid = $2)`, uid, unitid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	// Remove the user's certs from this exp
	_, err = c.DBtx.Exec(`delete from affiliation_unit_user_certificate
						  where unitid = $1
	  						and dnid in (select dnid
		  								 from user_certificates
		  								 where uid = $2)`, unitid, uid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	//Storage Resources
	_, err = c.DBtx.Exec(`delete from storage_quota
						  where unitid = $1 and uid = $2)`, unitid, uid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	//User Groups

	return nil, nil
}
