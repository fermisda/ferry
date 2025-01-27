package main

import (
	"database/sql"
	"strings"
	"time"

	_ "github.com/lib/pq"
	log "github.com/sirupsen/logrus"
)

// IncludeAllocationAPIs includes all APIs described in this file in an APICollection
func IncludeAllocationAPIs(c *APICollection) {

	createProject := BaseAPI{
		InputModel{
			Parameter{GroupName, true},
			Parameter{FiscalYear, true},
			Parameter{ProjectClass, false},
			Parameter{Email, false},
			Parameter{Piname, false},
		},
		createProject,
		RoleWrite,
	}
	c.Add("createProject", &createProject)

	editProject := BaseAPI{
		InputModel{
			Parameter{GroupName, true},
			Parameter{FiscalYear, true},
			Parameter{ProjectClass, false},
			Parameter{Email, false},
			Parameter{Piname, false},
		},
		editProject,
		RoleWrite,
	}
	c.Add("editProject", &editProject)

	deleteProject := BaseAPI{
		InputModel{
			Parameter{GroupName, true},
			Parameter{FiscalYear, true},
			Parameter{ProjectClass, false},
			Parameter{Email, false},
			Parameter{Piname, false},
		},
		deleteProject,
		RoleWrite,
	}
	c.Add("deleteProject", &deleteProject)

	createAllocation := BaseAPI{
		InputModel{
			Parameter{GroupName, true},
			Parameter{FiscalYear, true},
			Parameter{AllocationType, true},
			Parameter{OriginalHours, true},
		},
		createAllocation,
		RoleWrite,
	}
	c.Add("createAllocation", &createAllocation)

	editAllocation := BaseAPI{
		InputModel{
			Parameter{GroupName, true},
			Parameter{FiscalYear, true},
			Parameter{AllocationType, true},
			Parameter{OriginalHours, false},
			Parameter{UsedHours, false},
		},
		editAllocation,
		RoleWrite,
	}
	c.Add("editAllocation", &editAllocation)

	deleteAllocation := BaseAPI{
		InputModel{
			Parameter{GroupName, true},
			Parameter{FiscalYear, true},
			Parameter{AllocationType, true},
		},
		deleteAllocation,
		RoleWrite,
	}
	c.Add("deleteAllocation", &deleteAllocation)

	addAdjustment := BaseAPI{
		InputModel{
			Parameter{GroupName, true},
			Parameter{FiscalYear, true},
			Parameter{AllocationType, true},
			Parameter{CreateDate, false},
			Parameter{AdjustedHours, true},
			Parameter{Comments, false},
		},
		addAdjustment,
		RoleWrite,
	}
	c.Add("addAdjustment", &addAdjustment)

	deleteAdjustment := BaseAPI{
		InputModel{
			Parameter{GroupName, true},
			Parameter{FiscalYear, true},
			Parameter{AllocationType, true},
			Parameter{CreateDate, true},
		},
		deleteAdjustment,
		RoleWrite,
	}
	c.Add("deleteAdjustment", &deleteAdjustment)

	getProjects := BaseAPI{
		InputModel{
			Parameter{GroupName, false},
			Parameter{FiscalYear, false},
			Parameter{AllocationType, false},
			Parameter{ProjectClass, false},
		},
		getProjects,
		RoleRead,
	}
	c.Add("getProjects", &getProjects)
}

// createProject godoc
// @Summary      Adds a new project record.
// @Description  Adds a new project record.  There can be only one project for each unique combination of groupname and fiscalyear.
// @Tags         Projects
// @Accept       html
// @Produce      json
// @Param        groupname       query     string  true   "name of the group the project is created for"
// @Param        projectclass    query     string  false  "class of the project"
// @Param        fiscalyear      query     string  true   "the fiscal year YYYY assigned to the project"
// @Param        piname          query     string  false  "name of the principal investigator, point of contact for the project"
// @Param        email           query     string  false  "email address for the point of contact"
// @Router /createProject [post]
func createProject(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	if !isFiscalYearValid(i) {
		return nil, append(apiErr, DefaultAPIError(ErrorText, "fiscalyear must be YYYY"))
	}

	groupid := NewNullAttribute(GroupID)
	err := c.DBtx.QueryRow(`select groupid from groups where name=$1 and type='UnixGroup'`, i[GroupName]).Scan(&groupid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	} else if !groupid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, GroupName))
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`insert into projects (groupid, fiscal_year, project_class, email, piname)
						  values ($1, $2, $3, $4, $5)`,
		groupid, i[FiscalYear], i[ProjectClass], i[Email], i[Piname])
	if err != nil {
		if strings.Contains(err.Error(), "duplicate key value violates unique constraint \"unq_projects\"") {
			log.WithFields(QueryFields(c)).Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorDuplicateData, "project"))
			return nil, apiErr
		} else {
			log.WithFields(QueryFields(c)).Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
			return nil, apiErr
		}
	}

	return nil, nil
}

// editProject godoc
// @Summary      Allows limited changes to a project.
// @Description  Allows limited changes to a project.
// @Tags         Projects
// @Accept       html
// @Produce      json
// @Param        groupname       query     string  true   "name of the group to relate the project to"
// @Param        projectclass    query     string  false  "class to set the project to"
// @Param        fiscalyear      query     string  true   "the fiscal year YYYY assigned to the allocation"
// @Param        piname          query     string  false  "name of the irincipal investigator, point of contact for the project"
// @Param        email           query     string  false  "email address for the point of contact"
// @Router /editProject [post]
func editProject(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	if !isFiscalYearValid(i) {
		return nil, append(apiErr, DefaultAPIError(ErrorText, "fiscalyear must be YYYY"))
	}
	if !i[ProjectClass].Valid && !i[Piname].Valid && !i[Email].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "at least one parameter to change must be provided"))
		return nil, apiErr
	}

	groupid := NewNullAttribute(GroupID)
	err := c.DBtx.QueryRow(`select (select groupid from groups where name=$1 and type='UnixGroup')`, i[GroupName]).Scan(&groupid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	} else if !groupid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, GroupName))
		return nil, apiErr
	}

	projId := NewNullAttribute(GroupID)
	err = c.DBtx.QueryRow(`select projid from projects where groupid=$1 and fiscal_year=$2`, groupid, i[FiscalYear]).Scan(&projId)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	} else if err == sql.ErrNoRows {
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "project not found"))
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`update projects set project_class = coalesce($1, project_class), email = coalesce($2, email), piname = coalesce($3, piname)
	                      where projid = $4`, i[ProjectClass], i[Email], i[Piname], projId)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	return nil, nil
}

// deleteProject godoc
// @Summary      Deletes an existing project from the database.  This is a non-recoverable operation.
// @Description  Deletes an existing project.  This is a non-recoverable operation.  The call will fail if any allocations exist for the project.
// @Tags         Projects
// @Accept       html
// @Produce      json
// @Param        groupname      query     string  true   "name of the group from which the project will be deleted"
// @Param        fiscalyear     query     string  true   "the fiscal year of the project to be deleted - format YYYY"
// @Router /deleteProject [put]
func deleteProject(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	if !isFiscalYearValid(i) {
		return nil, append(apiErr, DefaultAPIError(ErrorText, "fiscalyear must be YYYY"))
	}
	groupid := NewNullAttribute(GroupID)
	err := c.DBtx.QueryRow(`select groupid from groups where name=$1 and type='UnixGroup'`, i[GroupName]).Scan(&groupid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	} else if !groupid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, GroupName))
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`delete from projects
						  where groupid = $1
						     and fiscal_year = $2`, groupid.Data, i[FiscalYear])
	if err != nil {
		if strings.Contains(err.Error(), "update or delete on table \"projects\" violates foreign key constraint \"fk_allocations_projects\"") {
			apiErr = append(apiErr, DefaultAPIError(ErrorText, "cannot delete project, allocations exist"))
		} else {
			log.WithFields(QueryFields(c)).Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		}
		return nil, apiErr
	}

	return nil, nil
}

// createAllocation godoc
// @Summary      Adds a new allocation to a project.
// @Description  Adds a new allocation to a project.  Each project can only have one allocation of a specific type for a fiscal year.
// @Tags         Projects
// @Accept       html
// @Produce      json
// @Param        groupname       query     string  true   "name of the group for the project's allocation"
// @Param        fiscalyear      query     string  true   "the fiscal year YYYY assigned project's allocation"
// @Param        allocationtype  query     string  true   "type of the project's allocation - i.e. 'cpu' or 'gpu'"
// @Param        originalhours   query     string  true   "the number of hours orignally assigned to the allocation/type for the fiscal year"
// @Router /createAllocation [post]
func createAllocation(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	if !isFiscalYearValid(i) {
		return nil, append(apiErr, DefaultAPIError(ErrorText, "fiscalyear must be YYYY"))
	}
	groupid := NewNullAttribute(GroupID)
	projid := NewNullAttribute(GroupID)
	err := c.DBtx.QueryRow(`select (select groupid from groups where name=$1 and type='UnixGroup'),
								   (select projid from projects where fiscal_year=$2 and groupid = (
								       select groupid from groups where name=$1 and type='UnixGroup'))`, i[GroupName], i[FiscalYear]).Scan(&groupid, &projid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	} else if !groupid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, GroupName))
		return nil, apiErr
	} else if !projid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "project not found for groupname with the supplied fiscalyear"))
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`insert into allocations (projid, type, original_hours) values ($1, $2, $3)`,
		projid, i[AllocationType], i[OriginalHours])
	if err != nil {
		if strings.Contains(err.Error(), "duplicate key value violates unique constraint \"unq_allocations\"") {
			log.WithFields(QueryFields(c)).Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorDuplicateData, "allocation"))
			return nil, apiErr
		} else if strings.Contains(err.Error(), "new row for relation \"allocations\" violates check constraint \"check_type\"") {
			log.WithFields(QueryFields(c)).Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorInvalidData, AllocationType))
			return nil, apiErr
		} else {
			log.WithFields(QueryFields(c)).Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
			return nil, apiErr
		}
	}

	return nil, apiErr
}

// editAllocation godoc
// @Summary      Allows limited changes to an allocation.
// @Description  Allows limited changes to an allocation.
// @Tags         Projects
// @Accept       html
// @Produce      json
// @Param        groupname       query     string  true   "name of the group for the project's allocation"
// @Param        fiscalyear      query     string  true   "the fiscal year YYYY assigned project's allocation"
// @Param        allocationtype  query     string  true   "type of allocation for the project - i.e. 'cpu' or 'gpu'"
// @Param        originalhours   query     string  true   "the number of hours orignally assigned to the allocation"
// @Param        usedhours       query     string  true   "number of the allocations's hours that have been used"
// @Router /editAllocation [post]
func editAllocation(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	if !isFiscalYearValid(i) {
		return nil, append(apiErr, DefaultAPIError(ErrorText, "fiscalyear must be YYYY"))
	}

	if !i[OriginalHours].Valid && !i[UsedHours].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "at least one parameter to change must be provided"))
		return nil, apiErr
	}

	groupid := NewNullAttribute(GroupID)
	projid := NewNullAttribute(GroupID)
	err := c.DBtx.QueryRow(`select (select groupid from groups where name=$1 and type='UnixGroup'),
								   (select projid from projects where fiscal_year=$2 and groupid = (
								       select groupid from groups where name=$1 and type='UnixGroup'))`, i[GroupName], i[FiscalYear]).Scan(&groupid, &projid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	} else if !groupid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, GroupName))
		return nil, apiErr
	} else if !projid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "project not found for group/fiscal year"))
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`update allocations set original_hours = coalesce($1, original_hours), used_hours = coalesce($2, used_hours) where projid=$3 and type=$4`,
		i[OriginalHours], i[UsedHours], projid, i[AllocationType])
	if err != nil {
		if strings.Contains(err.Error(), "duplicate key value violates unique constraint \"unq_allocations\"") {
			log.WithFields(QueryFields(c)).Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorDuplicateData, "allocation"))
			return nil, apiErr
		} else {
			log.WithFields(QueryFields(c)).Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
			return nil, apiErr
		}
	}

	return nil, apiErr
}

// deleteAllocation godoc
// @Summary      Deletes an existing allocation from the database.  This is a non-recoverable operation.
// @Description  Deletes an existing allocation.  This is a non-recoverable operation.  The call will fail if any adjustments exist for the allocation.
// @Tags         Projects
// @Accept       html
// @Produce      json
// @Param        groupname      query     string  true   "name of the group from which the allocation will be deleted"
// @Param        fiscalyear     query     string  true   "the fiscal year of the allocation to delete - format YYYY"
// @Param        allocationtype query     string  true   "type of the allocation to be deleted - i.e. 'cpu' or 'gpu'"
// @Router /deleteAllocation [put]
func deleteAllocation(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	if !isFiscalYearValid(i) {
		return nil, append(apiErr, DefaultAPIError(ErrorText, "fiscalyear must be YYYY"))
	}
	groupid := NewNullAttribute(GroupID)
	projid := NewNullAttribute(GroupID)
	allocExists := NewNullAttribute(AllocationType)
	err := c.DBtx.QueryRow(`select (select groupid from groups where name=$1 and type='UnixGroup'),
								   (select projid from projects where fiscal_year=$2 and groupid = (
								       select groupid from groups where name=$1 and type='UnixGroup')),
								   (select 'exists' from allocations where type=$3 limit 1)`,
		i[GroupName], i[FiscalYear], i[AllocationType]).Scan(&groupid, &projid, &allocExists)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	} else if !groupid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, GroupName))
		return nil, apiErr
	} else if !projid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "project not found for group/fiscal year"))
		return nil, apiErr
	} else if !allocExists.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorInvalidData, AllocationType))
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`delete from allocations
						  where projid = $1
						     and type = $2`, projid, i[AllocationType])
	if err != nil {
		if strings.Contains(err.Error(), "update or delete on table \"allocations\" violates foreign key constraint \"fk_adjustments_allocations\"") {
			apiErr = append(apiErr, DefaultAPIError(ErrorText, "cannot delete, adjustments exist"))
		} else {
			log.WithFields(QueryFields(c)).Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		}
		return nil, apiErr
	}

	return nil, nil
}

// addAdjustment godoc
// @Summary      Records an adjustment to an allocation record.
// @Description  Records an adjustment to an allocation record, the record with the original hours is not changed.
// @Tags         Projects
// @Accept       html
// @Produce      json
// @Param        groupname      query     string  true   "name of the group the adjustment is created for"
// @Param        fiscalyear     query     string  true   "the fiscal year of the allocation being adjusted"
// @Param        allocationtype query     string  true   "type of the allocation against which the adjustment will be recorded - i.e. 'cpu' or 'gpu'"
// @Param        createdate     query     string  false  "the date for the adjustment - the default is today"
// @Param        adjustedhours  query     float64 true   "number of hours to adjust the allocation by, can be positive or negitive"
// @Param        comments       query     string  true   "optional comments about the adjustment"
// @Router /addAdjustment [put]
func addAdjustment(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	if !isFiscalYearValid(i) {
		return nil, append(apiErr, DefaultAPIError(ErrorText, "fiscalyear must be YYYY"))
	}
	groupid := NewNullAttribute(GroupID)
	projid := NewNullAttribute(GroupID)
	allocExists := NewNullAttribute(AllocationType)
	err := c.DBtx.QueryRow(`select (select groupid from groups where name=$1 and type='UnixGroup'),
								   (select projid from projects where fiscal_year=$2 and groupid =
								       (select groupid from groups where name=$1 and type='UnixGroup')),
								   (select 'exists' from allocations where type=$3 limit 1)`,
		i[GroupName], i[FiscalYear], i[AllocationType]).Scan(&groupid, &projid, &allocExists)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	} else if !groupid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, GroupName))
		return nil, apiErr
	} else if !projid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "project not found for group/fiscal year"))
		return nil, apiErr
	} else if !allocExists.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorInvalidData, AllocationType))
		return nil, apiErr
	}

	allocid := NewNullAttribute(GroupID)
	err = c.DBtx.QueryRow(`select allocid from allocations where projid = $1 and type = $2`, projid, i[AllocationType]).Scan(&allocid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	} else if !allocid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "allocation does not exist"))
		return nil, apiErr
	}

	// Use CreateDate if passed in otherwise create createDate with today's date.
	createDate := time.Now().Format(DateFormat)
	if i[CreateDate].Valid {
		parsedValue, _ := i[CreateDate].Data.(time.Time)
		createDate = parsedValue.Format(DateFormat)
	}

	_, err = c.DBtx.Exec(`insert into adjustments (allocid, create_date, hours_adjusted, comments)
							values ($1, $2, $3, $4)`, allocid.Data, createDate, i[AdjustedHours], i[Comments])
	if err != nil {
		if strings.Contains(err.Error(), "duplicate key value violates unique constraint \"pk_adjustments\"") {
			apiErr = append(apiErr, DefaultAPIError(ErrorText, "adjustment already exists for createdate"))
		} else {
			log.WithFields(QueryFields(c)).Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		}
		return nil, apiErr
	}

	return nil, nil
}

// deleteAdjustment godoc
// @Summary      Deletes an existing adjustment from an allocation.  This is a non-recoverable operation.
// @Description  Deletes an existing adjustment from an allocation.  This is a non-recoverable operation.
// @Tags         Projects
// @Accept       html
// @Produce      json
// @Param        groupname      query     string  true   "name of the group from which the adjustment will be deleted"
// @Param        fiscalyear     query     string  true   "the fiscal year of the allocation from which the adjustment will be deleted - format YYYY"
// @Param        allocationtype query     string  true   "type of the allocation from which the adjustment to be deleted - i.e. 'cpu' or 'gpu'"
// @Parm         createDate     query     string  true   "the date the adjustment to be deleted was created"
// @Router /deleteAdjustment [put]
func deleteAdjustment(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	if !isFiscalYearValid(i) {
		return nil, append(apiErr, DefaultAPIError(ErrorText, "fiscalyear must be YYYY"))
	}
	groupid := NewNullAttribute(GroupID)
	projid := NewNullAttribute(GroupID)
	allocExists := NewNullAttribute(AllocationType)
	err := c.DBtx.QueryRow(`select (select groupid from groups where name=$1 and type='UnixGroup'),
								   (select projid from projects where fiscal_year=$2 and groupid =
								   		(select groupid from groups where name=$1 and type='UnixGroup')),
								   (select 'exists' from allocations where type=$3 limit 1)`,
		i[GroupName], i[FiscalYear], i[AllocationType]).Scan(&groupid, &projid, &allocExists)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	} else if !groupid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, GroupName))
		return nil, apiErr
	} else if !projid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "project not found for group/fiscal year"))
		return nil, apiErr
	} else if !allocExists.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorInvalidData, AllocationType))
		return nil, apiErr
	}

	allocid := NewNullAttribute(GroupID)
	err = c.DBtx.QueryRow(`select allocid from allocations where projid = $1 and type = $2`, projid, i[AllocationType]).Scan(&allocid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	} else if !allocid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "allocation does not exist"))
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`delete from adjustments where allocid = $1 and create_date = $2`, allocid.Data, i[CreateDate])
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, nil
}

// getProjects godoc
// @Summary      Returns projects with all their allocations and respective adjustments.
// @Description  Returns projects with all their allocations and respective adjustments.  A sum of the original hours with adjustments is provided.
// @Tags         Projects
// @Accept       html
// @Produce      json
// @Param        groupname       query     string  false   "limits returned data to a specific group"
// @Param        fiscalyear      query     string  false   "limits returned data to projects of a specific fiscal year - format YYYY"
// @Param        projectclass    query     string  false   "limits returned data to projects allocations of a specific class"
// @Param        allocationtype  query     string  false   "limits all project's allocation data returned to a specific type - i.e. 'cpu' or 'gpu'"
// @Router /getProjects [get]
func getProjects(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	if !isFiscalYearValid(i) {
		return nil, append(apiErr, DefaultAPIError(ErrorText, "fiscalyear must be YYYY"))
	}

	groupid := NewNullAttribute(GroupID)
	projid := NewNullAttribute(GroupID)
	allocExists := NewNullAttribute(AllocationType)
	err := c.DBtx.QueryRow(`select (select groupid from groups where name=$1 and type='UnixGroup'),
								   (select projid from projects where fiscal_year=$2 limit 1),
								   (select 'exists' from allocations where type=$3 limit 1)`,
		i[GroupName], i[FiscalYear], i[AllocationType]).Scan(&groupid, &projid, &allocExists)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	} else if i[GroupName].Valid && !groupid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, GroupName))
		return nil, apiErr
	} else if i[FiscalYear].Valid && !projid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, FiscalYear))
		return nil, apiErr
	} else if i[AllocationType].Valid && !allocExists.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, AllocationType))
		return nil, apiErr
	}

	rows, err := c.DBtx.Query(`select g.name, p.projid, p.fiscal_year, p.project_class, p.piname, p.email,
							   a.type, a.original_hours, a.used_hours, a.last_updated,
							   aj.create_date, aj.hours_adjusted, aj.comments
							   from projects as p
							     join groups as g using (groupid)
							     left outer join allocations as a using (projid)
							     left outer join adjustments as aj using (allocid)
							   where (g.name = $1 or $1 is null)
							     and (p.fiscal_year = $2 or $2 is null)
							     and (p.project_class = $3 or $3 is null)
							     and (a.type = $4 or $4 is null)
							    order by g.name, p.fiscal_year desc, a.type asc, aj.create_date desc`,
		i[GroupName], i[FiscalYear], i[ProjectClass], i[AllocationType])
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	const Allocations Attribute = "allocations"
	const Adjustments Attribute = "adjustments"
	const NetHours Attribute = "nethours"

	type jsonAdj map[Attribute]interface{}
	type jsonAlloc map[Attribute]interface{}
	type jsonProj map[Attribute]interface{}
	var out []jsonProj
	var projAlloc []jsonAlloc
	var allocAdj []jsonAdj

	proj := make(jsonProj)
	alloc := make(jsonAlloc)

	row := NewMapNullAttribute(GroupName, GID, FiscalYear, ProjectClass, Piname, Email,
		AllocationType, OriginalHours, UsedHours, LastUpdated,
		CreateDate, AdjustedHours, Comments)

	prevProjId := NewNullAttribute(GID) // There is no ProjID and I can't see making one for this.
	prevType := NewNullAttribute(AllocationType)
	for rows.Next() {
		rows.Scan(row[GroupName], row[GID], row[FiscalYear], row[ProjectClass], row[Piname], row[Email],
			row[AllocationType], row[OriginalHours], row[UsedHours], row[LastUpdated],
			row[CreateDate], row[AdjustedHours], row[Comments])
		if prevProjId != *row[GID] {
			proj = make(jsonProj)
			proj[GroupName] = row[GroupName].Data
			proj[FiscalYear] = row[FiscalYear].Data
			proj[ProjectClass] = row[ProjectClass].Data
			proj[Piname] = row[Piname].Data
			proj[Email] = row[Email].Data
			proj[Allocations] = make([]jsonAlloc, 0)
			out = append(out, proj)
			prevProjId = *row[GID]
			projAlloc = nil
		}
		if (prevType != *row[AllocationType]) && row[AllocationType].Valid {
			alloc = make(jsonAlloc)
			alloc[AllocationType] = row[AllocationType].Data
			alloc[OriginalHours] = row[OriginalHours].Data
			alloc[UsedHours] = row[UsedHours].Data
			parsedValue, _ := row[LastUpdated].Data.(time.Time)
			alloc[LastUpdated] = parsedValue.Format(DateFormat)
			alloc[Adjustments] = make([]jsonAdj, 0)
			alloc[NetHours] = row[OriginalHours].Data.(float64) - row[UsedHours].Data.(float64)
			projAlloc = append(projAlloc, alloc)
			proj[Allocations] = projAlloc
			prevType = *row[AllocationType]
			allocAdj = nil
		}
		if row[CreateDate].Valid {
			adj := make(jsonAdj)
			parsedValue, _ := row[CreateDate].Data.(time.Time)
			adj[CreateDate] = parsedValue.Format(DateFormat)
			adj[AdjustedHours] = row[AdjustedHours].Data
			adj[Comments] = row[Comments].Data
			alloc[NetHours] = alloc[NetHours].(float64) + row[AdjustedHours].Data.(float64)
			allocAdj = append(allocAdj, adj)
			alloc[Adjustments] = allocAdj

		}
	}

	return out, nil
}
