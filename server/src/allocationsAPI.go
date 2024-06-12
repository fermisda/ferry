package main

import (
	"database/sql"
	"strings"

	_ "github.com/lib/pq"
	log "github.com/sirupsen/logrus"
)

// IncludeAllocationAPIs includes all APIs described in this file in an APICollection
func IncludeAllocationAPIs(c *APICollection) {

	createAllocation := BaseAPI{
		InputModel{
			Parameter{GroupName, true},
			Parameter{FiscalYear, true},
			Parameter{AllocationType, true},
			Parameter{AllocationClass, false},
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
			Parameter{AllocationClass, false},
			Parameter{OriginalHours, false},
			Parameter{UsedHours, false},
		},
		editAllocation,
		RoleWrite,
	}
	c.Add("editAllocation", &editAllocation)

	addAdjustment := BaseAPI{
		InputModel{
			Parameter{GroupName, true},
			Parameter{FiscalYear, true},
			Parameter{AllocationType, true},
			Parameter{AdjustedHours, true},
			Parameter{Comments, false},
		},
		addAdjustment,
		RoleWrite,
	}
	c.Add("addAdjustment", &addAdjustment)

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

	getAllocations := BaseAPI{
		InputModel{
			Parameter{GroupName, false},
			Parameter{FiscalYear, false},
			Parameter{AllocationType, false},
			Parameter{AllocationClass, false},
		},
		getAllocations,
		RoleRead,
	}

	c.Add("getAllocations", &getAllocations)
}

// createAllocation godoc
// @Summary      Adds a new allocation record.
// @Description  Adds a new allocation record.  There can be only one allocation for each unique combination of groupname, allocationtype, fiscalyear.
// @Tags         Allocations
// @Accept       html
// @Produce      json
// @Param        groupname       query     string  true   "name of the group the allocation is created for"
// @Param        allocationtype  query     string  true   "type of allocation to create - i.e. 'cpu' or 'gpu'"
// @Param        allocationclass query     string  false  "class of the allocation"
// @Param        originalhours   query     float64 true   "original number of hours assigned to allocation"
// @Param        fiscalyear      query     string  true   "the fiscal year YYYY assigned to the allocation"
// @Success      200  {object}   main.jsonOutput
// @Failure      400  {object}   main.jsonOutput
// @Failure      401  {object}   main.jsonOutput
// @Router /createAllocation [post]
func createAllocation(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

<<<<<<< HEAD
	if !isFiscalYearValid(i) {
		return nil, append(apiErr, DefaultAPIError(ErrorText, "fiscalyear must be YYYY"))
	}

=======
>>>>>>> 51b9b867fb60a86c0c6b3f12ec6117616aac8327
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

	_, err = c.DBtx.Exec(`insert into allocations (groupid, fiscal_year, type, original_hours, alloc_class)
						  values ($1, $2, $3, $4, $5)
						  on conflict (groupid, fiscal_year, type) do nothing`,
<<<<<<< HEAD
		groupid, i[FiscalYear], i[AllocationType], i[OriginalHours], i[AllocationClass])
=======
		groupid, i[FiscalYear], i[AllocationType], i[OriginalHours])
>>>>>>> 51b9b867fb60a86c0c6b3f12ec6117616aac8327
	if err != nil {
		if strings.Contains(err.Error(), "new row for relation \"allocations\" violates check constraint \"check_type\"") {
			log.WithFields(QueryFields(c)).Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorText, "invalid allocationtype"))
			return nil, apiErr
		} else {
			log.WithFields(QueryFields(c)).Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
			return nil, apiErr
		}
	}

	return nil, nil
}

// editAllocation godoc
// @Summary      Allows limited changes to an allocation record.
// @Description  Allows limited changes to an allocation record.
// @Tags         Allocations
// @Accept       html
// @Produce      json
<<<<<<< HEAD
// @Param        groupname       query     string  true   "name of the group to relate the allocation to"
// @Param        allocationtype  query     string  true   "type to set the allocation to - i.e. 'cpu' or 'gpu'"
// @Param        allocationclass query     string  false  "class to set the allocation to"
// @Param        fiscalyear      query     string  true   "the fiscal year YYYY assigned to the allocation"
// @Param        originalhours   query     float64 true   "original number of hours assigned to allocation"
// @Param        usedhours       query     float64 true   "number of hours used by the allocation"
// @Success      200  {object}   main.jsonOutput
// @Failure      400  {object}   main.jsonOutput
// @Failure      401  {object}   main.jsonOutput
=======
// @Param        groupname      query     string  true   "name of the group the allocation is created for"
// @Param        allocationtype query     string  true   "type of allocation to create - i.e. 'cpu' or 'gpu'"
// @Param        fiscalyear     query     string  true   "the fiscal year YYYY assigned to the allocation - default current year with format YYYY"
// @Param        originalhours  query     float64 false   "original number of hours assigned to allocation"
// @Param        usedhours      query     float64 false   "number of hours used by the allocation"
// @Success      200  {object}  main.jsonOutput
// @Failure      400  {object}  main.jsonOutput
// @Failure      401  {object}  main.jsonOutput
>>>>>>> 51b9b867fb60a86c0c6b3f12ec6117616aac8327
// @Router /editAllocation [post]
func editAllocation(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	if !isFiscalYearValid(i) {
		return nil, append(apiErr, DefaultAPIError(ErrorText, "fiscalyear must be YYYY"))
	}
	if !i[OriginalHours].Valid && !i[UsedHours].Valid && !i[AllocationClass].Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "one of originalhours, usedhours, allocationclass must be provided"))
		return nil, apiErr
	}

	groupid := NewNullAttribute(GroupID)
	var allocCnt int
	err := c.DBtx.QueryRow(`select (select groupid from groups where name=$1 and type='UnixGroup'),
							(select count(*) from allocations where type = $2)`, i[GroupName], i[AllocationType]).Scan(&groupid, &allocCnt)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	} else if !groupid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, GroupName))
		return nil, apiErr
	} else if allocCnt == 0 {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, AllocationType))
		return nil, apiErr
	}

	allocId := NewNullAttribute(GroupID)
	err = c.DBtx.QueryRow(`select allocid from allocations
						   where groupid=$1 and type=$2 and fiscal_year=$3`, groupid, i[AllocationType], i[FiscalYear]).Scan(&allocId)
<<<<<<< HEAD
	if err != nil {
=======
	if err != nil && err != sql.ErrNoRows {
>>>>>>> 51b9b867fb60a86c0c6b3f12ec6117616aac8327
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	} else if err == sql.ErrNoRows {
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "allocation record not found"))
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`update allocations set original_hours = coalesce($1, original_hours), used_hours = coalesce($2, used_hours),
	                        alloc_class = coalesce($3, alloc_class)
	                      where allocid = $4`, i[OriginalHours], i[UsedHours], i[AllocationClass], allocId)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	return nil, nil
}

// addAdjustment godoc
// @Summary      Records an adjustment to an allocation record.
// @Description  Records an adjustment to an allocation record, the record with the original hours is not changed.
// @Tags         Allocations
// @Accept       html
// @Produce      json
// @Param        groupname      query     string  true   "name of the group the adjustment is created for"
// @Param        allocationtype query     string  true   "type of the allocation against which the adjustment will be recorded - i.e. 'cpu' or 'gpu'"
<<<<<<< HEAD
// @Param        adjustedhours          query     float64 true   "number of hours to adjust the allocation by, can be positive or negitive"
// @Param        fiscalyear     query     string  true   "the fiscal year of the allocation being adjusted"
// @Param        comments       query     string  true   "optional comments about the adjustment"
=======
// @Param        adjustedhours  query     float64 true   "number of hours to adjust the allocation by, can be positive or negitive"
// @Param        fiscalyear     query     string  true   "the fiscal year of the allocation being adjusted - default is current year with format YYYY"
// @Param        comments       query     string  false   "optional comments about the adjustment"
>>>>>>> 51b9b867fb60a86c0c6b3f12ec6117616aac8327
// @Success      200  {object}  main.jsonOutput
// @Failure      400  {object}  main.jsonOutput
// @Failure      401  {object}  main.jsonOutput
// @Router /addAdjustment [put]
func addAdjustment(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

<<<<<<< HEAD
	if !isFiscalYearValid(i) {
		return nil, append(apiErr, DefaultAPIError(ErrorText, "fiscalyear must be YYYY"))
	}
=======
>>>>>>> 51b9b867fb60a86c0c6b3f12ec6117616aac8327
	groupid := NewNullAttribute(GroupID)
	var typeCnt int64
	err := c.DBtx.QueryRow(`select (select groupid from groups where name=$1 and type='UnixGroup'),
							(select count(*) from allocations where type = $2)`, i[GroupName], i[AllocationType]).Scan(&groupid, &typeCnt)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	} else if !groupid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, GroupName))
		return nil, apiErr
	} else if typeCnt == 0 {
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "invalid allocation type"))
		return nil, apiErr
	}

	allocid := NewNullAttribute(GroupID)
	err = c.DBtx.QueryRow(`select allocid from allocations
						   where groupid = $1
						     and fiscal_year = $2
							 and type = $3`, groupid, i[FiscalYear], i[AllocationType]).Scan(&allocid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	} else if !allocid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "allocation does not exist"))
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`insert into adjustments (allocid, create_date, hours_adjusted, comments)
							values ($1, now(), $2, $3)`, allocid.Data, i[AdjustedHours], i[Comments])
	if err != nil {
		if strings.Contains(err.Error(), "duplicate key value violates unique constraint \"pk_adjustments\"") {
			apiErr = append(apiErr, DefaultAPIError(ErrorDuplicateData, "create_date"))
		} else {
			log.WithFields(QueryFields(c)).Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		}
		return nil, apiErr
	}

	return nil, nil
}

// deleteAllocation godoc
// @Summary      Deletes an existing allocation from the database.  This is a non-recoverable operation.
// @Description  Deletes an existing allocation.  This is a non-recoverable operation.  The call will fail if any adjustments exist for the allocation. See deleteAdjustment.
// @Tags         Allocations
// @Accept       html
// @Produce      json
// @Param        groupname      query     string  true   "name of the group from which the allocation will be deleted"
// @Param        allocationtype query     string  true   "type of the allocation to be deleted - i.e. 'cpu' or 'gpu'"
// @Param        fiscalyear     query     string  true   "the fiscal year of the allocation to delete - format YYYY"
// @Success      200  {object}  main.jsonOutput
// @Failure      400  {object}  main.jsonOutput
// @Failure      401  {object}  main.jsonOutput
// @Router /deleteAllocation [put]
func deleteAllocation(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

<<<<<<< HEAD
	if !isFiscalYearValid(i) {
		return nil, append(apiErr, DefaultAPIError(ErrorText, "fiscalyear must be YYYY"))
	}
=======
>>>>>>> 51b9b867fb60a86c0c6b3f12ec6117616aac8327
	groupid := NewNullAttribute(GroupID)
	var typeCnt int64
	err := c.DBtx.QueryRow(`select (select groupid from groups where name=$1 and type='UnixGroup'),
								   (select count(*) from allocations where type = $2)`, i[GroupName], i[AllocationType]).Scan(&groupid, &typeCnt)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	} else if !groupid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, GroupName))
		return nil, apiErr
	} else if typeCnt == 0 {
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "invalid allocation type"))
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`delete from allocations
						  where groupid = $1
						     and type = $2
						     and fiscal_year = $3`, groupid.Data, i[AllocationType], i[FiscalYear])
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

// deleteAdjustment godoc
// @Summary      Deletes an existing adjustment from an allocation.  This is a non-recoverable operation.
// @Description  Deletes an existing adjustment from an allocation.  This is a non-recoverable operation.
// @Tags         Allocations
// @Accept       html
// @Produce      json
// @Param        groupname      query     string  true   "name of the group from which the adjustment will be deleted"
// @Param        allocationtype query     string  true   "type of the allocation from which the adjustment to be deleted - i.e. 'cpu' or 'gpu'"
// @Param        fiscalyear     query     string  true   "the fiscal year of the allocation from which the adjustment will be deleted - format YYYY"
// @Parm         createDate     query     string  true   "the date the adjustment to be deleted was created"
// @Success      200  {object}  main.jsonOutput
// @Failure      400  {object}  main.jsonOutput
// @Failure      401  {object}  main.jsonOutput
// @Router /deleteAdjustment [put]
func deleteAdjustment(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

<<<<<<< HEAD
	if !isFiscalYearValid(i) {
		return nil, append(apiErr, DefaultAPIError(ErrorText, "fiscalyear must be YYYY"))
	}
=======
>>>>>>> 51b9b867fb60a86c0c6b3f12ec6117616aac8327
	groupid := NewNullAttribute(GroupID)
	allocid := NewNullAttribute(GroupID)
	var typeCnt int64
	err := c.DBtx.QueryRow(`select (select groupid from groups where name=$1 and type='UnixGroup'),
								   (select count(*) from allocations where type = $2),
								   (select allocid from allocations as a
									join groups as g using (groupid)
									where g.name=$1
									  and g.type = 'UnixGroup'
									  and a.groupid = g.groupid
									  and a.type=$2
									  and a.fiscal_year=$3)`, i[GroupName], i[AllocationType], i[FiscalYear]).Scan(&groupid, &typeCnt, &allocid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	} else if !groupid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, GroupName))
		return nil, apiErr
	} else if i[AllocationType].Data != "cpu" && i[AllocationType].Data != "gpu" {
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "invalid allocation type"))
		return nil, apiErr
	} else if !allocid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "allocation not found"))
		return nil, apiErr
	} else if typeCnt == 0 {
		apiErr = append(apiErr, DefaultAPIError(ErrorText, "invalid allocation type"))
		return nil, apiErr
	}

	_, err = c.DBtx.Exec(`delete from adjustments
						  where allocid = $1
							 and create_date = $2`, allocid.Data, i[CreateDate])
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	return nil, nil
}

// getAllocations godoc
// @Summary      Returns allocations with all their adjustments.
// @Description  Returns allocations with all their adjustments.  A sum of the original hours with adjustments is provided.
// @Tags         Allocations
// @Accept       html
// @Produce      json
// @Param        groupname       query     string  false   "limits returned data to a specific group name"
// @Param        allocationtype  query     string  false   "limits returned data to allocations of a specific type - i.e. 'cpu' or 'gpu'"
// @Param        allocationclass query     string  false   "limits returned data to a allocations of a specific class"
// @Param        fiscalyear      query     string  false   "limits returned data to allocations for a specific fiscal year - format YYYY"
// @Success      200  {object}   main.allocations
// @Failure      400  {object}   main.jsonOutput
// @Failure      401  {object}   main.jsonOutput
// @Router /getAllocations [get]
func getAllocations(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	if !isFiscalYearValid(i) {
		return nil, append(apiErr, DefaultAPIError(ErrorText, "fiscalyear must be YYYY"))
	}

	gid := NewNullAttribute(GID)
	var typeCnt int64
	if i[GroupName].Valid {
		err := c.DBtx.QueryRow(`select (select gid from groups where name = $1),
					     		       (select count(*) from allocations where type = $2)`, i[GroupName], i[AllocationType]).Scan(&gid, &typeCnt)
		if err != nil {
			log.WithFields(QueryFields(c)).Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
			return nil, apiErr
		} else if !gid.Valid {
			apiErr = append(apiErr, DefaultAPIError(ErrorInvalidData, GroupName))
			return nil, apiErr
		} else if typeCnt == 0 {
			apiErr = append(apiErr, DefaultAPIError(ErrorText, "invalid allocation type"))
			return nil, apiErr
		}
	}

	rows, err := c.DBtx.Query(`select g.name, g.gid, a.fiscal_year, a.type, a.alloc_class, a.original_hours, a.used_hours,
								 aj.create_date, aj.hours_adjusted, aj.comments
							   from groups as g
								 join allocations as a using (groupid)
								 left outer join adjustments as aj using (allocid)
							   where (g.gid = $1 or $1 is null)
							     and (a.fiscal_year = $2 or $2 is null)
								 and (a.type = $3 or $3 is null)
								 and (a.alloc_class = $4 or $4 is null)
							   order by g.name, a.fiscal_year desc, a.type asc, aj.create_date desc`, gid, i[FiscalYear], i[AllocationType], i[AllocationClass])
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	defer rows.Close()

	out := make([]allocations, 0)
	var curAlloc allocations
	firstRec := true
	for rows.Next() {
		var allocEntry allocations
		var adjEntry adjustments
		rows.Scan(&allocEntry.GroupName, &allocEntry.GID, &allocEntry.FiscalYear, &allocEntry.AllocationType, &allocEntry.AllocationClass, &allocEntry.OriginalHours,
			&allocEntry.UsedHours, &adjEntry.CreateDate, &adjEntry.AdjustedHours, &adjEntry.Comments)

		// TODO determine why CreateDate is coming back with T00:00:00Z. For now, hack it.
		adjEntry.CreateDate = strings.Split(adjEntry.CreateDate, "T")[0]
		if firstRec {
			curAlloc.GroupName = allocEntry.GroupName
			curAlloc.GID = allocEntry.GID
			curAlloc.FiscalYear = allocEntry.FiscalYear
			curAlloc.AllocationType = allocEntry.AllocationType
			curAlloc.AllocationClass = allocEntry.AllocationClass
			curAlloc.OriginalHours = allocEntry.OriginalHours
			curAlloc.UsedHours = allocEntry.UsedHours
			curAlloc.AdjustedHours = allocEntry.OriginalHours
			if len(adjEntry.CreateDate) > 0 {
				curAlloc.Adjustments = append(curAlloc.Adjustments, adjEntry)
				curAlloc.AdjustedHours = curAlloc.AdjustedHours + adjEntry.AdjustedHours
			}
			firstRec = false
		} else if (curAlloc.GID != allocEntry.GID) || (curAlloc.FiscalYear != allocEntry.FiscalYear) ||
			(curAlloc.AllocationType != allocEntry.AllocationType) {
			out = append(out, curAlloc)
			var n []adjustments
			curAlloc.Adjustments = n
			curAlloc.AdjustedHours = allocEntry.OriginalHours
			curAlloc.UsedHours = allocEntry.UsedHours
			curAlloc.GroupName = allocEntry.GroupName
			curAlloc.GID = allocEntry.GID
			curAlloc.FiscalYear = allocEntry.FiscalYear
			curAlloc.AllocationType = allocEntry.AllocationType
			curAlloc.AllocationClass = allocEntry.AllocationClass
			curAlloc.OriginalHours = allocEntry.OriginalHours
			if len(adjEntry.CreateDate) > 0 {
				curAlloc.Adjustments = append(curAlloc.Adjustments, adjEntry)
				curAlloc.AdjustedHours = curAlloc.AdjustedHours + adjEntry.AdjustedHours
			}
		} else if len(adjEntry.CreateDate) > 0 {
			curAlloc.Adjustments = append(curAlloc.Adjustments, adjEntry)
			curAlloc.AdjustedHours = curAlloc.AdjustedHours + adjEntry.AdjustedHours
		}
	}
	if curAlloc.GID > 0 {
		out = append(out, curAlloc)
	}
	return out, nil
}
