package main

import (
	"database/sql"
	"errors"
	"fmt"

	_ "github.com/lib/pq"
	log "github.com/sirupsen/logrus"
)

// IncludeWrapperAPIs includes all APIs described in this file in an APICollection
func IncludeWrapperAPIs(c *APICollection) {
	addUserToExperiment := BaseAPI{
		InputModel{
			Parameter{UserName, true},
			Parameter{UnitName, true},
		},
		addUserToExperiment,
		RoleWrite,
	}
	c.Add("addUserToExperiment", &addUserToExperiment)

	addLPCCollaborationGroup := BaseAPI{
		InputModel{
			Parameter{GroupName, true},
			Parameter{Quota, true},
			Parameter{QuotaUnit, false},
		},
		addLPCCollaborationGroup,
		RoleWrite,
	}
	c.Add("addLPCCollaborationGroup", &addLPCCollaborationGroup)

	setLPCStorageAccess := BaseAPI{
		InputModel{
			Parameter{UserName, true},
			Parameter{DN, true},
			Parameter{ExternalUsername, true},
		},
		setLPCStorageAccess,
		RoleWrite,
	}
	c.Add("setLPCStorageAccess", &setLPCStorageAccess)

	addLPCConvener := BaseAPI{
		InputModel{
			Parameter{UserName, true},
			Parameter{GroupName, true},
		},
		addLPCConvener,
		RoleWrite,
	}
	c.Add("addLPCConvener", &addLPCConvener)

	removeLPCConvener := BaseAPI{
		InputModel{
			Parameter{UserName, true},
			Parameter{GroupName, true},
			Parameter{RemoveGroup, false},
		},
		removeLPCConvener,
		RoleWrite,
	}
	c.Add("removeLPCConvener", &removeLPCConvener)

	createExperiment := BaseAPI{
		InputModel{
			Parameter{UnitName, true},
			Parameter{VOMSURL, false},
			Parameter{HomeDir, false},
			Parameter{UserName, false},
			Parameter{GroupName, false},
			Parameter{Standalone, false},
		},
		createExperiment,
		RoleWrite,
	}
	c.Add("createExperiment", &createExperiment)

	testWrapper := BaseAPI{
		nil,
		testWrapper,
		RolePublic,
	}
	c.Add("testWrapper", &testWrapper)
}

func testWrapper(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	return "this is a test wrapper", apiErr
}

func addUserToExperiment(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	dnTemplate := "/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=%s/CN=UID:%s"
	fullName := NewNullAttribute(FullName)
	status := NewNullAttribute(Status)
	unitid := NewNullAttribute(UnitID)

	err := c.DBtx.QueryRow("select full_name, status from users where uname = $1", i[UserName]).Scan(&fullName, &status)
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

	if !status.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UserName))
	}
	if !unitid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, UnitName))
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	if !status.Data.(bool) {
		apiErr = append(apiErr, APIError{errors.New("user status is not valid"), ErrorAPIRequirement})
		return nil, apiErr
	}

	dn := NewNullAttribute(DN)
	dn.Scan(fmt.Sprintf(dnTemplate, fullName.Data.(string), i[UserName].Data.(string)))

	fnalUnit := NewNullAttribute(UnitName).Default("fermilab")
	for _, unitname := range []NullAttribute{i[UnitName], fnalUnit} {
		input := Input{
			UserName: i[UserName],
			UnitName: unitname,
			DN:       dn,
		}
		_, apiErr = addCertificateDNToUser(c, input)
		if len(apiErr) > 0 {
			return nil, apiErr
		}
	}

	for _, r := range []string{"Analysis", "NULL"} {
		role := NewNullAttribute(Role)
		role.Scan(r)

		input := Input{
			UserName: i[UserName],
			UnitName: i[UnitName],
			Role:     role,
			FQAN:     NewNullAttribute(FQAN),
		}

		_, apiErr = setUserExperimentFQAN(c, input)
		if len(apiErr) > 0 {
			return nil, apiErr
		}
	}

	compResource := NewNullAttribute(ResourceName)
	err = c.DBtx.QueryRow(`select name from compute_resources
						   where unitid = $1 and type = 'Interactive';`,
		unitid).Scan(&compResource)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !compResource.Valid {
		apiErr = append(apiErr, APIError{errors.New("interactive compute resource not found"), ErrorAPIRequirement})
		return nil, apiErr
	}

	compGroup := NewNullAttribute(GroupName)
	err = c.DBtx.QueryRow(`select name from affiliation_unit_group
						 left join groups as g using(groupid)
						 where is_primary and unitid = $1;`,
		unitid).Scan(&compGroup)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !compGroup.Valid {
		apiErr = append(apiErr, APIError{errors.New("primary group not found for this affiliation unit"), ErrorAPIRequirement})
		return nil, apiErr
	}

	primary := NewNullAttribute(Primary)
	primary.Scan(true)

	input := Input{
		UserName:     i[UserName],
		GroupName:    compGroup,
		ResourceName: compResource,
		Primary:      primary,
		Shell:        NewNullAttribute(Shell),
		HomeDir:      NewNullAttribute(HomeDir),
	}

	_, apiErr = setUserAccessToComputeResource(c, input)
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	// Add user to wilson_cluster and wilson group
	input = Input{
		UserName:     i[UserName],
		GroupName:    NewNullAttribute(GroupName).Default("wilson"),
		ResourceName: NewNullAttribute(ResourceName).Default("wilson_cluster"),
		Primary:      NewNullAttribute(Primary).Default(true),
		Shell:        NewNullAttribute(Shell),
		HomeDir:      NewNullAttribute(HomeDir),
	}

	_, apiErr = setUserAccessToComputeResource(c, input)
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	// Add new experimenter to all required groups with compute resource
	rows, err := c.DBtx.Query(`select name from affiliation_unit_group
							   left join groups as g using(groupid)
						 	   where is_required and unitid = $1;`, unitid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	var names []string
	for rows.Next() {
		var name string
		rows.Scan(&name)
		names = append(names, name)
	}
	rows.Close()
	for _, name := range names {
		groupName := NewNullAttribute(GroupName).Default(name)
		auxInput := Input{
			UserName:     i[UserName],
			GroupName:    groupName,
			ResourceName: compResource,
			Primary:      NewNullAttribute(Primary).Default(false),
			Shell:        NewNullAttribute(Shell),
			HomeDir:      NewNullAttribute(HomeDir),
		}
		_, apiErr = setUserAccessToComputeResource(c, auxInput)
		if len(apiErr) > 0 {
			return nil, apiErr
		}
	}

	if i[UnitName].Data.(string) == "cms" {
		rows, err := c.DBtx.Query(`select name, default_path, default_quota, default_unit from storage_resources`)
		if err != nil {
			log.WithFields(QueryFields(c)).Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
			return nil, apiErr
		}

		var inputs []Input
		for rows.Next() {
			storageInfo := NewMapNullAttribute(ResourceName, Path, Quota, QuotaUnit)
			err = rows.Scan(storageInfo[ResourceName], storageInfo[Path],
				storageInfo[Quota], storageInfo[QuotaUnit])

			*storageInfo[QuotaUnit] = storageInfo[QuotaUnit].Default("B")
			fullPath := storageInfo[Path].Data.(string) + "/" + i[UserName].Data.(string)

			input := make(Input)

			input.Add(i[UserName])
			input.Add(i[UnitName])
			input.Add(*storageInfo[ResourceName])
			input.Add(*storageInfo[Quota])
			input.Add(*storageInfo[QuotaUnit])
			input.AddValue(Path, fullPath)
			input.AddValue(GroupAccount, false)
			input.Add(NewNullAttribute(GroupName))
			input.Add(NewNullAttribute(ExpirationDate))

			inputs = append(inputs, input)
		}
		rows.Close()

		for _, input := range inputs {
			_, apiErr = setStorageQuota(c, input)
			if len(apiErr) > 0 {
				return nil, apiErr
			}
		}
	}

	return nil, nil
}

func setLPCStorageAccess(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	unitName := NewNullAttribute(UnitName).Default("cms")
	storageName := NewNullAttribute(ResourceName).Default("EOS")
	groupName := NewNullAttribute(GroupName).Default("us_cms")

	input := Input{
		UserName: i[UserName],
		UnitName: unitName,
		DN:       i[DN],
	}

	_, apiErr = addCertificateDNToUser(c, input)
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	input = Input{
		UserName:      i[UserName],
		UserAttribute: NewNullAttribute(UserAttribute).Default("cern_username"),
		Value:         i[ExternalUsername],
	}

	_, apiErr = setUserExternalAffiliationAttribute(c, input)
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	var nQuotas sql.NullInt64
	err := c.DBtx.QueryRow(`select count(*) from storage_quota
							join users using(uid)
							join storage_resources using(storageid)
							where uname = $1 and name = $2;`,
		i[UserName], storageName).Scan(&nQuotas)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}
	if nQuotas.Int64 == 0 {
		defaultPath := NewNullAttribute(Path)
		defaultQuota := NewNullAttribute(Quota)
		defaultUnit := NewNullAttribute(QuotaUnit)
		err = c.DBtx.QueryRow("select default_path, default_quota, default_unit from storage_resources where name = $1",
			storageName).Scan(&defaultPath, &defaultQuota, &defaultUnit)
		if err != nil {
			log.WithFields(QueryFields(c)).Error(err)
			apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
			return nil, apiErr
		}

		defaultPath.Scan(fmt.Sprintf("%s/%s", defaultPath.Data.(string), i[UserName].Data.(string)))

		input = Input{
			UserName:       i[UserName],
			GroupName:      groupName,
			UnitName:       unitName,
			ResourceName:   storageName,
			Quota:          defaultQuota,
			QuotaUnit:      defaultUnit,
			Path:           defaultPath,
			GroupAccount:   NewNullAttribute(GroupAccount),
			ExpirationDate: NewNullAttribute(ExpirationDate),
		}

		_, apiErr = setStorageQuota(c, input)
		if len(apiErr) > 0 {
			return nil, apiErr
		}
	}

	return nil, nil
}

func createExperiment(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	homeDir := i[HomeDir].Default("/nashome")
	userName := i[UserName].Default(i[UnitName].Data.(string) + "pro")
	groupName := i[GroupName].Default(i[UnitName].Data.(string))
	groupType := NewNullAttribute(GroupType).Default("UnixGroup")
	resourceName := NewNullAttribute(ResourceName).Default("fermigrid")

	vomsURL := NewNullAttribute(VOMSURL).Default("https://voms.fnal.gov:8443/voms/fermilab/" + i[UnitName].Data.(string))
	if i[Standalone].Valid {
		vomsURL = i[VOMSURL].Default("https://voms.fnal.gov:8443/voms/" + i[UnitName].Data.(string))
	}

	input := Input{
		UnitName:        i[UnitName],
		VOMSURL:         vomsURL,
		AlternativeName: NewNullAttribute(AlternativeName),
		UnitType:        NewNullAttribute(UnitType),
	}

	_, apiErr = createAffiliationUnit(c, input)
	if len(apiErr) > 0 && apiErr[0].Type != ErrorDuplicateData {
		return nil, apiErr
	}

	input = Input{
		ResourceName: i[UnitName],
		ResourceType: NewNullAttribute(ResourceType).Default("Interactive"),
		HomeDir:      homeDir,
		Shell:        NewNullAttribute(Shell).Default("/bin/bash"),
		UnitName:     i[UnitName],
	}

	_, apiErr = createComputeResource(c, input)
	if len(apiErr) > 0 && apiErr[0].Type != ErrorDuplicateData {
		return nil, apiErr
	}

	input = Input{
		GroupName: groupName,
		GroupType: groupType,
		UnitName:  i[UnitName],
		Primary:   NewNullAttribute(Primary).Default(true),
	}

	_, apiErr = addGroupToUnit(c, input)
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	for _, role := range []string{"Analysis", "NULL", "Production"} {
		fqanString := "/Role=" + role + "/Capability=NULL"
		if i[Standalone].Valid {
			fqanString = "/" + i[UnitName].Data.(string) + fqanString
		} else {
			fqanString = "/fermilab/" + i[UnitName].Data.(string) + fqanString
		}
		fqan := NewNullAttribute(FQAN).Default(fqanString)

		input = Input{
			FQAN:      fqan,
			GroupName: groupName,
			UserName:  NewNullAttribute(UserName),
			UnitName:  i[UnitName],
		}

		if role == "Production" {
			var userInGroup bool
			err := c.DBtx.QueryRow(`select ($1, $2) in (select uname, name from user_group join groups using (groupid) join users using(uid))`,
				userName, groupName).Scan(&userInGroup)
			if err != nil {
				log.WithFields(QueryFields(c)).Error(err)
				apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
				return nil, apiErr
			}

			if !userInGroup {
				auxInput := Input{
					UserName:  userName,
					GroupName: groupName,
					GroupType: groupType,
					Leader:    NewNullAttribute(Leader).Default(false),
				}
				_, apiErr = addUserToGroup(c, auxInput)
				if len(apiErr) > 0 {
					return nil, apiErr
				}
			}

			input[UserName] = userName
		}

		_, apiErr = createFQAN(c, input)
		if len(apiErr) > 0 && apiErr[0].Type != ErrorDuplicateData {
			return nil, apiErr
		}
	}

	var quotaExists bool
	err := c.DBtx.QueryRow(`select ($1, $2) in (select b.name, c.name from compute_batch b join compute_resources c using(compid))`,
		i[UnitName], resourceName).Scan(&quotaExists)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !quotaExists {
		input = Input{
			CondorGroup:    i[UnitName],
			ResourceName:   NewNullAttribute(ResourceName).Default("fermigrid"),
			Quota:          NewNullAttribute(Quota).Default("1"),
			ExpirationDate: NewNullAttribute(ExpirationDate),
			Surplus:        NewNullAttribute(Surplus),
		}

		_, apiErr = setCondorQuota(c, input)
		if len(apiErr) > 0 {
			return nil, apiErr
		}
	}

	return nil, nil
}

func addLPCConvener(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	if i[GroupName].Data.(string)[0:3] != "lpc" {
		apiErr = append(apiErr, APIError{errors.New("groupname must begin with lpc"), ErrorAPIRequirement})
		return nil, apiErr
	}

	input := Input{
		UserName:  i[UserName],
		GroupName: i[GroupName],
		GroupType: NewNullAttribute(GroupType).Default("UnixGroup"),
	}

	_, apiErr = setGroupLeader(c, input)
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	input = Input{
		UserName:     i[UserName],
		GroupName:    i[GroupName],
		ResourceName: NewNullAttribute(ResourceName).Default("lpcinteractive"),
		Shell:        NewNullAttribute(Shell),
		HomeDir:      NewNullAttribute(HomeDir),
		Primary:      NewNullAttribute(Primary),
	}

	_, apiErr = setUserAccessToComputeResource(c, input)
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	return nil, nil
}

func removeLPCConvener(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	if i[GroupName].Data.(string)[0:3] != "lpc" {
		apiErr = append(apiErr, APIError{errors.New("groupname must begin with lpc"), ErrorAPIRequirement})
		return nil, apiErr
	}

	input := Input{
		UserName:  i[UserName],
		GroupName: i[GroupName],
		GroupType: NewNullAttribute(GroupType).Default("UnixGroup"),
	}

	_, apiErr = removeGroupLeader(c, input)
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	if i[RemoveGroup].Valid {
		input = Input{
			UserName:     i[UserName],
			GroupName:    i[GroupName],
			ResourceName: NewNullAttribute(ResourceName).Default("lpcinteractive"),
		}

		_, apiErr = removeUserAccessFromResource(c, input)
		if len(apiErr) > 0 {
			return nil, apiErr
		}
	}

	return nil, nil
}

func addLPCCollaborationGroup(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	uid := NewNullAttribute(UID)
	groupid := NewNullAttribute(GroupID)
	unitid := NewNullAttribute(UnitID)
	resourceid := NewNullAttribute(ResourceID)

	username := NewNullAttribute(UserName).Default(i[GroupName].Data)
	unitname := NewNullAttribute(UnitName).Default("cms")
	computeres := NewNullAttribute(ResourceName).Default("lpcinteractive")
	storageres := NewNullAttribute(ResourceName).Default("EOS")
	grouptype := NewNullAttribute(GroupType).Default("UnixGroup")
	primaryUnit := NewNullAttribute(Primary).Default(false)
	primaryComp := NewNullAttribute(Primary).Default(true)
	quotaunit := i[QuotaUnit].Default("B")

	if i[GroupName].Data.(string)[0:3] != "lpc" {
		apiErr = append(apiErr, APIError{errors.New("groupname must begin with lpc"), ErrorAPIRequirement})
		return nil, apiErr
	}

	err := c.DBtx.QueryRow(`select (select uid from users where uname = $1),
								   (select groupid from groups where name = $1 and type = $2),
								   (select unitid from affiliation_units where name = $3),
								   (select compid from compute_resources where name = $4);`,
		i[GroupName], grouptype, unitname, computeres).Scan(&uid, &groupid, &unitid, &resourceid)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !uid.Valid {
		apiErr = append(apiErr, APIError{errors.New("LPC groups require a user with the same name"), ErrorAPIRequirement})
	}
	if !groupid.Valid {
		apiErr = append(apiErr, DefaultAPIError(ErrorDataNotFound, GroupName))
	}
	if !unitid.Valid {
		apiErr = append(apiErr, APIError{fmt.Errorf("LPC groups require the affiliation unit '%s'", unitname.Data.(string)), ErrorAPIRequirement})
	}
	if !resourceid.Valid {
		apiErr = append(apiErr, APIError{fmt.Errorf("LPC groups require the compute resource '%s'", computeres.Data.(string)), ErrorAPIRequirement})
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	input := make(Input)

	input.Add(i[GroupName])
	input.Add(grouptype)
	input.Add(unitname)
	input.Add(primaryUnit)

	_, apiErr = addGroupToUnit(c, input)
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	shell := NewNullAttribute(Shell)
	homedir := NewNullAttribute(HomeDir)
	err = c.DBtx.QueryRow(`select (select default_shell from compute_resources where name = $1),
								  (select default_home_dir from compute_resources where name = $1)`,
		computeres).Scan(&shell, &homedir)
	if err != nil {
		log.WithFields(QueryFields(c)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !shell.Valid {
		apiErr = append(apiErr, APIError{fmt.Errorf("default shell for '%s' not found", computeres.Data.(string)), ErrorAPIRequirement})
	}
	if !homedir.Valid {
		apiErr = append(apiErr, APIError{fmt.Errorf("default home directory for '%s' not found", computeres.Data.(string)), ErrorAPIRequirement})
	}
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	input = make(Input)

	input.Add(username)
	input.Add(i[GroupName])
	input.Add(computeres)
	input.Add(shell)
	input.Add(primaryComp)
	input.Add(homedir)

	_, apiErr = setUserAccessToComputeResource(c, input)
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	input = make(Input)

	input.Add(username)
	input.Add(i[GroupName])
	input.Add(unitname)
	input.Add(storageres)
	input.Add(i[Quota])
	input.Add(quotaunit)
	input.Add(NewNullAttribute(Path))
	input.AddValue(GroupAccount, true)
	input.Add(NewNullAttribute(ExpirationDate))

	_, apiErr = setStorageQuota(c, input)
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	return nil, nil
}
