package main

import (
	"errors"
	"strings"
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"fmt"
 	_ "github.com/lib/pq"
	"net/http"
	"time"
	"strconv"
	"database/sql"
)

// IncludeWrapperAPIs includes all APIs described in this file in an APICollection
func IncludeWrapperAPIs(c *APICollection) {
	addUserToExperiment := BaseAPI {
		InputModel {
			Parameter{UserName, true},
			Parameter{UnitName, true},
		},
		addUserToExperiment,
	}
	c.Add("addUserToExperiment", &addUserToExperiment)

	addLPCCollaborationGroup := BaseAPI {
		InputModel {
			Parameter{GroupName, true},
			Parameter{Quota, true},
			Parameter{QuotaUnit, false},
		},
		addLPCCollaborationGroup,
	}
	c.Add("addLPCCollaborationGroup", &addLPCCollaborationGroup)
}

func testWrapper(w http.ResponseWriter, r *http.Request) {
	cas, _ := FetchCAs(`C:\Users\coimb\Documents\Ferry\Certificates`)
	rows, _ := DBptr.Query("select dn, issuer_ca from user_certificates;")

	var dn, issuer string
	for rows.Next() {
		rows.Scan(&dn, &issuer)
		ca, err := cas.MatchCA(dn)
		if err != nil {
			print(err.Error())
		} else {
			if ca["subjectdn"] != issuer {
				print(ca["subjectdn"] + " != " + issuer)
			} else {
				print("Match!")
			}
		}
	}
}

func addUserToExperiment(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	dnTemplate	:= "/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=%s/CN=UID:%s"
	fullName 	:= NewNullAttribute(FullName)
	status		:= NewNullAttribute(Status)
	unitid		:= NewNullAttribute(UnitID)

	err := c.DBtx.QueryRow("select full_name, status from users where uname = $1", i[UserName]).Scan(&fullName, &status)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	err = c.DBtx.QueryRow("select unitid from affiliation_units where name = $1", i[UnitName]).Scan(&unitid)
	if err != nil && err != sql.ErrNoRows {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
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

	input := Input {
		UserName:	i[UserName],
		UnitName:	i[UnitName],
		DN:			dn,
	}
	_, apiErr = addCertificateDNToUser(c, input)
	if len(apiErr) > 0 {
		return nil, apiErr
	}

	for _, r := range []string{"Analysis", "NULL"} {
		role := NewNullAttribute(Role)
		role.Scan(r)

		input = Input {
			UserName:	i[UserName],
			UnitName:	i[UnitName],
			Role:		role,
			FQAN:		NewNullAttribute(FQAN),
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
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
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
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
		apiErr = append(apiErr, DefaultAPIError(ErrorDbQuery, nil))
		return nil, apiErr
	}

	if !compGroup.Valid {
		apiErr = append(apiErr, APIError{errors.New("primary group not found for this affiliation unit"), ErrorAPIRequirement})
		return nil, apiErr
	}

	primary := NewNullAttribute(Primary)
	primary.Scan(true)

	input = Input {
		UserName: i[UserName],
		GroupName: compGroup,
		ResourceName: compResource,
		Primary: primary,
		Shell: NewNullAttribute(Shell),
		HomeDir: NewNullAttribute(HomeDir),
	}

	_, apiErr = setUserAccessToComputeResource(c, input)
	if len(apiErr) > 0 {
		return nil, apiErr
	}
	
	if i[UnitName].Data.(string) == "cms" {
		rows, err := c.DBtx.Query(`select name, default_path, default_quota, default_unit from storage_resources`)
		if err != nil {
			log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
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

		for _, input := range(inputs) {
			_, apiErr = setStorageQuota(c, input)
			if len(apiErr) > 0 {
				return nil, apiErr
			}
		}
	}

	return nil, nil
}

func setLPCStorageAccess(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	const unitName = "cms"
	const storageName = "EOS"
	const groupName = "us_cms"
	
	authorized,authout := authorize(r)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w,"{ \"ferry_error\": \"" + authout + "not authorized.\" }")
		return
	}
	
	var DBtx Transaction
	R := WithTransaction(r, &DBtx)

	key, err := DBtx.Start(DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error starting database transaction: " + err.Error())
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error starting database transaction.\" }")
		return
	}
	defer DBtx.Rollback(key)

	q.Set("unitname", unitName)
	R.URL.RawQuery = q.Encode()

	DBtx.Savepoint("addCertificateDNToUser")
	DBtx.Continue()
	addCertificateDNToUserLegacy(w, R)
	if !DBtx.Complete() {
		if !strings.Contains(DBtx.Error().Error(), `pk_affiliation_unit_user_certificate`) {
			log.WithFields(QueryFields(r, startTime)).Error("addCertificateDNToUser failed.")
			if DBtx.Error().Error() == "User does not exist." {
				fmt.Fprintf(w, "{ \"ferry_error\": \"User does not exist.\" }")
			}
			return
		}
		DBtx.RollbackToSavepoint("addCertificateDNToUser")
	}

	cernUname := strings.TrimSpace(q.Get("external_username"))

	if cernUname != "" {
		q.Set("attribute", "cern_username")
		q.Set("value", cernUname)
		R.URL.RawQuery = q.Encode()

		DBtx.Continue()
		setUserExternalAffiliationAttributeLegacy(w, R)
		if !DBtx.Complete() {
			log.WithFields(QueryFields(r, startTime)).Error("setUserExternalAffiliationAttribute failed.")
			return
		}
	}

	uname := q.Get("username")

	var nQuotas sql.NullInt64
	err = DBtx.QueryRow(`select count(*) from storage_quota as sq
						 join users as u on sq.uid = u.uid
						 join storage_resources as sr on sq.storageid = sr.storageid
						 where uname = $1 and name = $2;`, uname, storageName).Scan(&nQuotas)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error querying user quotas: " + err.Error())
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error querying user quotas.\" }")
		return
	}
	if nQuotas.Int64 == 0 {
		var defaultPath, defaultQuota, defaultUnit sql.NullString
		err = DBtx.QueryRow("select default_path, default_quota, default_unit from storage_resources where name = $1",
		storageName).Scan(&defaultPath, &defaultQuota, &defaultUnit)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Error querying default storage values: " + err.Error())
			fmt.Fprintf(w,"{ \"ferry_error\": \"Error querying default storage values.\" }")
			return
		}

		q.Set("resourcename", storageName)
		q.Set("groupname", groupName)
		q.Set("unitname", unitName)
		q.Set("quota", defaultQuota.String)
		q.Set("quota_unit", defaultUnit.String)
		q.Set("path", fmt.Sprintf("%s/%s", defaultPath.String, uname))
		R.URL.RawQuery = q.Encode()

		DBtx.Continue()
		setUserStorageQuotaLegacy(w, R)
		if !DBtx.Complete() {
			log.WithFields(QueryFields(r, startTime)).Error("setUserStorageQuota failed.")
			return
		}
	}

	log.WithFields(QueryFields(r, startTime)).Info("Success!")
	fmt.Fprintf(w, "{ \"ferry_status\": \"success\" }")

	DBtx.Commit(key)
}

func createExperiment(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	
	unitName := strings.TrimSpace(q.Get("unitname"))
	voms_url := strings.TrimSpace(q.Get("voms_url"))
	homedir := strings.TrimSpace(q.Get("defaulthomedir"))
	userName := strings.TrimSpace(q.Get("username"))
	groupName := strings.TrimSpace(q.Get("groupname"))
	standalone := strings.TrimSpace(q.Get("standalone")) // it is a standalone VO, i.e. not a subgroup of the Fermilab VO.

	saVO, parserr := strconv.ParseBool(standalone)
	if standalone == "" {
		saVO = false
	}
	type jsonerror struct {
		Error string `json:"ferry_error"`
	}
	var inputErr []jsonerror
	
	if parserr != nil && standalone != "" {
		log.WithFields(QueryFields(r, startTime)).Error("Error parsing the standalone option.")
		inputErr = append(inputErr, jsonerror{"Error parsing the standalone option. If provided it should be true or false."})
	}
	if unitName == "" {
		
		log.WithFields(QueryFields(r, startTime)).Error("No unitname specified in http query.")
		inputErr = append(inputErr, jsonerror{"No unitname specified."})	
	}
	//Set the default home directory to /nashome if it was not provided.
	if homedir == "" {
		homedir = "/nashome"
	}
	//Use experimentpro if username was not provided.
	if userName == "" {
		userName = unitName + "pro"
	}
	//Use unitname as groupname if it was not provided.
	if groupName == "" {
		groupName = unitName
	}
	authorized,authout := authorize(r)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w,"{ \"ferry_error\": \"" + authout + "not authorized.\" }")
		return
	}

	duplicateCount := 0
	duplicateCountRef := 0
	var DBtx Transaction
	R := WithTransaction(r, &DBtx)
	key, err := DBtx.Start(DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error starting database transaction: " + err.Error())
		inputErr = append(inputErr, jsonerror{"Error starting database transaction."})
		return
	}
	defer DBtx.Rollback(key)
	if len(inputErr) > 0 {
		jsonout, err := json.Marshal(inputErr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error(err)
		}
		fmt.Fprintf(w, string(jsonout))
		return		
	}
	
// first create the affiliation unit
	if saVO {		
		if voms_url != "" {
			q.Set("voms_url",voms_url)
		} else {
			q.Set("voms_url","https://voms.fnal.gov:8443/voms/" + unitName)
		}
		
	} else {
		q.Set("voms_url","https://voms.fnal.gov:8443/voms/fermilab/" + unitName)	
	}

	R.URL.RawQuery = q.Encode()	

	DBtx.Savepoint("createAffiliationUnit")
//	DBtx.Continue()
	duplicateCountRef ++
	createAffiliationUnitLegacy(w,R)
	if ! DBtx.Complete() {
		// ERROR HANDLING AND ROLLBACK		
		if !strings.Contains(DBtx.Error().Error(), "duplicate key value violates unique constraint") &&
		   !strings.Contains(DBtx.Error().Error(), "already exists") {
			log.WithFields(QueryFields(r, startTime)).Error("Unit already exists.")
			fmt.Fprintf(w,"{ \"ferry_error\": \"Error in createAffiliationUnit: " + DBtx.Error().Error() + ". Rolling back transaction.\" }")
			return
		}
		DBtx.RollbackToSavepoint("createAffiliationUnit")
		duplicateCount ++	
	} else {
			log.WithFields(QueryFields(r, startTime)).Info("Successfully created affiliation_unit " + unitName + "." )
	}

	//OK, we made the unit. Now, create the compute resource. By default its name is the same as the unit name.
	q.Set("unitname", unitName)
	q.Set("resourcename", unitName)
	q.Set("type", "Interactive")
	q.Set("defaultshell", "/bin/bash")
	q.Set("defaulthomedir", homedir)
	
	R.URL.RawQuery = q.Encode()
	DBtx.Savepoint("createComputeResource")
//	DBtx.Continue()
	duplicateCountRef ++
	createComputeResource(w,R)
	if !DBtx.Complete() {
		if !strings.Contains(DBtx.Error().Error(), "duplicate key value violates unique constraint") &&
		   !strings.Contains(DBtx.Error().Error(), "already exists") {
			log.WithFields(QueryFields(r, startTime)).Error("createComputeResource failed.")
			fmt.Fprintf(w,"{ \"ferry_error\": \"Error in createComputeResource: " + DBtx.Error().Error() + ". Rolling back transaction.\" }")
			return
		} else {
			DBtx.RollbackToSavepoint("createComputeResource")
			duplicateCount ++
		}
	}
	
// now we need to add the default group (which we assume is the same name as the unit) to affiliation_unit_group
// Set that group to be the primary group

	q.Set("is_primary", "true")
	q.Set("grouptype", "UnixGroup")
	q.Set("groupname", groupName)
	R.URL.RawQuery = q.Encode()
	DBtx.Savepoint("addGroupToUnit")
//	DBtx.Continue()
	duplicateCountRef ++
	addGroupToUnitLegacy(w,R)
	if !DBtx.Complete() {
		if !strings.Contains(DBtx.Error().Error(), "duplicate key value violates unique constraint") &&
		   !strings.Contains(DBtx.Error().Error(), "Group and unit combination already in DB") {
			log.WithFields(QueryFields(r, startTime)).Error("addGroupToUnit failed.")
			log.WithFields(QueryFields(r, startTime)).Error("actual error: " + DBtx.Error().Error() )
			fmt.Fprintf(w,"{ \"ferry_error\": \"Error in addGroupToUnit: " + DBtx.Error().Error() + ". Rolling back transaction.\" }")
			return
		} else {
			log.WithFields(QueryFields(r, startTime)).Error("actual error: " + DBtx.Error().Error() )
			DBtx.RollbackToSavepoint("addGroupToUnit")
			duplicateCount ++
		}
	}

	for _, role := range []string{"Analysis", "NULL", "Production"} {
		//createFQAN
		// if standalone VO, change the string a bit
		fqan := "/Role=" + role  + "/Capability=NULL"
		if saVO {
			fqan = "/" + unitName + fqan
		} else {
			fqan = "/fermilab/" + unitName + fqan
		}
		q.Set("fqan",fqan)
		q.Set("mapped_group", groupName)
		if role == "Production" {
			q.Set("mapped_user", userName)
			q.Set("is_leader", "false")
			q.Set("username", userName)
		} else {
			q.Set("mapped_user","")
		}
		R.URL.RawQuery = q.Encode()

		//Production is a special case since we need a mapped user. We should check if experimentpro has been added to the relevant group already.
		//We also skip CMS since it is another special case.

		if role == "Production" && unitName != "cms" {
			var tmpuid,tmpgid int
			DBtx.Savepoint("QuerryRow")
			queryerr := DBtx.QueryRow(`select uid, groupid from user_group ug join groups g using (groupid) join users u using(uid) where u.uname=$1 and g.name=$2`,unitName + "pro", unitName).Scan(&tmpuid, &tmpgid)
			if queryerr == sql.ErrNoRows {
				DBtx.RollbackToSavepoint("QuerryRow")
				DBtx.Savepoint("addUserToGroup_" + role)
				addUserToGroupLegacy(w,R)
				if !DBtx.Complete() {
					log.WithFields(QueryFields(r, startTime)).Error("Error in addUserToGroup for " + unitName + "pro: " + DBtx.Error().Error())
					if strings.Contains(DBtx.Error().Error(), "null value in column \"uid\"") {
						fmt.Fprintf(w,"{ \"ferry_error\": \"User " + userName + " doesn't exist.\" }")
					} else {
						fmt.Fprintf(w,"{ \"ferry_error\": \"Error in addUserToGroup: " + strings.Replace(DBtx.Error().Error(), "\"", "'", -1) + ". Rolling back transaction.\" }")
					}
					return
				}
			}
		}
		//		DBtx.Continue()
		DBtx.Savepoint("createFQAN_" + role)
		duplicateCountRef ++
		createFQAN(w, R)
		if !DBtx.Complete() {
			// do some error handling and rollback 
			
			if !strings.Contains(DBtx.Error().Error(), "Specified FQAN already associated") {
				fmt.Fprintf(w,"{ \"ferry_error\": \"Error in createFQAN for " + role + ": " + DBtx.Error().Error() + ". Rolling back transaction.\" }")
				log.WithFields(QueryFields(r, startTime)).Error("Error in createFQAN for role " + role + ": " +  DBtx.Error().Error())
				return
			}
			DBtx.RollbackToSavepoint("createFQAN_" + role)
			duplicateCount ++
		}
	}
	
	// If everything worked
	if duplicateCount < duplicateCountRef {
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		fmt.Fprintf(w, "{ \"ferry_status\": \"success\" }")
	} else {
		log.WithFields(QueryFields(r, startTime)).Info("Experiment already exists.")
		fmt.Fprintf(w, "{ \"ferry_status\": \"Experiment already exists.\" }")
	}
	
	DBtx.Commit(key)
}

func addLPCConvener(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	
	authorized,authout := authorize(r)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w,"{ \"ferry_error\": \"" + authout + "not authorized.\" }")
		return
	}
	
	var DBtx Transaction
	R := WithTransaction(r, &DBtx)

	key, err := DBtx.Start(DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error starting database transaction: " + err.Error())
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error starting database transaction.\" }")
		return
	}
	defer DBtx.Rollback(key)

	if q.Get("groupname") != "" && q.Get("groupname")[0:3] != "lpc" {
		log.WithFields(QueryFields(r, startTime)).Error("LPC groupnames must begin with \"lpc\".")
		fmt.Fprintf(w,"{ \"ferry_error\": \"groupname must begin with lpc.\" }")
		return
	}

	q.Set("grouptype", "UnixGroup")
	R.URL.RawQuery = q.Encode()

	DBtx.Continue()
	setGroupLeaderLegacy(w, R)
	if !DBtx.Complete() {
		log.WithFields(QueryFields(r, startTime)).Error("setGroupLeader failed.")
		return
	}

	q.Set("resourcename", "lpcinteractive")
	R.URL.RawQuery = q.Encode()

	DBtx.Continue()
	setUserAccessToComputeResourceLegacy(w, R)
	if !DBtx.Complete() {
		if !strings.Contains(DBtx.Error().Error(), `The request already exists in the database`) {
			log.WithFields(QueryFields(r, startTime)).Error("setUserAccessToComputeResource failed.")
			return
		}
	}

	log.WithFields(QueryFields(r, startTime)).Info("Success!")
	fmt.Fprintf(w, "{ \"ferry_status\": \"success\" }")

	DBtx.Commit(key)
}

func removeLPCConvener(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	
	authorized,authout := authorize(r)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w,"{ \"ferry_error\": \"" + authout + "not authorized.\" }")
		return
	}
	
	var DBtx Transaction
	R := WithTransaction(r, &DBtx)

	key, err := DBtx.Start(DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error starting database transaction: " + err.Error())
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error starting database transaction.\" }")
		return
	}
	defer DBtx.Rollback(key)

	if q.Get("groupname") != "" && q.Get("groupname")[0:3] != "lpc" {
		log.WithFields(QueryFields(r, startTime)).Error("LPC groupnames must begin with \"lpc\".")
		fmt.Fprintf(w,"{ \"ferry_error\": \"groupname must begin with lpc.\" }")
		return
	}

	q.Set("grouptype", "UnixGroup")
	R.URL.RawQuery = q.Encode()

	DBtx.Savepoint("removeGroupLeader")
	DBtx.Continue()
	removeGroupLeaderLegacy(w, R)
	if !DBtx.Complete() {
		if !strings.Contains(DBtx.Error().Error(), `User is not a leader of this group.`) {
			log.WithFields(QueryFields(r, startTime)).Error("removeGroupLeader failed.")
			return
		}
		DBtx.RollbackToSavepoint("removeGroupLeader")
	}

	if strings.ToLower(q.Get("removegroup")) == "true" {
		q.Set("resourcename", "lpcinteractive")
		R.URL.RawQuery = q.Encode()

		DBtx.Continue()
		removeUserAccessFromResource(w, R)
		if !DBtx.Complete() {
			if !strings.Contains(DBtx.Error().Error(), `The request already exists in the database`) {
				log.WithFields(QueryFields(r, startTime)).Error("setUserAccessToComputeResource failed.")
				return
			}
		}
	}

	log.WithFields(QueryFields(r, startTime)).Info("Success!")
	fmt.Fprintf(w, "{ \"ferry_status\": \"success\" }")

	DBtx.Commit(key)
}

func addLPCCollaborationGroup(c APIContext, i Input) (interface{}, []APIError) {
	var apiErr []APIError

	uid			:= NewNullAttribute(UID)
	groupid		:= NewNullAttribute(GroupID)
	unitid		:= NewNullAttribute(UnitID)
	resourceid	:= NewNullAttribute(ResourceID)

	username	:= NewNullAttribute(UserName).Default(i[GroupName].Data)
	unitname	:= NewNullAttribute(UnitName).Default("cms")
	computeres	:= NewNullAttribute(ResourceName).Default("lpcinteractive")
	storageres  := NewNullAttribute(ResourceName).Default("EOS")
	grouptype	:= NewNullAttribute(GroupType).Default("UnixGroup")
	primaryUnit	:= NewNullAttribute(Primary).Default(false)
	primaryComp	:= NewNullAttribute(Primary).Default(true)
	quotaunit	:= i[QuotaUnit].Default("B")

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
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
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

	shell 	:= NewNullAttribute(Shell)
	homedir	:= NewNullAttribute(HomeDir)
	err = c.DBtx.QueryRow(`select (select default_shell from compute_resources where name = $1),
								  (select default_home_dir from compute_resources where name = $1)`,
								  computeres).Scan(&shell, &homedir)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err)
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