package main

import (
	"strings"
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"fmt"
 	_ "github.com/lib/pq"
	"net/http"
	"time"
	"strconv"
)

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

func addUsertoExperiment(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	var duplicateCount int

	var compResource string
	var compGroup string

	type jsonerror struct {
		Error string `json:"ferry_error"`
	}
	var inputErr []jsonerror

	unit := strings.TrimSpace(q.Get("unitname"))
	if unit == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No unitname specified in http query.")
		inputErr = append(inputErr, jsonerror{"No unitname specified."})
	}

	if len(inputErr) > 0 {
		jsonout, err := json.Marshal(inputErr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error(err)
		}
		fmt.Fprintf(w, string(jsonout))
		return
	}
	
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
		inputErr = append(inputErr, jsonerror{"Error starting database transaction." })
		jsonout, err := json.Marshal(inputErr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error(err)
		}
		fmt.Fprintf(w, string(jsonout))
		return
	}
	defer DBtx.Rollback(key)

	uName := strings.TrimSpace(q.Get("username"))
	dnTemplate := "/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=%s/CN=UID:%s"
	var fullName string
	var valid bool

	rows, err := DBtx.Query("select full_name, status from users where uname = $1", uName)
	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Error(err)
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
		return
	}

	if !rows.Next() {
		log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"User does not exist.\" }")
		return
	}
	rows.Scan(&fullName, &valid)
	rows.Close()

	if !valid {
		log.WithFields(QueryFields(r, startTime)).Error("User status is not valid.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"User status is not valid.\" }")
		return
	}

	q.Set("dn", fmt.Sprintf(dnTemplate, fullName, uName))
	R.URL.RawQuery = q.Encode()

	DBtx.Savepoint("addCertificateDNToUser")
	addCertificateDNToUser(w, R)
	if !DBtx.Complete() {
		if !strings.Contains(DBtx.Error().Error(), "duplicate key value violates unique constraint") {
			log.WithFields(QueryFields(r, startTime)).Error("addCertificateDNToUser failed.")
			return
		}
		DBtx.RollbackToSavepoint("addCertificateDNToUser")
		duplicateCount ++
	}

	for _, fqan := range []string{"Analysis", "None"} {
		rows, err := DBtx.Query(`select fqan from grid_fqan
								 where fqan like $1 and lower(fqan) like lower($2);`, "%" + fqan + "%", "%" + unit + "%")
		if err != nil {
			defer log.WithFields(QueryFields(r, startTime)).Error(err)
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
			return
		}
		if !rows.Next() {
			log.WithFields(QueryFields(r, startTime)).Error("FQAN not found.")
			fmt.Fprintf(w, "{ \"ferry_error\": \"FQAN not found.\" }")
			return
		}

		var fullFqan string
		rows.Scan(&fullFqan)
		rows.Close()

		q.Set("fqan", fullFqan)
		R.URL.RawQuery = q.Encode()

		DBtx.Continue()
		DBtx.Savepoint("setUserExperimentFQAN_" + fqan)
		setUserExperimentFQAN(w, R)
		if !DBtx.Complete() {
			if !strings.Contains(DBtx.Error().Error(), "duplicate key value violates unique constraint") {
				log.WithFields(QueryFields(r, startTime)).Error("Failed to set FQAN to user.")
				return	
			}
			DBtx.RollbackToSavepoint("setUserExperimentFQAN_" + fqan)
			duplicateCount ++
		}
	}

	rows, err = DBtx.Query(`select cr.name from compute_resources as cr
							 left join affiliation_units au on cr.unitid = au.unitid
							 where au.name = $1 and cr.type = 'Interactive';`, unit)
	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Error(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
		return
	}

	if !rows.Next() {
		log.WithFields(QueryFields(r, startTime)).Error("Compute resource not found.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"Compute resource not found.\" }")
		return
	}
	rows.Scan(&compResource)
	rows.Close()

	rows, err = DBtx.Query(`select gp.name from affiliation_unit_group as ag
							left join affiliation_units as au on ag.unitid = au.unitid
							left join groups as gp on ag.groupid = gp.groupid
							where ag.is_primary and au.name = $1;`, unit)
	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Error(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
		return
	}

	if !rows.Next() {
		log.WithFields(QueryFields(r, startTime)).Error("Primary group not found.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"Primary group not found.\" }")
		return
	}
	rows.Scan(&compGroup)
	rows.Close()

	q.Set("resourcename", compResource)
	q.Set("groupname", compGroup)
	q.Set("is_primary", "true")
	R.URL.RawQuery = q.Encode()

	DBtx.Continue()
	DBtx.Savepoint("setUserAccessToComputeResource_" + compResource)
	setUserAccessToComputeResource(w, R)
	if !DBtx.Complete() {
		if !strings.Contains(DBtx.Error().Error(), "The request already exists in the database.") {
			log.WithFields(QueryFields(r, startTime)).Error("addUserToGroup failed")
			return
		}
		DBtx.RollbackToSavepoint("setUserAccessToComputeResource_" + compResource)
		duplicateCount ++
	}
	
	// now we need to do the storage resources. Comment out for now 20180813, until we figure out how to do it.
	
	
//	rows, err = DBtx.Query(`select sr.name from storage_resources as cr
//							 left join affiliation_units au on sr.unitid = au.unitid
//							 where au.name = $1;`, unit)
//	if err != nil {
//		defer log.WithFields(QueryFields(r, startTime)).Error(err)
//		w.WriteHeader(http.StatusNotFound)
//		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
//		return
//	}
//	
//	if !rows.Next() {
//		log.WithFields(QueryFields(r, startTime)).Error("Compute resource not found.")
//		fmt.Fprintf(w, "{ \"ferry_error\": \"Compute resource not found.\" }")
//		return
//	}
//	rows.Scan(&compResource)
//	rows.Close()
//	


	if duplicateCount == 4 {
		fmt.Fprintf(w, "{ \"ferry_error\": \"User already belongs to the experiment.\" }")
	} else {
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		fmt.Fprintf(w, "{ \"ferry_status\": \"success\" }")

		DBtx.Commit(key)
	}
}

func setLPCStorageAccess(w http.ResponseWriter, r *http.Request) {
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

	q.Set("unitname", "cms")
	R.URL.RawQuery = q.Encode()

	DBtx.Savepoint("addCertificateDNToUser")
	DBtx.Continue()
	addCertificateDNToUser(w, R)
	if !DBtx.Complete() {
		if !strings.Contains(DBtx.Error().Error(), `pk_affiliation_unit_user_certificate`) {
			log.WithFields(QueryFields(r, startTime)).Error("addCertificateDNToUser failed.")
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
		setUserExternalAffiliationAttribute(w, R)
		if !DBtx.Complete() {
			log.WithFields(QueryFields(r, startTime)).Error("setUserExternalAffiliationAttribute failed.")
			return
		}
	}

	uname := q.Get("username")

	q.Set("resourcename", "EOS")
	q.Set("groupname", "us_cms")
	q.Set("unitname", "cms")
	q.Set("quota", "100")
	q.Set("unit", "B")
	q.Set("path", fmt.Sprintf("/eos/uscms/store/user/%s", uname))
	R.URL.RawQuery = q.Encode()

	DBtx.Continue()
	setUserStorageQuota(w, R)
	if !DBtx.Complete() {
		log.WithFields(QueryFields(r, startTime)).Error("setUserStorageQuota failed.")
		return
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
	authorized,authout := authorize(r)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w,"{ \"ferry_error\": \"" + authout + "not authorized.\" }")
		return
	}

	duplicateCount := 0
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
			q.Set("voms_url","https://voms.fnal.gov:8443/vomses/" + unitName)
		}
		
	} else {
		q.Set("voms_url","https://voms.fnal.gov:8443/vomses/fermilab/" + unitName)	
	}

	R.URL.RawQuery = q.Encode()	

	DBtx.Savepoint("createAffiliationUnit")
//	DBtx.Continue()
	createAffiliationUnit(w,R)
	if ! DBtx.Complete() {
		// ERROR HANDLING AND ROLLBACK		
		if !strings.Contains(DBtx.Error().Error(), "duplicate key value violates unique constraint") {
			log.WithFields(QueryFields(r, startTime)).Error("Unit already exists.")
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
	q.Set("default_shell", "/bin/bash")
	q.Set("defaulthomedir", homedir)
	
	R.URL.RawQuery = q.Encode()
	DBtx.Savepoint("createComputeResource")
//	DBtx.Continue()
	createComputeResource(w,R)
	if !DBtx.Complete() {
		if !strings.Contains(DBtx.Error().Error(), "duplicate key value violates unique constraint") {
			log.WithFields(QueryFields(r, startTime)).Error("createComputeResource failed.")
			return
		} else {
			DBtx.RollbackToSavepoint("createComputeResource")
			duplicateCount++
		}
	}
	
// now we need to add the default group (which we assume is the same name as the unit) to affiliation_unit_group
// Set that group to be the primary group

	q.Set("is_primary", "true")
	q.Set("grouptype","UnixGroup")
	q.Set("groupname",unitName)
	R.URL.RawQuery = q.Encode()
	DBtx.Savepoint("addGroupToUnit")
//	DBtx.Continue()
	addGroupToUnit(w,R)
	if !DBtx.Complete() {
		if !strings.Contains(DBtx.Error().Error(), "duplicate key value violates unique constraint") && !strings.Contains(DBtx.Error().Error(), "Group and unit combination already in DB") {
			log.WithFields(QueryFields(r, startTime)).Error("addGroupToUnit failed.")
			log.WithFields(QueryFields(r, startTime)).Error("actual error: " + DBtx.Error().Error() )
			return
		} else {
			log.WithFields(QueryFields(r, startTime)).Error("actual error: " + DBtx.Error().Error() )
			DBtx.RollbackToSavepoint("addGroupToUnit")
			duplicateCount++
		}
	}

	for _, role := range []string{"Analysis", "None", "Production"} {
		//createFQAN
		// if standalone VO, change the string a bit
		fqan := "/Role=" + role
		if saVO {
			fqan = "/" + unitName + fqan
		} else {
			fqan = "/fermilab/" + unitName + fqan
		}
		q.Set("fqan",fqan)
		q.Set("mapped_group",unitName)
		if role == "Production" {
			q.Set("mapped_user", unitName + "pro")
		} else {
			q.Set("mapped_user","")
		}
		R.URL.RawQuery = q.Encode()
		DBtx.Continue()
		DBtx.Savepoint("createFQAN_" + role)
		createFQAN(w, R)
		if !DBtx.Complete() {
			// do some error handling and rollback 
			DBtx.RollbackToSavepoint("crateFQAN_"+role)
		}
	}
	
	
	// If everything worked
	DBtx.Commit(key)
}
