package main

import (
	"strings"
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"fmt"
 	_ "github.com/lib/pq"
	"net/http"
	"time"
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

	unit := q.Get("unitname")
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

	var DBtx Transaction
	R := WithTransaction(r, &DBtx)
	
	key, err := DBtx.Start(DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error starting database transaction: " + err.Error())
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error starting database transaction.\" }")
		return
	}

	uName := q.Get("username")
	dnTemplate := "/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=%s/CN=UID:%s"
	var fullName string
	var valid bool

	rows, err := DBtx.Query("select full_name, status from users where uname = $1;", uName)
	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Error(err)
		w.WriteHeader(http.StatusNotFound)
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
			DBtx.Rollback()
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
		DBtx.Savepoint("setUserExperimentFQAN")
		setUserExperimentFQAN(w, R)
		if !DBtx.Complete() {
			if !strings.Contains(DBtx.Error().Error(), "duplicate key value violates unique constraint") {
				log.WithFields(QueryFields(r, startTime)).Error("Failed to set FQAN to user.")
				DBtx.Rollback()
				return	
			}
			DBtx.RollbackToSavepoint("setUserExperimentFQAN")
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
	R.URL.RawQuery = q.Encode()

	DBtx.Continue()
	DBtx.Savepoint("setUserAccessToComputeResource")
	setUserAccessToComputeResource(w, R)
	if !DBtx.Complete() {
		if !strings.Contains(DBtx.Error().Error(), "The request already exists in the database.") {
			log.WithFields(QueryFields(r, startTime)).Error("addUserToGroup failed")
			DBtx.Rollback()
			return
		}
		DBtx.RollbackToSavepoint("setUserAccessToComputeResource")
		duplicateCount ++
	}

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

	var DBtx Transaction
	R := WithTransaction(r, &DBtx)

	key, err := DBtx.Start(DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error starting database transaction: " + err.Error())
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error starting database transaction.\" }")
		return
	}

	q.Set("unitname", "cms")
	R.URL.RawQuery = q.Encode()

	DBtx.Savepoint("addCertificateDNToUser")
	DBtx.Continue()
	addCertificateDNToUser(w, R)
	if !DBtx.Complete() {
		if !strings.Contains(DBtx.Error().Error(), `pk_affiliation_unit_user_certificate`) {
			log.WithFields(QueryFields(r, startTime)).Error("addCertificateDNToUser failed.")
			DBtx.Rollback()
			return
		}
		DBtx.RollbackToSavepoint("addCertificateDNToUser")
	}

	cernUname := q.Get("external_username")

	if cernUname != "" {
		q.Set("attribute", "cern_username")
		q.Set("value", cernUname)
		R.URL.RawQuery = q.Encode()

		DBtx.Continue()
		setUserExternalAffiliationAttribute(w, R)
		if !DBtx.Complete() {
			log.WithFields(QueryFields(r, startTime)).Error("setUserExternalAffiliationAttribute failed.")
			DBtx.Rollback()
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
		DBtx.Rollback()
		return
	}

	log.WithFields(QueryFields(r, startTime)).Info("Success!")
	fmt.Fprintf(w, "{ \"ferry_status\": \"success\" }")

	DBtx.Commit(key)
}
