package main
import (
	"encoding/json"
	"strconv"
	log "github.com/sirupsen/logrus"
	"fmt"
 	_ "github.com/lib/pq"
	"net/http"
	"time"
)

func testWrapper(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

	var DBtx Transaction
	R := WithTransaction(r, &DBtx)
	
	key, err := DBtx.Start(DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error starting database transaction: " + err.Error())
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error starting database transaction.\" }")
		return
	}

	createUser(w, R)
	if !DBtx.Complete() {
		log.WithFields(QueryFields(r, startTime)).Error("createUser failed.")
		DBtx.Rollback()
		return
	}
	addUserToGroup(w, R)
	if !DBtx.Complete() {
		log.WithFields(QueryFields(r, startTime)).Error("addUserToGroup failed")
		DBtx.Rollback()
		return
	}

	log.WithFields(QueryFields(r, startTime)).Info("Success!")
	fmt.Fprintf(w, "{ \"ferry_status\": \"success\" }")

	DBtx.Commit(key)
}

func requestExperimentAccount(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	var compOnly bool

	type jsonerror struct {
		Error string `json:"ferry_error"`
	}
	var inputErr []jsonerror

	if q.Get("computingonly") == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No computingonly specified in http query.")
		inputErr = append(inputErr, jsonerror{"No computingonly specified."})
	} else {
		var err error
		if compOnly, err = strconv.ParseBool(q.Get("computingonly")) ; err != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Invalid value for computingonly.")
			inputErr = append(inputErr, jsonerror{"Invalid value for computingonly. Must be true or false."})
		}
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

	if !compOnly {
		setUserExperimentFQAN(w, R)
		if !DBtx.Complete() {
			log.WithFields(QueryFields(r, startTime)).Error("createUser failed.")
			DBtx.Rollback()
			return
		}
	}
	DBtx.Continue()
	setUserAccessToComputeResource(w, R)
	if !DBtx.Complete() {
		log.WithFields(QueryFields(r, startTime)).Error("addUserToGroup failed")
		DBtx.Rollback()
		return
	}

	log.WithFields(QueryFields(r, startTime)).Info("Success!")
	fmt.Fprintf(w, "{ \"ferry_status\": \"success\" }")

	DBtx.Commit(key)
}
