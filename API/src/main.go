package main

import (
	"net"
	"database/sql"
	"fmt"

	"crypto/tls"
	"net/http"
	"os"
	"time"

	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
	log "github.com/sirupsen/logrus"
	golog "log"
	"github.com/spf13/viper"
)

var DBptr *sql.DB
var DBtx Transaction
var AuthorizedDNs []string
var Mainsrv *http.Server

func handler(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	log.WithFields(QueryFields(r, startTime)).Debug(r.URL.Path)
	fmt.Fprintf(w, "This is a placeholder for paths like %s!", r.URL.Path[1:])
}

//QueryFields builds fields for a logger from an http request
func QueryFields(r *http.Request, t time.Time) log.Fields {
	fields := make(log.Fields)

	fields["client"] = r.RemoteAddr
	fields["action"] = r.URL.Path[1:]
	fields["query"] = r.URL
	fields["duration"] = time.Since(t).Nanoseconds() / 1E6
	if len(r.TLS.PeerCertificates) > 0 {
		fields["subject"] = ParseDN(r.TLS.PeerCertificates[0].Subject.Names, "/")
	}

	return fields
}

func gatekeeper(c net.Conn, s http.ConnState) {
	fields := make(log.Fields)

	fields["client"] = c.RemoteAddr()
	fields["state"] = s.String()

	if s.String() == "new" {
		log.WithFields(fields).Info("New connection started.")
	} else {
		log.WithFields(fields).Debug("Connection status changed.")
	}
}

func main() {

	//Setup configutation manager
	viper.SetConfigName("default")
	viper.AddConfigPath(".")
	cfgErr := viper.ReadInConfig()
	if cfgErr != nil {
		panic(fmt.Errorf("Fatal error config file: %s \n", cfgErr))
	}

	//Setup log file
	logConfig := viper.GetStringMapString("log")

	if len(logConfig) > 0 {
		if len(logConfig["file"]) > 0 {
			logFile, logErr := os.OpenFile(logConfig["file"], os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
			if logErr != nil {
				log.Errorf("Error log file: %s \n", logErr)
			}
			defer logFile.Close()
			log.SetOutput(logFile)
		}
		if len(logConfig["level"]) > 0 {
			level, logErr := log.ParseLevel(logConfig["level"])
			if logErr != nil {
				log.Error(logErr)
			} else {
				log.SetLevel(level)
			}
		}
	}

	log.Debug("Here we go...")

	//Make sure we are not running as root, and exit if we are.
	if os.Getuid() == 0 {
		log.Fatal("You are running as root (uid=0). Please run as a different user. Exiting.")
	}

	//NOTE: By default we have SSL mode set to "require" because the host cert on the DB machine is expired as of 10-25-2017.
	//		Once that is fixed we should set it to "verify-ca" or "verify-full" so that it actually checks that the cert that the DB machine presents is valid.
	//		If you set it to "require" it skips the verification step.
	dbConfig := viper.GetStringMapString("database")
	connString := fmt.Sprintf("user=%s password=%s host=%s dbname=%s connect_timeout=%s sslmode=%s sslrootcert=%s",
		dbConfig["user"], dbConfig["password"], dbConfig["host"], dbConfig["name"],
		dbConfig["timeout"], dbConfig["sslmode"], dbConfig["certificate"])
	Mydb, err := sql.Open("postgres", connString)
	if err != nil {
		log.Error("there is an issue here")
		log.Fatal(err)
	} else {
		DBptr = Mydb
		Mydb.SetMaxOpenConns(200)
		pingerr := Mydb.Ping()
		if pingerr != nil {
			log.Fatal(pingerr)
		}
	}

	grouter := mux.NewRouter()
	grouter.HandleFunc("/", handler)

	//user API calls
	grouter.HandleFunc("/getUserCertificateDNs", getUserCertificateDNs)
	grouter.HandleFunc("/getUserFQANs", getUserFQANs)
	grouter.HandleFunc("/getSuperUserList", getSuperUserList)
	grouter.HandleFunc("/getUserGroups", getUserGroups)
	grouter.HandleFunc("/getUserInfo", getUserInfo)
	grouter.HandleFunc("/addUserToGroup", addUserToGroup)
	grouter.HandleFunc("/addUsertoExperiment", setUserExperimentFQAN)
	grouter.HandleFunc("/setUserExperimentFQAN", setUserExperimentFQAN)
	grouter.HandleFunc("/setUserShellAndHomeDir", setUserShellAndHomeDir)
	grouter.HandleFunc("/getUserShellAndHomeDir", getUserShellAndHomeDir)
	grouter.HandleFunc("/setUserAccessToResource", setUserAccessToResource)
	grouter.HandleFunc("/removeUserAccessFromResource", removeUserAccessFromResource)
	grouter.HandleFunc("/getUserStorageQuota", 	    getUserStorageQuota)
	grouter.HandleFunc("/setUserStorageQuota", 	    setUserStorageQuota)
	grouter.HandleFunc("/getUserExternalAffiliationAttributes", 	    getUserExternalAffiliationAttributes)
	grouter.HandleFunc("/addCertDNtoUser"    ,    addCertDNtoUser)
	grouter.HandleFunc("/setSuperUser"    ,    setSuperUser)
	grouter.HandleFunc("/removeUserCertificateDN"    ,    removeUserCertificateDN)
	grouter.HandleFunc("/setUserInfo"    ,    setUserInfo)
	grouter.HandleFunc("/setUserExternalAffiliationAttribute",    setUserExternalAffiliationAttribute)
	grouter.HandleFunc("/removeUserExternalAffiliationAttribute",    removeUserExternalAffiliationAttribute)
	grouter.HandleFunc("/createUser"    ,    createUser)
	grouter.HandleFunc("/deleteUser"    ,    deleteUser)
	grouter.HandleFunc("/getUserUname"    ,    getUserUname)
	grouter.HandleFunc("/getMemberAffiliations",    getMemberAffiliations)
	grouter.HandleFunc("/getUserAccessToComputeResources",    getUserAccessToComputeResources)
	grouter.HandleFunc("/getUserAllStorageQuotas",    getUserAllStorageQuotas)

	//group API calls
	grouter.HandleFunc("/getgroupmembers", getGroupMembers)
	grouter.HandleFunc("/createGroup", createGroup)
	grouter.HandleFunc("/deleteGroupt", deleteGroupt)
	grouter.HandleFunc("/deleteGroup", deleteGroup)
	grouter.HandleFunc("/addGroupToUnit", addGroupToUnit)
	grouter.HandleFunc("/removeGroupFromUnit", removeGroupFromUnit)
	grouter.HandleFunc("/setPrimaryStatusGroup", setPrimaryStatusGroup)
	grouter.HandleFunc("/removePrimaryStatusfromGroup", removePrimaryStatusfromGroup)
	grouter.HandleFunc("/getGroupMembers", getGroupMembers)
	grouter.HandleFunc("/IsUserLeaderOf", IsUserLeaderOf)
	grouter.HandleFunc("/setGroupLeader", setGroupLeader)
	grouter.HandleFunc("/removeGroupLeader", removeGroupLeader)
	grouter.HandleFunc("/getGroupUnits", getGroupUnits)
	grouter.HandleFunc("/getGroupBatchPriorities", getGroupBatchPriorities)
	grouter.HandleFunc("/getGroupCondorQuotas", getGroupCondorQuotas)
	grouter.HandleFunc("/setGroupBatchPriority", setGroupBatchPriority)
	grouter.HandleFunc("/setGroupCondorQuota", setGroupCondorQuota)
	grouter.HandleFunc("/getGroupStorageQuotas", getGroupStorageQuotas)
	grouter.HandleFunc("/setGroupStorageQuota", setGroupStorageQuota)

	// misc API cals
	grouter.HandleFunc("/getPasswdFile", getPasswdFile)
	grouter.HandleFunc("/getGroupFile", getGroupFile)
	grouter.HandleFunc("/getGridMapFile", getGridMapFile)
	grouter.HandleFunc("/getVORoleMapFile", getVORoleMapFile)
	grouter.HandleFunc("/getUserUID", getUserUID)
	grouter.HandleFunc("/getUserUname", getUserUname)
	grouter.HandleFunc("/getGroupGID", getGroupGID)
	grouter.HandleFunc("/getGroupName", getGroupName)
	grouter.HandleFunc("/lookupCertificateDN", lookupCertificateDN)
	grouter.HandleFunc("/getMappedGidFile", getMappedGidFile)
	grouter.HandleFunc("/getStorageAuthzDBFile", getStorageAuthzDBFile)
	grouter.HandleFunc("/getAffiliationMembersRoles", getAffiliationMembersRoles)
	grouter.HandleFunc("/getStorageAccessLists", getStorageAccessLists)
	grouter.HandleFunc("/createComputeResource", createComputeResource)

	//affiliation unit API calls
	grouter.HandleFunc("/createAffiliationUnit",             createAffiliationUnit)           
	grouter.HandleFunc("/removeAffiliationUnit", 		   removeAffiliationUnit)           
	grouter.HandleFunc("/setAffiliationUnitInfo", 	   setAffiliationUnitInfo)          
	grouter.HandleFunc("/getAffiliationUnitMembers",	   getAffiliationUnitMembers)       
	grouter.HandleFunc("/getGroupsInAffiliationUnit", 	   getGroupsInAffiliationUnit)      
	grouter.HandleFunc("/getGroupLeadersinAffiliationUnit",  getGroupLeadersinAffiliationUnit)
	grouter.HandleFunc("/getAffiliationUnitStorageResources",getAffiliationUnitStorageResources)
	grouter.HandleFunc("/getAffiliationUnitComputeResources",getAffiliationUnitComputeResources)
	grouter.HandleFunc("/createFQAN",			   createFQAN)                        
	grouter.HandleFunc("/removeFQAN",			   removeFQAN)                        
	grouter.HandleFunc("/setFQANMappings",                     setFQANMappings)                    

	srvConfig := viper.GetStringMapString("server")
	Mainsrv = &http.Server{
		Addr:        fmt.Sprintf(":%s", srvConfig["port"]),
		ReadTimeout: 10 * time.Second,
		Handler:     grouter,
		ConnState:	 gatekeeper,
		ErrorLog:	 golog.New(log.StandardLogger().Writer(), "", 0),
	}

	certslice := viper.GetStringSlice("certificates")
	Certpool, err := loadCerts(certslice)
	if err != nil {
		log.Fatal(err)
	}
	Mainsrv.TLSConfig = &tls.Config{
		ClientAuth:         tls.RequireAndVerifyClientCert,
		ClientCAs:          Certpool,
		GetConfigForClient: checkClientIP,
		Certificates:       nil,
	}

	dnlist, listerror := createDNlist(srvConfig["dnlist"])
	if listerror != nil {
		log.Fatal(listerror)
	}
	AuthorizedDNs = make([]string, len(dnlist))
	copy(AuthorizedDNs, dnlist)
	log.Debugf("Authorized DN list created with %d entries.", len(AuthorizedDNs))
	if len(AuthorizedDNs) == 0 {
		log.Fatal("Authorized DN slice has zero elements.")
	}
	// We should probably make the cert and key paths variables in a config file at some point
	log.WithFields(log.Fields{"port": Mainsrv.Addr[1:]}).Infof("Starting FERRY API")
	serverror := Mainsrv.ListenAndServeTLS(srvConfig["cert"], srvConfig["key"])
	if serverror != nil {
		log.Fatal(serverror)
	}
	defer Mydb.Close()
}
