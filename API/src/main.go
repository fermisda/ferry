package main

import (
	"strconv"
	"database/sql"
	"flag"
	"fmt"
	"net"

	"github.com/fsnotify/fsnotify"

	"crypto/tls"
	"net/http"
	"os"
	"time"

	golog "log"

	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var DBptr *sql.DB
var DBtx Transaction
var Mainsrv *http.Server
var ValidCAs CAs

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
	//Read command-line arguments
	var configDir string
	flag.StringVar(&configDir, "c", ".", "Specify a configuration directory.")
	flag.Parse()

	//Setup configutation manager
	viper.SetEnvPrefix("ferry")
	viper.BindEnv("db_user")
	viper.BindEnv("db_pass")
	viper.BindEnv("db_host")
	viper.BindEnv("db_port")
	viper.BindEnv("db_name")
	viper.SetConfigName("default")
	viper.AddConfigPath(configDir)
	cfgErr := viper.ReadInConfig()
	if cfgErr != nil {
		panic(fmt.Errorf("Fatal error config file: %s \n", cfgErr))
	}
	viper.WatchConfig()
	viper.OnConfigChange(func(e fsnotify.Event) {
		log.WithFields(log.Fields{"file": e.Name}).Info("Config file changed.")
	})

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

	APIs := make(APICollection)
	IncludeUserAPIs(&APIs)
	IncludeGroupAPIs(&APIs)
	IncludeMiscAPIs(&APIs)
	IncludeWrapperAPIs(&APIs)

	log.Debug("Here we go...")

	//Make sure we are not running as root, and exit if we are.
	if os.Getuid() == 0 {
		log.Fatal("You are running as root (uid=0). Please run as a different user. Exiting.")
	}

	//NOTE: By default we have SSL mode set to "require" because the host cert on the DB machine is expired as of 10-25-2017.
	//		Once that is fixed we should set it to "verify-ca" or "verify-full" so that it actually checks that the cert that the DB machine presents is valid.
	//		If you set it to "require" it skips the verification step.
	dbConfig := viper.GetStringMapString("database")
	dbUser := viper.Get("db_user")
	if dbUser == nil {
		dbUser = dbConfig["user"]
	}
	dbPass := viper.Get("db_pass")
	if dbPass == nil {
		dbPass = dbConfig["password"]
	}
	dbHost := viper.Get("db_host")
	if dbHost == nil {
		dbHost = dbConfig["host"]
	}
	dbName := viper.Get("db_name")
	if dbName == nil {
		dbName = dbConfig["name"]
	}
	dbPort := viper.Get("db_port")
	if dbPort == nil {
		dbPort = dbConfig["port"]
	}
	connString := fmt.Sprintf("user=%s password=%s host=%s port=%s dbname=%s connect_timeout=%s sslmode=%s sslrootcert=%s",
		dbUser, dbPass, dbHost, dbPort, dbName,
		dbConfig["timeout"], dbConfig["sslmode"], dbConfig["certificate"])
	Mydb, err := sql.Open("postgres", connString)
	if err != nil {
		log.Error("there is an issue here")
		log.Fatal(err)
	} else {
		maxOpen, err := strconv.Atoi(dbConfig["max_open_conns"])
		if err != nil {
			log.Error("error converting max_open_conns")
			log.Fatal(err)
		}
		maxIdle, err := strconv.Atoi(dbConfig["max_idle_conns"])
		if err != nil {
			log.Error("error converting max_idel_conns")
			log.Fatal(err)
		}

		DBptr = Mydb
		Mydb.SetMaxOpenConns(maxOpen)
		Mydb.SetMaxIdleConns(maxIdle)
		pingerr := Mydb.Ping()
		if pingerr != nil {
			log.Fatal(pingerr)
		}
	}

	grouter := mux.NewRouter()
	grouter.HandleFunc("/", handler)

	//user API calls
	grouter.HandleFunc("/getUserCertificateDNs", APIs["getUserCertificateDNs"].Run)
	grouter.HandleFunc("/getUserFQANs", APIs["getUserFQANs"].Run)
	grouter.HandleFunc("/getSuperUserList", APIs["getSuperUserList"].Run)
	grouter.HandleFunc("/getUserGroups", APIs["getUserGroups"].Run)
	grouter.HandleFunc("/getUserInfo", APIs["getUserInfo"].Run)
	grouter.HandleFunc("/addUserToGroup", APIs["addUserToGroup"].Run)
	grouter.HandleFunc("/removeUserFromGroup", APIs["removeUserFromGroup"].Run)
	grouter.HandleFunc("/setUserExperimentFQAN", APIs["setUserExperimentFQAN"].Run)
	grouter.HandleFunc("/setUserShellAndHomeDir", APIs["setUserShellAndHomeDir"].Run)
	grouter.HandleFunc("/getUserShellAndHomeDir", APIs["getUserShellAndHomeDir"].Run)
	grouter.HandleFunc("/setUserShell", APIs["setUserShell"].Run)
	grouter.HandleFunc("/setUserAccessToComputeResource", APIs["setUserAccessToComputeResource"].Run)
	grouter.HandleFunc("/removeUserAccessFromResource", removeUserAccessFromResource)
	grouter.HandleFunc("/getUserStorageQuota", APIs["getUserStorageQuota"].Run)
	grouter.HandleFunc("/setUserStorageQuota", APIs["setUserStorageQuota"].Run)
	grouter.HandleFunc("/getUserExternalAffiliationAttributes", APIs["getUserExternalAffiliationAttributes"].Run)
	grouter.HandleFunc("/addCertificateDNToUser", APIs["addCertificateDNToUser"].Run)
	grouter.HandleFunc("/setSuperUser", setSuperUser)
	grouter.HandleFunc("/removeSuperUser", removeSuperUser)
	grouter.HandleFunc("/removeUserCertificateDN", removeUserCertificateDN)
	grouter.HandleFunc("/setUserInfo", APIs["setUserInfo"].Run)
	grouter.HandleFunc("/setUserExternalAffiliationAttribute", APIs["setUserExternalAffiliationAttribute"].Run)
	grouter.HandleFunc("/removeUserExternalAffiliationAttribute", removeUserExternalAffiliationAttribute)
	grouter.HandleFunc("/createUser", APIs["createUser"].Run)
	grouter.HandleFunc("/deleteUser", deleteUser)
	grouter.HandleFunc("/getUserUname", APIs["getUserUname"].Run)
	grouter.HandleFunc("/getUserUID", APIs["getUserUID"].Run)
	grouter.HandleFunc("/getMemberAffiliations", APIs["getMemberAffiliations"].Run)
	grouter.HandleFunc("/getUserAccessToComputeResources", APIs["getUserAccessToComputeResources"].Run)
	grouter.HandleFunc("/getStorageQuotas", APIs["getStorageQuotas"].Run)
	grouter.HandleFunc("/getAllUsers", APIs["getAllUsers"].Run)
	grouter.HandleFunc("/getAllUsersFQANs", getAllUsersFQANs)
	grouter.HandleFunc("/getAllUsersCertificateDNs", APIs["getAllUsersCertificateDNs"].Run)

	//group API calls
	grouter.HandleFunc("/getgroupmembers", APIs["getGroupMembers"].Run)
	grouter.HandleFunc("/getGroupMembers", APIs["getGroupMembers"].Run)
	grouter.HandleFunc("/createGroup", createGroup)
	grouter.HandleFunc("/deleteGroupt", deleteGroupt)
	grouter.HandleFunc("/deleteGroup", deleteGroup)
	grouter.HandleFunc("/addGroupToUnit", addGroupToUnit)
	grouter.HandleFunc("/removeGroupFromUnit", removeGroupFromUnit)
	grouter.HandleFunc("/setPrimaryStatusGroup", setPrimaryStatusGroup)
	grouter.HandleFunc("/removePrimaryStatusfromGroup", removePrimaryStatusfromGroup)
	grouter.HandleFunc("/IsUserLeaderOfGroup", APIs["isUserLeaderOfGroup"].Run)
	grouter.HandleFunc("/IsUserMemberOfGroup", APIs["isUserMemberOfGroup"].Run)
	grouter.HandleFunc("/setGroupLeader", APIs["setGroupLeader"].Run) //add user to group
	grouter.HandleFunc("/removeGroupLeader", APIs["removeGroupLeader"].Run)
	grouter.HandleFunc("/getGroupUnits", getGroupUnits) //don't remove the last leader
	grouter.HandleFunc("/getBatchPriorities", getBatchPriorities)
	grouter.HandleFunc("/getCondorQuotas", getCondorQuotas)
	grouter.HandleFunc("/setGroupBatchPriority", setGroupBatchPriority)
	grouter.HandleFunc("/setCondorQuota", setCondorQuota)
	grouter.HandleFunc("/removeCondorQuota", removeCondorQuota)
	grouter.HandleFunc("/getGroupStorageQuota", getGroupStorageQuota)
	grouter.HandleFunc("/setGroupStorageQuota", setGroupStorageQuota)
	grouter.HandleFunc("/getAllGroups", getAllGroups)
	grouter.HandleFunc("/getAllGroupsMembers", getAllGroupsMembers)
	grouter.HandleFunc("/addLPCCollaborationGroup", addLPCCollaborationGroup)
	grouter.HandleFunc("/getGroupAccessToResource", getGroupAccessToResource)

	// misc API cals
	grouter.HandleFunc("/getPasswdFile", getPasswdFile)
	grouter.HandleFunc("/getGroupFile", getGroupFile)
	grouter.HandleFunc("/getGridMapFile", getGridMapFile)
	grouter.HandleFunc("/getGridMapFileByVO", getGridMapFileByVO)
	grouter.HandleFunc("/getVORoleMapFile", getVORoleMapFile)
	grouter.HandleFunc("/getGroupGID", getGroupGID)
	grouter.HandleFunc("/getGroupName", getGroupName)
	grouter.HandleFunc("/lookupCertificateDN", lookupCertificateDN)
	grouter.HandleFunc("/getMappedGidFile", getMappedGidFile)
	grouter.HandleFunc("/getStorageAuthzDBFile", getStorageAuthzDBFile)
	grouter.HandleFunc("/getAffiliationMembersRoles", getAffiliationMembersRoles)
	grouter.HandleFunc("/getStorageAccessLists", getStorageAccessLists)
	grouter.HandleFunc("/createComputeResource", createComputeResource)
	grouter.HandleFunc("/setComputeResourceInfo", setComputeResourceInfo)
	grouter.HandleFunc("/getGroupUnits", getGroupUnits)
	grouter.HandleFunc("/createStorageResource", createStorageResource)
	grouter.HandleFunc("/setStorageResourceInfo", setStorageResourceInfo)
	grouter.HandleFunc("/getStorageResourceInfo", getStorageResourceInfo)
	grouter.HandleFunc("/getAllComputeResources", getAllComputeResources)
	grouter.HandleFunc("/getVOUserMap", getVOUserMap)
	grouter.HandleFunc("/cleanStorageQuotas", cleanStorageQuotas)
	grouter.HandleFunc("/cleanCondorQuotas", cleanCondorQuotas)
	grouter.HandleFunc("/ping", ping)

	grouter.HandleFunc("/testBaseAPI", APIs["testBaseAPI"].Run)

	//affiliation unit API calls
	grouter.HandleFunc("/createAffiliationUnit", createAffiliationUnit)
	grouter.HandleFunc("/removeAffiliationUnit", removeAffiliationUnit)
	grouter.HandleFunc("/setAffiliationUnitInfo", setAffiliationUnitInfo)
	grouter.HandleFunc("/getAffiliationUnitMembers", getAffiliationUnitMembers)
	grouter.HandleFunc("/getGroupsInAffiliationUnit", getGroupsInAffiliationUnit)
	grouter.HandleFunc("/getGroupLeadersinAffiliationUnit", getGroupLeadersinAffiliationUnit)
	grouter.HandleFunc("/getAffiliationUnitStorageResources", getAffiliationUnitStorageResources)
	grouter.HandleFunc("/getAffiliationUnitComputeResources", getAffiliationUnitComputeResources)
	grouter.HandleFunc("/createFQAN", createFQAN)
	grouter.HandleFunc("/removeFQAN", removeFQAN)
	grouter.HandleFunc("/setFQANMappings", setFQANMappings)
	grouter.HandleFunc("/getAllAffiliationUnits", getAllAffiliationUnits)

	//wrapper API calls
	grouter.HandleFunc("/testWrapper", testWrapper)
	grouter.HandleFunc("/addUsertoExperiment", APIs["addUserToExperiment"].Run)
	grouter.HandleFunc("/setLPCStorageAccess", setLPCStorageAccess)
	grouter.HandleFunc("/createExperiment", createExperiment)
	grouter.HandleFunc("/addLPCConvener", addLPCConvener)
	grouter.HandleFunc("/removeLPCConvener", removeLPCConvener)

	srvConfig := viper.GetStringMapString("server")
	Mainsrv = &http.Server{
		Addr:        fmt.Sprintf(":%s", srvConfig["port"]),
		ReadTimeout: 10 * time.Second,
		Handler:     grouter,
		ConnState:   gatekeeper,
		ErrorLog:    golog.New(log.StandardLogger().Writer(), "", 0),
	}

	certslice := viper.GetStringSlice("certificates")
	Certpool, err := loadCerts(certslice)
	if err != nil {
		log.Fatal(err)
	}

	ValidCAs, err = FetchCAs(srvConfig["cas"])
	if err != nil {
		log.Fatal(err)
	}

	// Support only a specific set of ciphers.
	// Use the constants defined in the tls package. Be careful here: THE ORDER MATTERS!
	// It seems that all http2-approved ciphers have to be first. For now just
	// list the ones we want to use in reverse order of their true hex value.
	// Question: should 128-bit ECDHE suite be allowed? at least one site says no. That would be tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256 and 	tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA. Then again another site says to prefer CGM suite over other regardless of size.
	Ciphers := []uint16{
		tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
		tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
		tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
		tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA}

	Mainsrv.TLSConfig = &tls.Config{
		ClientAuth:               tls.RequireAndVerifyClientCert,
		ClientCAs:                Certpool,
		GetConfigForClient:       checkClientIP,
		Certificates:             nil,
		MinVersion:               tls.VersionTLS12,
		InsecureSkipVerify:       false,
		CipherSuites:             Ciphers,
		PreferServerCipherSuites: true,
	}

	// We should probably make the cert and key paths variables in a config file at some point
	log.WithFields(log.Fields{"port": Mainsrv.Addr[1:]}).Infof("Starting FERRY API")
	serverror := Mainsrv.ListenAndServeTLS(srvConfig["cert"], srvConfig["key"])
	if serverror != nil {
		log.Fatal(serverror)
	}
	defer Mydb.Close()
}
