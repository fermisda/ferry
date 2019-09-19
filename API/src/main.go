package main

import (
	"strings"
	"regexp"
	"database/sql"
	"flag"
	"fmt"
	"net"
	"strconv"

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
	var c APIContext
	c.StartTime = time.Now()
	log.WithFields(QueryFields(c)).Debug(r.URL.Path)
	fmt.Fprintf(w, "This is a placeholder for paths like %s!", r.URL.Path[1:])
}

//QueryFields builds fields for a logger from an http request
func QueryFields(c APIContext) log.Fields {
	const Unknown = "unknown"
	fields := make(log.Fields)

	fields["action"] = c.R.URL.Path[1:]
	fields["query"] = c.R.URL
	fields["auth_level"] = c.AuthLevel.String()
	fields["duration"] = time.Since(c.StartTime).Nanoseconds() / 1E6

	clientIP := strings.Split(c.R.RemoteAddr, ":")[0]
	fields["client_ip"] = clientIP

	clientNames, err := net.LookupAddr(clientIP)
	if err == nil {
		fields["hostname"] = strings.Trim(clientNames[0], ".")
	} else {
		fields["hostname"] = Unknown
	}

	if len(c.R.TLS.PeerCertificates) > 0 {
		fields["subject"] = ParseDN(c.R.TLS.PeerCertificates[0].Subject.Names, "/")
	}

	if c.AuthRole != "" {
		fields["auth_role"] = c.AuthRole
	} else {
		fields["auth_role"] = Unknown
	}

	re := regexp.MustCompile(fmt.Sprintf(`(?:\&|\?)%s=([\w]+)`, string(UnitName)))
	unitname := re.FindStringSubmatch(c.R.URL.String())
	if len(unitname) > 1 {
		fields[string(UnitName)] = unitname[1]
	} else {
		fields[string(UnitName)] = Unknown
	}

	return fields
}

func gatekeeper(c net.Conn, s http.ConnState) {
	fields := make(log.Fields)

	fields["client"] = c.RemoteAddr()
	fields["state"] = s.String()

	if s.String() == "new" {
		log.WithFields(fields).Debug("New connection started.")
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

	log.SetFormatter(&log.TextFormatter{
		FullTimestamp: true,
		DisableColors: true,
	})

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
	IncludeUnitAPIs(&APIs)

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
	grouter.HandleFunc("/getUserGroups", APIs["getUserGroups"].Run)
	grouter.HandleFunc("/getUserInfo", APIs["getUserInfo"].Run)
	grouter.HandleFunc("/addUserToGroup", APIs["addUserToGroup"].Run)
	grouter.HandleFunc("/removeUserFromGroup", APIs["removeUserFromGroup"].Run)
	grouter.HandleFunc("/setUserExperimentFQAN", APIs["setUserExperimentFQAN"].Run)
	grouter.HandleFunc("/setUserShellAndHomeDir", APIs["setUserShellAndHomeDir"].Run)
	grouter.HandleFunc("/getUserShellAndHomeDir", APIs["getUserShellAndHomeDir"].Run)
	grouter.HandleFunc("/setUserShell", APIs["setUserShell"].Run)
	grouter.HandleFunc("/setUserAccessToComputeResource", APIs["setUserAccessToComputeResource"].Run)
	grouter.HandleFunc("/removeUserAccessFromResource", APIs["removeUserAccessFromResource"].Run)
	grouter.HandleFunc("/getUserStorageQuota", APIs["getUserStorageQuota"].Run)
	grouter.HandleFunc("/getUserExternalAffiliationAttributes", APIs["getUserExternalAffiliationAttributes"].Run)
	grouter.HandleFunc("/addCertificateDNToUser", APIs["addCertificateDNToUser"].Run)
	grouter.HandleFunc("/removeUserCertificateDN", APIs["removeUserCertificateDN"].Run)
	grouter.HandleFunc("/setUserInfo", APIs["setUserInfo"].Run)
	grouter.HandleFunc("/setUserExternalAffiliationAttribute", APIs["setUserExternalAffiliationAttribute"].Run)
	grouter.HandleFunc("/removeUserExternalAffiliationAttribute", APIs["removeUserExternalAffiliationAttribute"].Run)
	grouter.HandleFunc("/createUser", APIs["createUser"].Run)
	grouter.HandleFunc("/deleteUser", APIs["deleteUser"].Run)
	grouter.HandleFunc("/getUserUname", APIs["getUserUname"].Run)
	grouter.HandleFunc("/getUserUID", APIs["getUserUID"].Run)
	grouter.HandleFunc("/getMemberAffiliations", APIs["getMemberAffiliations"].Run)
	grouter.HandleFunc("/getUserAccessToComputeResources", APIs["getUserAccessToComputeResources"].Run)
	grouter.HandleFunc("/getStorageQuotas", APIs["getStorageQuotas"].Run)
	grouter.HandleFunc("/getAllUsers", APIs["getAllUsers"].Run)
	grouter.HandleFunc("/getAllUsersFQANs", APIs["getAllUsersFQANs"].Run)
	grouter.HandleFunc("/getAllUsersCertificateDNs", APIs["getAllUsersCertificateDNs"].Run)
	grouter.HandleFunc("/setUserGridAccess", APIs["setUserGridAccess"].Run)

	//group API calls
	grouter.HandleFunc("/getgroupmembers", APIs["getGroupMembers"].Run)
	grouter.HandleFunc("/getGroupMembers", APIs["getGroupMembers"].Run)
	grouter.HandleFunc("/createGroup", APIs["createGroup"].Run)
	grouter.HandleFunc("/addGroupToUnit", APIs["addGroupToUnit"].Run)
	grouter.HandleFunc("/removeGroupFromUnit", APIs["removeGroupFromUnit"].Run)
	grouter.HandleFunc("/setPrimaryStatusGroup", APIs["setPrimaryStatusGroup"].Run)
	grouter.HandleFunc("/IsUserLeaderOfGroup", APIs["isUserLeaderOfGroup"].Run)
	grouter.HandleFunc("/IsUserMemberOfGroup", APIs["isUserMemberOfGroup"].Run)
	grouter.HandleFunc("/setGroupLeader", APIs["setGroupLeader"].Run) //add user to group
	grouter.HandleFunc("/removeGroupLeader", APIs["removeGroupLeader"].Run)
	grouter.HandleFunc("/getGroupUnits", APIs["getGroupUnits"].Run) //don't remove the last leader
	grouter.HandleFunc("/getBatchPriorities", APIs["getBatchPriorities"].Run)
	grouter.HandleFunc("/getCondorQuotas", APIs["getCondorQuotas"].Run)
	grouter.HandleFunc("/setCondorQuota", APIs["setCondorQuota"].Run)
	grouter.HandleFunc("/removeCondorQuota", APIs["removeCondorQuota"].Run)
	grouter.HandleFunc("/getGroupStorageQuota", APIs["getGroupStorageQuota"].Run)
	grouter.HandleFunc("/getAllGroups", APIs["getAllGroups"].Run)
	grouter.HandleFunc("/getAllGroupsMembers", APIs["getAllGroupsMembers"].Run)
	grouter.HandleFunc("/getGroupAccessToResource", APIs["getGroupAccessToResource"].Run)

	// misc API cals
	grouter.HandleFunc("/getPasswdFile", APIs["getPasswdFile"].Run)
	grouter.HandleFunc("/getGroupFile", APIs["getGroupFile"].Run)
	grouter.HandleFunc("/getGridMapFile", APIs["getGridMapFile"].Run)
	grouter.HandleFunc("/getGridMapFileByVO", APIs["getGridMapFileByVO"].Run)
	grouter.HandleFunc("/getVORoleMapFile", APIs["getVORoleMapFile"].Run)
	grouter.HandleFunc("/getGroupGID", APIs["getGroupGID"].Run)
	grouter.HandleFunc("/getGroupName", APIs["getGroupName"].Run)
	grouter.HandleFunc("/lookupCertificateDN", APIs["lookupCertificateDN"].Run)
	grouter.HandleFunc("/getMappedGidFile", APIs["getMappedGidFile"].Run)
	grouter.HandleFunc("/getStorageAuthzDBFile", APIs["getStorageAuthzDBFile"].Run)
	grouter.HandleFunc("/getAffiliationMembersRoles", APIs["getAffiliationMembersRoles"].Run)
	grouter.HandleFunc("/getStorageAccessLists", APIs["getStorageAccessLists"].Run)
	grouter.HandleFunc("/createComputeResource", APIs["createComputeResource"].Run)
	grouter.HandleFunc("/setComputeResourceInfo", APIs["setComputeResourceInfo"].Run)
	grouter.HandleFunc("/createStorageResource", APIs["createStorageResource"].Run)
	grouter.HandleFunc("/setStorageResourceInfo", APIs["setStorageResourceInfo"].Run)
	grouter.HandleFunc("/getStorageResourceInfo", APIs["getStorageResourceInfo"].Run)
	grouter.HandleFunc("/getAllComputeResources", APIs["getAllComputeResources"].Run)
	grouter.HandleFunc("/getVOUserMap", APIs["getVOUserMap"].Run)
	grouter.HandleFunc("/setStorageQuota", APIs["setStorageQuota"].Run)
	grouter.HandleFunc("/cleanStorageQuotas", APIs["cleanStorageQuotas"].Run)
	grouter.HandleFunc("/cleanCondorQuotas", APIs["cleanCondorQuotas"].Run)
	grouter.HandleFunc("/ping", APIs["ping"].Run)

	grouter.HandleFunc("/testBaseAPI", APIs["testBaseAPI"].Run)

	//affiliation unit API calls
	grouter.HandleFunc("/createAffiliationUnit", APIs["createAffiliationUnit"].Run)
	grouter.HandleFunc("/removeAffiliationUnit", APIs["removeAffiliationUnit"].Run)
	grouter.HandleFunc("/setAffiliationUnitInfo", APIs["setAffiliationUnitInfo"].Run)
	grouter.HandleFunc("/getAffiliationUnitMembers", APIs["getAffiliationUnitMembers"].Run)
	grouter.HandleFunc("/getGroupsInAffiliationUnit", APIs["getGroupsInAffiliationUnit"].Run)
	grouter.HandleFunc("/getGroupLeadersinAffiliationUnit", APIs["getGroupLeadersinAffiliationUnit"].Run)
	grouter.HandleFunc("/getAffiliationUnitComputeResources", APIs["getAffiliationUnitComputeResources"].Run)
	grouter.HandleFunc("/createFQAN", APIs["createFQAN"].Run)
	grouter.HandleFunc("/removeFQAN", APIs["removeFQAN"].Run)
	grouter.HandleFunc("/setFQANMappings", APIs["setFQANMappings"].Run)
	grouter.HandleFunc("/getAllAffiliationUnits", APIs["getAllAffiliationUnits"].Run)

	//wrapper API calls
	grouter.HandleFunc("/testWrapper", APIs["testWrapper"].Run)
	grouter.HandleFunc("/addUsertoExperiment", APIs["addUserToExperiment"].Run)
	grouter.HandleFunc("/setLPCStorageAccess", APIs["setLPCStorageAccess"].Run)
	grouter.HandleFunc("/createExperiment", APIs["createExperiment"].Run)
	grouter.HandleFunc("/addLPCConvener", APIs["addLPCConvener"].Run)
	grouter.HandleFunc("/removeLPCConvener", APIs["removeLPCConvener"].Run)
	grouter.HandleFunc("/addLPCCollaborationGroup", APIs["addLPCCollaborationGroup"].Run)

	//legacy API calls
	// grouter.HandleFunc("/setSuperUser", setSuperUser)
	// grouter.HandleFunc("/removeSuperUser", removeSuperUser)
	// grouter.HandleFunc("/setGroupStorageQuota", setGroupStorageQuota)

	//never implemented API calls
	// grouter.HandleFunc("/setGroupBatchPriority", setGroupBatchPriority)
	// grouter.HandleFunc("/getAffiliationUnitStorageResources", getAffiliationUnitStorageResources)
	// grouter.HandleFunc("/deleteGroupt", deleteGroupt)
	// grouter.HandleFunc("/deleteGroup", deleteGroup)
	// grouter.HandleFunc("/removePrimaryStatusfromGroup", removePrimaryStatusfromGroup)

	srvConfig := viper.GetStringMapString("server")
	Mainsrv = &http.Server{
		Addr:        fmt.Sprintf(":%s", srvConfig["port"]),
		ReadTimeout: 10 * time.Second,
		Handler:     grouter,
		ConnState:   gatekeeper,
		ErrorLog:    golog.New(log.StandardLogger().WriterLevel(log.DebugLevel), "", 0),
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
