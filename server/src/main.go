package main

import (
	"database/sql"
	"flag"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"

	"github.com/fsnotify/fsnotify"
	"github.com/go-openapi/runtime/middleware"

	"crypto/tls"
	"net/http"
	"os"
	"time"

	golog "log"

	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
	"github.com/patrickmn/go-cache"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var DBptr *sql.DB
var DBtx Transaction
var Mainsrv *http.Server
var ValidCAs CAs
var AccCache *cache.Cache
var FerryAlertsURL string
var serverRole string

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
	fields["duration"] = time.Since(c.StartTime).Nanoseconds() / 1e6

	clientIP := strings.Split(c.R.RemoteAddr, ":")[0]
	fields["client_ip"] = clientIP

	clientNames, err := net.LookupAddr(clientIP)
	if err == nil {
		fields["hostname"] = strings.Trim(clientNames[0], ".")
	} else {
		fields["hostname"] = Unknown
	}

	if len(c.Subject) > 0 {
		fields["subject"] = c.Subject
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

// @title FERRY API
// @version 2.2.3

// @description FERRY API Documentation.
// @description (For all APIs, you can also use ferry.fnal.gov:8445/api?help)
func main() {
	//Read command-line arguments
	var configDir string
	var configFile string
	flag.StringVar(&configDir, "c", ".", "Specify a configuration directory.")
	flag.StringVar(&configFile, "f", "default", "Specify the configuration file name.")
	flag.Parse()

	//Setup configutation manager
	viper.SetEnvPrefix("ferry")
	//password for DB is in PostgreSQL's .pgpass file
	viper.BindEnv("ldap_password")
	viper.BindEnv("ldap_readpassword")
	viper.SetConfigName(configFile)
	viper.AddConfigPath(configDir)
	cfgErr := viper.ReadInConfig()
	if cfgErr != nil {
		log.Error(cfgErr)
		panic(fmt.Errorf("fatal error config file: %s ", cfgErr))
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

	ldapErr := LDAPinitialize()
	if ldapErr != nil {
		log.Fatal(ldapErr)
	}

	ldapErr = AuthInitialize()
	if ldapErr != nil {
		log.Fatal(ldapErr)
	}

	APIs := make(APICollection)
	IncludeUserAPIs(&APIs)
	IncludeGroupAPIs(&APIs)
	IncludeMiscAPIs(&APIs)
	IncludeWrapperAPIs(&APIs)
	IncludeUnitAPIs(&APIs)
	IncludeResourceAPIs(&APIs)
	IncludeLdapAPIs(&APIs)

	log.Debug("Here we go...")

	//Make sure we are not running as root, and exit if we are.
	if os.Getuid() == 0 {
		log.Fatal("You are running as root (uid=0). Please run as a different user. Exiting.")
	}

	//NOTE: By default we have SSL mode set to "require" because the host cert on the DB machine is expired as of 10-25-2017.
	//		Once that is fixed we should set it to "verify-ca" or "verify-full" so that it actually checks that the cert that the DB machine presents is valid.
	//		If you set it to "require" it skips the verification step.
	dbConfig := viper.GetStringMapString("database")
	dbUser := dbConfig["user"]
	dbPass := dbConfig["password"]
	dbHost := dbConfig["host"]
	dbName := dbConfig["name"]
	dbPort := dbConfig["port"]
	// Allow the use of .pgpass by not requiring the password in the connection string
	connString := fmt.Sprintf("user=%s host=%s port=%s dbname=%s connect_timeout=%s sslmode=%s sslrootcert=%s",
		dbUser, dbHost, dbPort, dbName,
		dbConfig["timeout"], dbConfig["sslmode"], dbConfig["certificate"])
	if dbPass != "" {
		connString = fmt.Sprintf("%s password=%s", connString, dbPass)
	}
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

		accConfig := viper.GetStringMapString("accessors")
		expTime, err := strconv.Atoi(accConfig["expire"])
		if err != nil {
			log.Error("error converting expire")
			log.Fatal(err)
		}
		verTime, err := strconv.Atoi(accConfig["verify"])
		if err != nil {
			log.Error("error converting verify")
			log.Fatal(err)
		}
		AccCache = cache.New(time.Duration(expTime)*time.Minute, time.Duration(verTime)*time.Minute)

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

	srvConfig := viper.GetStringMapString("server")
	srvDocDir := srvConfig["docdir"]
	srvDocPath := srvConfig["docpath"]
	if (len(srvDocDir) == 0) && (len(srvDocPath) == 0) {
		log.Info("skipping swagger documentation, set server.docdir and server.docpath in the config file")
	} else {
		// Setup swagger documentation
		fs := http.FileServer((http.Dir(srvDocDir)))
		grouter.PathPrefix(srvDocPath + "/").Handler(http.StripPrefix(srvDocPath+"/", fs))
		opts := middleware.SwaggerUIOpts{SpecURL: srvDocPath + "/swagger.json"}
		sh := middleware.SwaggerUI(opts, nil)
		grouter.Handle(srvDocPath, sh)
		log.Info("swagger Documentation was not setup configured.")
	}

	//user API calls
	grouter.HandleFunc("/banUser", APIs["banUser"].Run)
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
	grouter.HandleFunc("/dropUser", APIs["dropUser"].Run)
	grouter.HandleFunc("/getUserUname", APIs["getUserUname"].Run)
	grouter.HandleFunc("/getUserUID", APIs["getUserUID"].Run)
	grouter.HandleFunc("/getMemberAffiliations", APIs["getMemberAffiliations"].Run)
	grouter.HandleFunc("/getUserAccessToComputeResources", APIs["getUserAccessToComputeResources"].Run)
	grouter.HandleFunc("/getStorageQuotas", APIs["getStorageQuotas"].Run)
	grouter.HandleFunc("/getAllUsers", APIs["getAllUsers"].Run)
	grouter.HandleFunc("/getAllUsersFQANs", APIs["getAllUsersFQANs"].Run)
	grouter.HandleFunc("/getAllUsersCertificateDNs", APIs["getAllUsersCertificateDNs"].Run)
	grouter.HandleFunc("/setUserGridAccess", APIs["setUserGridAccess"].Run)
	grouter.HandleFunc("/getUserGroupsForComputeResource", APIs["getUserGroupsForComputeResource"].Run)
	grouter.HandleFunc("/removeUserFromComputeResource", APIs["removeUserFromComputeResource"].Run)

	//group API calls
	grouter.HandleFunc("/getgroupmembers", APIs["getGroupMembers"].Run)
	grouter.HandleFunc("/getGroupMembers", APIs["getGroupMembers"].Run)
	grouter.HandleFunc("/createGroup", APIs["createGroup"].Run)
	grouter.HandleFunc("/addGroupToUnit", APIs["addGroupToUnit"].Run)
	grouter.HandleFunc("/removeGroupFromUnit", APIs["removeGroupFromUnit"].Run)
	grouter.HandleFunc("/setGroupRequired", APIs["setGroupRequired"].Run)
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
	grouter.HandleFunc("/getAffiliationMembers", APIs["getAffiliationMembers"].Run)
	grouter.HandleFunc("/getGroupsInAffiliationUnit", APIs["getGroupsInAffiliationUnit"].Run)
	grouter.HandleFunc("/getGroupLeadersinAffiliationUnit", APIs["getGroupLeadersinAffiliationUnit"].Run)
	grouter.HandleFunc("/getAffiliationUnitComputeResources", APIs["getAffiliationUnitComputeResources"].Run)
	grouter.HandleFunc("/createFQAN", APIs["createFQAN"].Run)
	grouter.HandleFunc("/removeFQAN", APIs["removeFQAN"].Run)
	grouter.HandleFunc("/setFQANMappings", APIs["setFQANMappings"].Run)
	grouter.HandleFunc("/getAllAffiliationUnits", APIs["getAllAffiliationUnits"].Run)

	//wrapper API calls
	grouter.HandleFunc("/testWrapper", APIs["testWrapper"].Run)
	grouter.HandleFunc("/addUserToExperiment", APIs["addUserToExperiment"].Run) //Added to fix missed init-cap on next line
	grouter.HandleFunc("/addUsertoExperiment", APIs["addUserToExperiment"].Run)
	grouter.HandleFunc("/setLPCStorageAccess", APIs["setLPCStorageAccess"].Run)
	grouter.HandleFunc("/createExperiment", APIs["createExperiment"].Run)
	grouter.HandleFunc("/addLPCConvener", APIs["addLPCConvener"].Run)
	grouter.HandleFunc("/removeLPCConvener", APIs["removeLPCConvener"].Run)
	grouter.HandleFunc("/addLPCCollaborationGroup", APIs["addLPCCollaborationGroup"].Run)

	// resource API calls
	grouter.HandleFunc("/getUsersForSharedAccountComputeResource", APIs["getUsersForSharedAccountComputeResource"].Run)
	grouter.HandleFunc("/addUserToSharedAccountComputeResource", APIs["addUserToSharedAccountComputeResource"].Run)
	grouter.HandleFunc("/removeUserFromSharedAccountComputeResource", APIs["removeUserFromSharedAccountComputeResource"].Run)
	grouter.HandleFunc("/setSharedAccountComputeResourceApprover", APIs["setSharedAccountComputeResourceApprover"].Run)
	grouter.HandleFunc("/getSharedAccountForComputeResource", APIs["getSharedAccountForComputeResource"].Run)

	// ldap API Calls
	grouter.HandleFunc("/syncLdapWithFerry", APIs["syncLdapWithFerry"].Run)
	grouter.HandleFunc("/getUserLdapInfo", APIs["getUserLdapInfo"].Run)
	grouter.HandleFunc("/removeUserFromLdap", APIs["removeUserFromLdap"].Run)
	grouter.HandleFunc("/getCapabilitySet", APIs["getCapabilitySet"].Run)
	grouter.HandleFunc("/createCapabilitySet", APIs["createCapabilitySet"].Run)
	grouter.HandleFunc("/setCapabilitySetAttributes", APIs["setCapabilitySetAttributes"].Run)
	grouter.HandleFunc("/dropCapabilitySet", APIs["dropCapabilitySet"].Run)
	grouter.HandleFunc("/addScopeToCapabilitySet", APIs["addScopeToCapabilitySet"].Run)
	grouter.HandleFunc("/removeScopeFromCapabilitySet", APIs["removeScopeFromCapabilitySet"].Run)
	grouter.HandleFunc("/addCapabilitySetToFQAN", APIs["addCapabilitySetToFQAN"].Run)
	grouter.HandleFunc("/removeCapabilitySetFromFQAN", APIs["removeCapabilitySetFromFQAN"].Run)
	grouter.HandleFunc("/addOrUpdateUserInLdap", APIs["addOrUpdateUserInLdap"].Run)
	grouter.HandleFunc("/updateLdapForAffiliation", APIs["updateLdapForAffiliation"].Run)
	grouter.HandleFunc("/updateLdapForCapabilitySet", APIs["updateLdapForCapabilitySet"].Run)
	grouter.HandleFunc("/modifyUserLdapAttributes", APIs["modifyUserLdapAttributes"].Run)

	Mainsrv = &http.Server{
		Addr:        fmt.Sprintf("%s", srvConfig["port"]),
		ReadTimeout: 10 * time.Second,
		Handler:     grouter,
		ConnState:   gatekeeper,
		ErrorLog:    golog.New(log.StandardLogger().WriterLevel(log.DebugLevel), "", 0),
	}

	serverRole = srvConfig["role"]
	if len(serverRole) == 0 {
		serverRole = "unknown"
	}

	FerryAlertsURL = srvConfig["ferryalertsurl"]
	if len(FerryAlertsURL) == 0 {
		log.Warning("ferryalertsurl not defined in config file")
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
	// Use the constants defined in the tls package.
	// As of go 1.17 order no longer matters.
	Ciphers := []uint16{
		tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
		tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
		tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
		tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,
		tls.TLS_AES_128_GCM_SHA256,
		tls.TLS_AES_256_GCM_SHA384}

	Mainsrv.TLSConfig = &tls.Config{
		ClientAuth:               tls.VerifyClientCertIfGiven,
		ClientCAs:                Certpool,
		GetConfigForClient:       checkClientIP,
		Certificates:             nil,
		MinVersion:               tls.VersionTLS12,
		InsecureSkipVerify:       false,
		CipherSuites:             Ciphers,
		PreferServerCipherSuites: true,
	}

	// We should probably make the cert and key paths variables in a config file at some point
	log.WithFields(log.Fields{"port": Mainsrv.Addr[1:]}).Infof("Starting FERRY API Database: %s  ldap: %s", dbName, ldapURL)
	serverror := Mainsrv.ListenAndServeTLS(srvConfig["cert"], srvConfig["key"])
	if serverror != nil {
		log.Fatal(serverror)
	}
	defer Mydb.Close()
}
