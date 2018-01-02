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
	"github.com/spf13/viper"
)

var DBptr *sql.DB
var DBtx Transaction
var AuthorizedDNs []string
var Mainsrv *http.Server

func handler(w http.ResponseWriter, r *http.Request) {
	fmt.Println(r.URL.Path)
	fmt.Fprintf(w, "This is a placeholder for paths like %s!", r.URL.Path[1:])
}

func foo(c net.Conn, s http.ConnState) {
	log.Print(s)
}

func main() {

	fmt.Println("Here we go...")

	//Setup configutation manager
	viper.SetConfigName("default")
	viper.AddConfigPath(".")
	cfgErr := viper.ReadInConfig()
	if cfgErr != nil {
		panic(fmt.Errorf("Fatal error config file: %s \n", cfgErr))
	}

	//Setup log file
	generalConfig := viper.GetStringMapString("general")

	if len(generalConfig["log"]) > 0 {
		logFile, logErr := os.OpenFile(generalConfig["log"], os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
		if logErr != nil {
			panic(fmt.Errorf("Fatal error log file: %s \n", logErr))
		}
		defer logFile.Close()
		log.SetOutput(logFile)
	}

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
		fmt.Println("there is an issue here")
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
	grouter.HandleFunc("/setUserExperimentFQAN", setUserExperimentFQAN)
	grouter.HandleFunc("/setUserShellAndHomeDir", setUserShellAndHomeDir)
	grouter.HandleFunc("/getUserShellAndHomeDir", getUserShellAndHomeDir)
	grouter.HandleFunc("/setUserAccessToResource", setUserAccessToResource)
	grouter.HandleFunc("/removeUserAccessFromResource", removeUserAccessFromResource)
	grouter.HandleFunc("/getUserStorageQuota", getUserStorageQuota)
	grouter.HandleFunc("/setUserStorageQuota", setUserStorageQuota)
	grouter.HandleFunc("/getUserExternalAffiliationAttributes", getUserExternalAffiliationAttributes)
	grouter.HandleFunc("/addCertDNtoUser", addCertDNtoUser)
	grouter.HandleFunc("/setSuperUser", setSuperUser)
	grouter.HandleFunc("/removeUserCertificateDN", removeUserCertificateDN)
	grouter.HandleFunc("/setUserInfo", setUserInfo)
	grouter.HandleFunc("/setUserExternalAffiliationAttribute", setUserExternalAffiliationAttribute)
	grouter.HandleFunc("/removeUserExternalAffiliationAttribute", removeUserExternalAffiliationAttribute)
	grouter.HandleFunc("/createUser", createUser)
	//	grouter.HandleFunc("/deleteUser"    ,    deleteUser)
	//	grouter.HandleFunc("/getUserID"    ,    getUserID)
	grouter.HandleFunc("/getUserUname", getUserUname)
	grouter.HandleFunc("/getMemberAffiliations", getMemberAffiliations)

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

	//collaboration unit API calls
	grouter.HandleFunc("/createCollaborationUnit", createCollaborationUnit)
	grouter.HandleFunc("/removeCollaborationUnit", removeCollaborationUnit)
	grouter.HandleFunc("/setCollaborationUnitInfo", setCollaborationUnitInfo)
	grouter.HandleFunc("/getCollaborationUnitMembers", getCollaborationUnitMembers)
	grouter.HandleFunc("/getGroupsInCollaborationUnit", getGroupsInCollaborationUnit)
	grouter.HandleFunc("/getGroupLeadersinCollaborationUnit", getGroupLeadersinCollaborationUnit)
	grouter.HandleFunc("/getCollaborationUnitStorageResources", getCollaborationUnitStorageResources)
	grouter.HandleFunc("/getCollaborationUnitComputeResources", getCollaborationUnitComputeResources)
	grouter.HandleFunc("/createFQAN", createFQAN)
	grouter.HandleFunc("/removeFQAN", removeFQAN)
	grouter.HandleFunc("/setFQANMappings", setFQANMappings)


	srvConfig := viper.GetStringMapString("server")
	Mainsrv = &http.Server{
		Addr:        fmt.Sprintf(":%s", srvConfig["port"]),
		ReadTimeout: 10 * time.Second,
		Handler:     grouter,
		ConnState:	 foo,
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
	log.Printf("Authorized DN list created with %d entries.", len(AuthorizedDNs))
	if len(AuthorizedDNs) == 0 {
		log.Fatal("Authorized DN slice has zero elements.")
	}
	// We should probably make the cert and key paths variables in a config file at some point
	serverror := Mainsrv.ListenAndServeTLS(srvConfig["cert"], srvConfig["key"])
	if serverror != nil {
		log.Fatal(serverror)
	}
	defer Mydb.Close()
}
