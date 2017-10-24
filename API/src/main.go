package main 
import (
	"fmt"
	"log"
	"database/sql"
	_ "github.com/lib/pq"
	"github.com/gorilla/mux"
	//"io"
	"time"
	"net/http"
	"crypto/tls"
)

var DBptr *sql.DB

func handler(w http.ResponseWriter, r *http.Request) {
	fmt.Println(r.URL.Path)
	fmt.Fprintf(w, "This is a placeholder for paths like %s!", r.URL.Path[1:])
}

func main () {
	
	fmt.Println("Here we go...")
//NOTE: here we have SSL mode set to "require" because the host cert on the DB machine is expired as of 10-25-2017. Once that is fixed we should set it to "verify-ca" or "verify-full" so that it actually checks that the cert that the DB machine presents is valid. If you set it to "require" it skips the verification step.
	Mydb, err := sql.Open("postgres","user=ferry password=ferry5634 host=fermicloud051.fnal.gov dbname=ferry connect_timeout=60 sslmode=require")
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
	grouter.HandleFunc("/getUserFQANs"	      , getUserFQANs)     
	grouter.HandleFunc("/getSuperUserList"     , getSuperUserList)      
	grouter.HandleFunc("/getUserGroups"	      , getUserGroups)	       
	grouter.HandleFunc("/getUserInfo"          , getUserInfo)     
	
	//group API calls
	grouter.HandleFunc("/getgroupmembers", getGroupMembers)
	grouter.HandleFunc("/createGroup",                  createGroup)                 
	grouter.HandleFunc("/deleteGroupt", 		    deleteGroupt)                
	grouter.HandleFunc("/deleteGroup", 		    deleteGroup)                 
	grouter.HandleFunc("/addGroupToUnit", 		    addGroupToUnit)              
	grouter.HandleFunc("/removeGroupFromUnit", 	    removeGroupFromUnit)         
	grouter.HandleFunc("/setPrimaryStatusGroup", 	    setPrimaryStatusGroup)       
	grouter.HandleFunc("/removePrimaryStatusfromGroup", removePrimaryStatusfromGroup)
	grouter.HandleFunc("/getGroupMembers", 		    getGroupMembers)             
	grouter.HandleFunc("/IsUserLeaderOf", 		    IsUserLeaderOf)              
	grouter.HandleFunc("/setGroupLeader", 		    setGroupLeader)              
	grouter.HandleFunc("/removeGroupLeader", 	    removeGroupLeader)           
	grouter.HandleFunc("/getGroupUnits", 		    getGroupUnits)               
	grouter.HandleFunc("/getGroupBatchPriorities", 	    getGroupBatchPriorities)     
	grouter.HandleFunc("/getGroupCondorQuotas", 	    getGroupCondorQuotas)        
	grouter.HandleFunc("/setGroupBatchPriority", 	    setGroupBatchPriority)       
	grouter.HandleFunc("/setGroupCondorQuota", 	    setGroupCondorQuota)         
	grouter.HandleFunc("/getGroupStorageQuotas", 	    getGroupStorageQuotas)       
	grouter.HandleFunc("/setGroupStorageQuota", 	    setGroupStorageQuota)        
	grouter.HandleFunc("/setUserAccessToResource", 	    setUserAccessToResource)     
	grouter.HandleFunc("/removeUserAccessFromResource", removeUserAccessFromResource)
	// misc API cals
	grouter.HandleFunc("/getPasswdFile",      getPasswdFile)       
	grouter.HandleFunc("/getGroupFile", 	  getGroupFile)      
	grouter.HandleFunc("/getGridmapFile", 	  getGridmapFile)    
	grouter.HandleFunc("/getVORoleMapFile",   getVORoleMapFile)  
	grouter.HandleFunc("/getUserUID", 	  getUserUID)        
	grouter.HandleFunc("/getUserUname", 	  getUserUname)      
	grouter.HandleFunc("/getGroupGID", 	  getGroupGID)       
	grouter.HandleFunc("/getGroupName", 	  getGroupName)      
	grouter.HandleFunc("/lookupCertificateDN",lookupCertificateDN)
	//collaboration unit API calls
	grouter.HandleFunc("/createCollaborationUnit",             createCollaborationUnit)           
	grouter.HandleFunc("/removeCollaborationUnit", 		   removeCollaborationUnit)           
	grouter.HandleFunc("/setCollaborationUnitInfo", 	   setCollaborationUnitInfo)          
	grouter.HandleFunc("/getCollaborationUnitMembers",	   getCollaborationUnitMembers)       
	grouter.HandleFunc("/getGroupsInCollaborationUnit", 	   getGroupsInCollaborationUnit)      
	grouter.HandleFunc("/getGroupLeadersinCollaborationUnit",  getGroupLeadersinCollaborationUnit)
	grouter.HandleFunc("/getCollaborationUnitStorageResources",getCollaborationUnitStorageResources)
	grouter.HandleFunc("/getCollaborationUnitComputeResources",getCollaborationUnitComputeResources)
	grouter.HandleFunc("/createFQAN",			   createFQAN)                        
	grouter.HandleFunc("/removeFQAN",			   removeFQAN)                        
	grouter.HandleFunc("/setFQANMappings",                     setFQANMappings)                    
	
	mainsrv := &http.Server{
		Addr: ":8443",	
		ReadTimeout: 10*time.Second,
		Handler: grouter,
	}	     
	
	var certstring = [1]string{"/etc/pki/tls/certs/ca-bundle.crt"}
	var certslice []string = certstring[0:1]
	certpool, err := loadCerts(certslice)
	if err != nil {
		log.Fatal(err)
	}
	mainsrv.TLSConfig = &tls.Config{
		ClientAuth: tls.VerifyClientCertIfGiven,
//tls.RequireAndVerifyClientCert,
		ClientCAs:  certpool,
	}
	
//	fmt.Println(certpool.Subjects())

	serverror := mainsrv.ListenAndServeTLS("/etc/grid-security/hostcert.pem","/etc/grid-security/hostkey.pem")
//	serverror := http.ListenAndServeTLS(":8443","/etc/grid-security/hostcert.pem","/etc/grid-security/hostkey.pem",nil)
	if serverror != nil {
		log.Fatal(serverror)
	}
	defer Mydb.Close()
}
