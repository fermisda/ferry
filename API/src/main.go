package main 
import (
	"fmt"
	"log"
	"database/sql"
	_ "github.com/lib/pq"
	"github.com/gorilla/mux"
	//"io"
	//"time"
	"net/http"
)

var DBptr *sql.DB

func handler(w http.ResponseWriter, r *http.Request) {
	fmt.Println(r.URL.Path)
	fmt.Fprintf(w, "This is a placeholder for paths like %s!", r.URL.Path[1:])
}

func main () {
	
	fmt.Println("Here we go...")
	Mydb, err := sql.Open("postgres","user=ferry password=ferry5634 host=fermicloud051.fnal.gov dbname=ferry connect_timeout=60 sslmode=disable")
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
	grouter.HandleFunc("/getUserGroups", getUserGroups)
	grouter.HandleFunc("/getUserInfo", getUserInfo)
	grouter.HandleFunc("/getUserCertificateDNs", getUserCertificateDNs)
	http.Handle("/", grouter)
	http.ListenAndServe(":8080", nil)
	
	defer Mydb.Close()
}
