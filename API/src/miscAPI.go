package main
import (
	"database/sql"
	"strconv"
	"log"
	"encoding/json"
	"fmt"
//"fmt"
//"log"
 _ "github.com/lib/pq"
"net/http"
//"encoding/json"
)

func NotDoneYet(w http.ResponseWriter) {
	fmt.Fprintf(w, `{"error": "This function is not done yet!"}`)
}

func getPasswdFile(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	
	unit := q.Get("unitname")
	comp := q.Get("resourcename")
	
	if unit == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No unitname specified in http query.")
		fmt.Fprintf(w,"{ \"error\": \"No unitname specified.\" }")
		return
	}
	if comp == "" {
		comp = "%"
	}

	rows, err := DBptr.Query(`select cname, uname, uid, gid, full_name, home_dir, shell, unit_exists, comp_exists from (
								select 1 as key, u.uname, u.uid, g.gid, u.full_name, ca.home_dir, ca.shell, cr.name as cname, au.name as aname
								from users as u 
								left join compute_access as ca on u.uid = ca.uid
								left join groups as g on ca.groupid = g.groupid
								left join compute_resources as cr on ca.compid = cr.compid
								left join affiliation_units as au on cr.unitid = au.unitid
								where au.name = $1 and cr.name like $2 order by cr.name
							) as t
								right join (select 1 as key,
								$1 in (select name from affiliation_units) as unit_exists,
								$2 in (select name from compute_resources) as comp_exists
							) as c on t.key = c.key;`, unit, comp)

	if err != nil {
		defer log.Fatal(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"error\": \"Error in DB query.\" }")
		return
	}
	defer rows.Close()

	var unitExists bool
	var compExists bool

	type jsonout struct {
		Uname string `json:"username"`
		Uid string `json:"uid"`
		Gid string `json:"gid"`
		Gecos string `json:"gecos"`
		Hdir string `json:"homedir"`
		Shell string `json:"shell"`
	}
	var Out jsonout

	prevRname := ""
	output := "[ { "
	for rows.Next() {
		var tmpRname, tmpUname, tmpUid, tmpGid, tmpGecos, tmpHdir, tmpShell sql.NullString
		rows.Scan(&tmpRname, &tmpUname, &tmpUid, &tmpGid, &tmpGecos, &tmpHdir, &tmpShell, &unitExists, &compExists)

		if tmpRname.Valid {
			if prevRname == "" {
				output += fmt.Sprintf(`"%s": [ `, tmpRname.String)
			} else if prevRname != tmpRname.String {
				output += fmt.Sprintf(` ], "%s": [ `, tmpRname.String)
			} else {
				output += ","
			}

			Out.Uname, Out.Uid, Out.Gid, Out.Gecos, Out.Hdir, Out.Shell = 
			tmpUname.String, tmpUid.String, tmpGid.String, tmpGecos.String, tmpHdir.String, tmpShell.String
			outline, jsonerr := json.Marshal(Out)
			if jsonerr != nil {
				log.Fatal(jsonerr)
			}
			output += string(outline)
			prevRname = tmpRname.String
		}
	}
	if prevRname == "" {
		w.WriteHeader(http.StatusNotFound)

		if !unitExists {
			fmt.Fprintf(w, `{ "error": "Affiliation unit does not exist." }`)
		} else if !compExists && comp != "%" {
			fmt.Fprintf(w, `{ "error": "Resource does not exist." }`)
		}
	} else {
		output += " ] } ]"
		fmt.Fprintf(w,output)
	}
}
func getGroupFile(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	
	unit := q.Get("unitname")
	comp := q.Get("resourcename")
	
	if unit == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No unitname specified in http query.")
		fmt.Fprintf(w,"{ \"error\": \"No unitname specified.\" }")
		return
	}
	if comp == "" {
		comp = "%"
	}

	rows, err := DBptr.Query(`select gname, groupid, uname, unit_exists, comp_exists from (
								select 1 as key, g.name as gname, ca.groupid, u.uname
								from compute_access as ca
								left join groups as g on ca.groupid = g.groupid
								left join users as u on ca.uid = u.uid
								left join compute_resources as cr on ca.compid = cr.compid
								left join affiliation_units as au on cr.unitid = au.unitid
								where au.name = $1 and cr.name like $2 order by ca.groupid
							) as t
								right join (select 1 as key,
								$1 in (select name from affiliation_units) as unit_exists,
								$2 in (select name from compute_resources) as comp_exists
							) as c on t.key = c.key;`, unit, comp)

	if err != nil {
		defer log.Fatal(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"error\": \"Error in DB query.\" }")
		return
	}
	defer rows.Close()

	var unitExists bool
	var compExists bool

	type jsonentry struct {
		Gname string `json:"groupname"`
		Gid string `json:"gid"`
		Unames []string `json:"unames"`
	}
	var Entry jsonentry
	var Out []jsonentry

	prevGname := ""
	for rows.Next() {
		var tmpGname, tmpGid, tmpUname sql.NullString
		rows.Scan(&tmpGname, &tmpGid, &tmpUname, &unitExists, &compExists)

		if tmpGname.Valid {
			if prevGname == "" {
				Entry.Gname = tmpGname.String
				Entry.Gid = tmpGid.String
				Entry.Unames = append(Entry.Unames, tmpUname.String)
			} else if prevGname != tmpGname.String {
				Out = append(Out, Entry)
				Entry.Gname = tmpGname.String
				Entry.Gid = tmpGid.String
				Entry.Unames = nil
				Entry.Unames = append(Entry.Unames, tmpUname.String)
			} else {
				Entry.Unames = append(Entry.Unames, tmpUname.String)
			}
			prevGname = tmpGname.String
		}
	}

	var out interface{}
	if prevGname == "" {
		w.WriteHeader(http.StatusNotFound)
		type jsonerror struct {Error string `json:"error"`}
		var Err []jsonerror
		if !unitExists {
			Err = append(Err, jsonerror{"Affiliation unit does not exist."})
		}
		if !compExists && comp != "%" {
			Err = append(Err, jsonerror{"Resource does not exist."})
		}
		out = Err
	} else {
		out = Out
	}
	output, err := json.Marshal(out)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Fprintf(w, string(output))
}
func getGridmapFile(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	unit := q.Get("unitname")
	if unit == "" {
		unit = "%"
	}

	rows, err := DBptr.Query(`select dn, uname, unit_exists from 
							 (select 1 as key, * from user_certificate as uc 
							  left join users as us on uc.uid = us.uid 
							  left join affiliation_units as au on uc.unitid = au.unitid
							  where au.name like $1) as t
	 						  right join (select 1 as key, $1 in (select name from affiliation_units) as unit_exists) as c on t.key = c.key`, unit)
	if err != nil {
		defer log.Fatal(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"error\": \"Error in DB query.\" }")
		return
	}
	defer rows.Close()

	var unitExists bool

	type jsonout struct {
		DN string `json:"userdn"`
		Uname string `json:"mapped_uname"`
	}
	var Out jsonout

	idx := 0
	output := "[ "
	for rows.Next() {
		if idx != 0 {
			output += ","
		}

		var tmpDN, tmpUname sql.NullString
		rows.Scan(&tmpDN, &tmpUname, &unitExists)
		if tmpDN.Valid {
			Out.DN, Out.Uname = tmpDN.String, tmpUname.String
			outline, jsonerr := json.Marshal(Out)
			if jsonerr != nil {
				log.Fatal(jsonerr)
			}
			output += string(outline)
			idx ++
		}
	}
	if idx == 0 {
		w.WriteHeader(http.StatusNotFound)

		if !unitExists {
			output += `"error": "Experiment does not exist.",`
		}
		output += `"error": "No DNs found."`
	}

	output += " ]"
	fmt.Fprintf(w,output)
}
func getVORoleMapFile(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	unit := q.Get("unitname")
	if unit == "" {
		unit = "%"
	}

	rows, err := DBptr.Query(`select t.fqan, t.mapped_user, c.unit_exists from
							 (select 1 as key, gf.fqan, gf.mapped_user, au.name from grid_fqan as gf
							  left join groups as g on gf.mapped_group = g.name
							  left join affiliation_unit_group as ag on g.groupid = ag.groupid
							  left join affiliation_units as au on ag.unitid = au.unitid
							  where au.name like $1) as t
							  right join (select 1 as key, $1 in (select name from affiliation_units) as unit_exists) as c on t.key = c.key`, unit)
	if err != nil {
		defer log.Fatal(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"error\": \"Error in DB query.\" }")
		return
	}
	defer rows.Close()

	var unitExists bool

	type jsonout struct {
		DN string `json:"fqan"`
		Uname string `json:"mapped_uname"`
	}
	var Out jsonout

	idx := 0
	output := "[ "
	for rows.Next() {
		if idx != 0 {
			output += ","
		}

		var tmpDN, tmpUname sql.NullString
		rows.Scan(&tmpDN, &tmpUname, &unitExists)
		if tmpDN.Valid {
			Out.DN, Out.Uname = tmpDN.String, tmpUname.String
			outline, jsonerr := json.Marshal(Out)
			if jsonerr != nil {
				log.Fatal(jsonerr)
			}
			output += string(outline)
			idx ++
		}
	}
	if idx == 0 {
		w.WriteHeader(http.StatusNotFound)

		if !unitExists {
			output += `"error": "Experiment does not exist.",`
		}
		output += `"error": "No FQANs found."`
	}

	output += " ]"
	fmt.Fprintf(w,output)
}

func getUserUID(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query() 
//	uname := q.Get("username")
	NotDoneYet(w)
}
func getUserUname(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query() 
//	uid := int(q.Get("uid"))
	NotDoneYet(w)
}
func getGroupGID(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	gName := q.Get("groupname")
	var iGid bool
	if gName == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No groupname specified in http query.")
		fmt.Fprintf(w,"{ \"error\": \"No groupname specified.\" }")
		return
	}
	if q.Get("include_gid") != "" {
		var err error
		iGid, err = strconv.ParseBool(q.Get("include_gid"))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			log.Print("Invalid include_gid specified in http query.")
			fmt.Fprintf(w,"{ \"error\": \"Invalid include_gid specified.\" }")
			return
		}
	}

	pingerr := DBptr.Ping()
	if pingerr != nil {
		log.Fatal(pingerr)
	}
	
	rows, err := DBptr.Query(`select groupid, gid from groups where name=$1`, gName)
	if err != nil {
		log.Fatal(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"Error in DB query\n")	
	} else {	
		defer rows.Close()

		type jsonout struct {
			Groupid int `json:"groupid"`;
			Gid int `json:"gid,omitempty"`;
		}
		var Out jsonout
		
		idx := 0
		for rows.Next() {
			if idx == 0 {
				fmt.Fprintf(w,"[ ")
			} else {
				fmt.Fprintf(w,",")
			}
			rows.Scan(&Out.Groupid, &Out.Gid)
			if !iGid {
				Out.Gid = 0
			}
			outline, jsonerr := json.Marshal(Out)
			if jsonerr != nil {
				log.Fatal(jsonerr)
			}
			fmt.Fprintf(w,string(outline))
			idx++
		}
		if idx == 0 {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w, `{ "error": "Group does not exist." }`)
		} else {
			fmt.Fprintf(w," ]")
		}		
	}
}

func getGroupName(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	gid := q.Get("gid")
	if gid == "" {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("No gid specified in http query.")
		fmt.Fprintf(w,"{ \"error\": \"No gid specified.\" }")
		return
	} else if _, err := strconv.Atoi(gid); err != nil  {
		w.WriteHeader(http.StatusBadRequest)
		log.Print("Invalid gid specified in http query.")
		fmt.Fprintf(w,"{ \"error\": \"Invalid gid specified.\" }")
		return
	}

	pingerr := DBptr.Ping()
	if pingerr != nil {
		log.Fatal(pingerr)
	}
	
	rows, err := DBptr.Query(`select name from groups where gid=$1`, gid)
	if err != nil {
		log.Fatal(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"Error in DB query\n")	
	} else {	
		defer rows.Close()

		type jsonout struct {
			Groupname string `json:"groupname"`
		}
		var Out jsonout
		
		idx := 0
		for rows.Next() {
			if idx == 0 {
				fmt.Fprintf(w,"[ ")
			} else {
				fmt.Fprintf(w,",")
			}
			rows.Scan(&Out.Groupname)
			outline, jsonerr := json.Marshal(Out)
			if jsonerr != nil {
				log.Fatal(jsonerr)
				}
			fmt.Fprintf(w,string(outline))
			idx++
			}
		if idx == 0 {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w, `{ "error": "Group does not exist." }`)
		} else {
			fmt.Fprintf(w," ]")
		}		
	}
}

func lookupCertificateDN(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
//	q := r.URL.Query() 
//	certDN := q.Get("certificatedn")
	NotDoneYet(w)
}
