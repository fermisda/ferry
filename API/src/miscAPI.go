package main
import (
	"database/sql"
	"strconv"
	log "github.com/sirupsen/logrus"
	"encoding/json"
	"fmt"
 	_ "github.com/lib/pq"
	"net/http"
	"time"
	"strings"
)

func NotDoneYet(w http.ResponseWriter, r *http.Request, t time.Time) {
	fmt.Fprintf(w, `{"ferry_error": "This function is not done yet!"}`)
	log.WithFields(QueryFields(r, t)).Error("This function is not done yet!")
}

func getPasswdFile(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	
	unit := q.Get("unitname")
	comp := q.Get("resourcename")

	rows, err := DBptr.Query(`select aname, rname, uname, uid, gid, full_name, home_dir, shell, unit_exists, comp_exists, last_updated from (
								select 1 as key, au.name as aname, u.uname, u.uid, g.gid, u.full_name, ca.home_dir, ca.shell, cr.name as rname, ca.last_updated as last_updated
								from users as u 
								left join compute_access as ca on u.uid = ca.uid
								left join groups as g on ca.groupid = g.groupid
								left join compute_resources as cr on ca.compid = cr.compid
								left join affiliation_units as au on cr.unitid = au.unitid
								where (au.name = $1 or $3) and (cr.name = $2 or $4) order by au.name, cr.name
							) as t
								right join (select 1 as key,
								$1 in (select name from affiliation_units) as unit_exists,
								$2 in (select name from compute_resources) as comp_exists
							) as c on t.key = c.key;`, unit, comp, unit == "", comp == "")

	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Error(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error in DB query.\" }")
		return
	}
	defer rows.Close()

	var unitExists bool
	var compExists bool

	type jsonuser struct {
		Uname string `json:"username"`
		Uid string `json:"uid"`
		Gid string `json:"gid"`
		Gecos string `json:"gecos"`
		Hdir string `json:"homedir"`
		Shell string `json:"shell"`
	}
	type jsonunit struct {
		Resources map[string][]jsonuser `json:"resources"`
		Lasttime int64 `json:"last_updated"`
	}
	Out := make(map[string]jsonunit)

	lasttime := int64(0)
	prevAname := ""
	prevRname := ""
	tmpResources := make(map[string][]jsonuser, 0)
	tmpUsers := make([]jsonuser, 0)
	for rows.Next() {
		var tmpAname, tmpRname, tmpUname, tmpUid, tmpGid, tmpGecos, tmpHdir, tmpShell,tmpTime sql.NullString
		rows.Scan(&tmpAname, &tmpRname, &tmpUname, &tmpUid, &tmpGid, &tmpGecos, &tmpHdir, &tmpShell, &unitExists, &compExists, &tmpTime)

		if prevRname == "" {
			prevRname = tmpRname.String
		}
		if prevAname == "" {
			prevAname = tmpAname.String
		}

		if tmpRname.Valid {
			if prevRname != tmpRname.String {
				tmpResources[prevRname] = tmpUsers
				tmpUsers = make([]jsonuser, 0)
				prevRname = tmpRname.String
			}
			if prevAname != tmpAname.String {
				Out[prevAname] = jsonunit{tmpResources, lasttime}
				tmpResources = make(map[string][]jsonuser, 0)
				lasttime = 0
				prevAname = tmpAname.String
			}
			if tmpTime.Valid {
				log.WithFields(QueryFields(r, startTime)).Println("tmpTime is valid" + tmpTime.String)
				
				parseTime,parserr := time.Parse(time.RFC3339,tmpTime.String)
				if parserr != nil {
					log.WithFields(QueryFields(r, startTime)).Error("Error parsing last updated time of " + tmpTime.String)
				} else {
					if lasttime == 0  || ( parseTime.Unix() > lasttime) {
						lasttime = parseTime.Unix()
					}
				}
			} else {
				log.WithFields(QueryFields(r, startTime)).Println("tmpTime is not valid")
			}
			tmpUsers = append(tmpUsers, jsonuser{tmpUname.String, tmpUid.String, tmpGid.String, tmpGecos.String, tmpHdir.String, tmpShell.String})
		}
	}
	if prevAname != "" {
		tmpResources[prevRname] = tmpUsers
		Out[prevAname] = jsonunit{tmpResources, lasttime}
	}
	
	var output interface{}
	if len(Out) == 0 {
		type jsonerror struct {Error string `json:"ferry_error"`}
		var Err []jsonerror
		if !unitExists {
			Err = append(Err, jsonerror{"Affiliation unit does not exist."})
			log.WithFields(QueryFields(r, startTime)).Error("Affiliation unit does not exist.")
		}
		if !compExists && comp != "%" {
			Err = append(Err, jsonerror{"Resource does not exist."})
			log.WithFields(QueryFields(r, startTime)).Error("Resource does not exist.")
		}
		if len(Err) == 0 {
			Err = append(Err, jsonerror{"Something went wrong."})
			log.WithFields(QueryFields(r, startTime)).Error("Something went wrong.")
		}
		output = Err
	} else {
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		output = Out
	}
	jsonout, err := json.Marshal(output)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
	}
	fmt.Fprintf(w, string(jsonout))
}
func getGroupFile(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	
	unit := q.Get("unitname")
	comp := q.Get("resourcename")
	
	if unit == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No unitname specified in http query.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No unitname specified.\" }")
		return
	}
	if comp == "" {
		comp = "%"
	}

	rows, err := DBptr.Query(`select gname, groupid, uname, unit_exists, comp_exists, last_updated from (
								select 1 as key, g.name as gname, ca.groupid, u.uname, ca.last_updated
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
		defer log.WithFields(QueryFields(r, startTime)).Error(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error in DB query.\" }")
		return
	}
	defer rows.Close()

	var unitExists bool
	var compExists bool

	type jsonentry struct {
		Gname string `json:"groupname"`
		Gid string `json:"gid"`
		Lasttime int64 `json:"last_updated"`
		Unames []string `json:"unames"`
	}
	var Entry jsonentry
	var Out []jsonentry

	prevGname := ""
	for rows.Next() {
		var tmpGname, tmpGid, tmpUname, tmpTime sql.NullString
		rows.Scan(&tmpGname, &tmpGid, &tmpUname, &unitExists, &compExists, &tmpTime)

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
			if tmpTime.Valid {
				log.WithFields(QueryFields(r, startTime)).Println("tmpTime is valid" + tmpTime.String)
				
				parseTime,parserr := time.Parse(time.RFC3339,tmpTime.String)
				lasttime := &Entry.Lasttime
				if parserr != nil {
					log.WithFields(QueryFields(r, startTime)).Error("Error parsing last updated time of " + tmpTime.String)
				} else {
					if *lasttime == 0  || ( parseTime.Unix() > *lasttime) {
						*lasttime = parseTime.Unix()
					}
				}
			} else {
				log.WithFields(QueryFields(r, startTime)).Println("tmpTime is not valid")
			}
		}
	}
	Out = append(Out, Entry)
	
	var output interface{}
	if prevGname == "" {
		type jsonerror struct {Error string `json:"ferry_error"`}
		var Err []jsonerror
		if !unitExists {
			Err = append(Err, jsonerror{"Affiliation unit does not exist."})
			log.WithFields(QueryFields(r, startTime)).Error("Affiliation unit does not exist.")
		}
		if !compExists && comp != "%" {
			Err = append(Err, jsonerror{"Resource does not exist."})
			log.WithFields(QueryFields(r, startTime)).Error("Resource does not exist.")
		}
		if len(Err) == 0 {
			Err = append(Err, jsonerror{"Something went wrong."})
			log.WithFields(QueryFields(r, startTime)).Error("Something went wrong.")
		}
		output = Err
	} else {
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		output = Out
	}
	jsonout, err := json.Marshal(output)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
	}
	fmt.Fprintf(w, string(jsonout))
}
func getGridMapFile(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	unit := q.Get("unitname")
	if unit == "" {
		unit = "%"
	}

	rows, err := DBptr.Query(`select name, dn, uname, unit_exists from 
							 (select 1 as key, au.name, uc.dn, us.uname from  affiliation_unit_user_certificate as ac
								left join user_certificates as uc on ac.dnid = uc.dnid
								left join users as us on uc.uid = us.uid
								left join affiliation_units as au on ac.unitid = au.unitid
								where au.name like $1) as t
	 						  right join (select 1 as key, $1 in (select name from affiliation_units) as unit_exists) as c on t.key = c.key`, unit)
	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Error(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error in DB query.\" }")
		return
	}
	defer rows.Close()

	var unitExists bool

	type jsonentry struct {
		DN string `json:"userdn"`
		Uname string `json:"mapped_uname"`
	}
	var dnmap jsonentry
	Out := make(map[string][]jsonentry)

	for rows.Next() {
		var tmpAname, tmpDN, tmpUname sql.NullString
		rows.Scan(&tmpAname, &tmpDN, &tmpUname, &unitExists)
		if tmpDN.Valid {
			dnmap.DN, dnmap.Uname = tmpDN.String, tmpUname.String
			Out[tmpAname.String] = append(Out[tmpAname.String], dnmap)
		}
	}

	var output interface{}
	if len(Out) == 0 {
		type jsonerror struct {Error []string `json:"ferry_error"`}
		var Err jsonerror

		if !unitExists {
			Err.Error = append(Err.Error, "Experiment does not exist.")
			log.WithFields(QueryFields(r, startTime)).Error("Experiment does not exist.")
		}
		Err.Error = append(Err.Error, "No DNs found.")
		log.WithFields(QueryFields(r, startTime)).Error("No DNs found.")

		output = Err
	} else {
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		output = Out
	}
	jsonout, err := json.Marshal(output)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
	}
	fmt.Fprintf(w, string(jsonout))
}
func getVORoleMapFile(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
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
		defer log.WithFields(QueryFields(r, startTime)).Error(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error in DB query.\" }")
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
				log.WithFields(QueryFields(r, startTime)).Error(jsonerr)
			}
			output += string(outline)
			idx ++
		}
	}
	if idx == 0 {
		if !unitExists {
			output += `"ferry_error": "Experiment does not exist.",`
			log.WithFields(QueryFields(r, startTime)).Error("Experiment does not exist.")
		}
		output += `"ferry_error": "No FQANs found."`
		log.WithFields(QueryFields(r, startTime)).Error("No FQANs found.")
	} else {
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
	}

	output += " ]"
	fmt.Fprintf(w,output)
}

func getGroupGID(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	gName := q.Get("groupname")
	var iGid bool
	if gName == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No groupname specified in http query.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No groupname specified.\" }")
		return
	}
	if q.Get("include_gid") != "" {
		var err error
		iGid, err = strconv.ParseBool(q.Get("include_gid"))
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Invalid include_gid specified in http query.")
			fmt.Fprintf(w,"{ \"ferry_error\": \"Invalid include_gid specified.\" }")
			return
		}
	}

	pingerr := DBptr.Ping()
	if pingerr != nil {
		log.WithFields(QueryFields(r, startTime)).Error(pingerr)
	}
	
	rows, err := DBptr.Query(`select groupid, gid from groups where name=$1`, gName)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
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
				log.WithFields(QueryFields(r, startTime)).Error(jsonerr)
			}
			fmt.Fprintf(w,string(outline))
			idx++
		}
		if idx == 0 {
			log.WithFields(QueryFields(r, startTime)).Error("Group does not exist.")
			fmt.Fprintf(w, `{ "ferry_error": "Group does not exist." }`)
		} else {
			log.WithFields(QueryFields(r, startTime)).Info("Success!")
			fmt.Fprintf(w," ]")
		}		
	}
}

func getGroupName(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	gid := q.Get("gid")
	if gid == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No gid specified in http query.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No gid specified.\" }")
		return
	} else if _, err := strconv.Atoi(gid); err != nil  {
		log.WithFields(QueryFields(r, startTime)).Error("Invalid gid specified in http query.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"Invalid gid specified.\" }")
		return
	}

	pingerr := DBptr.Ping()
	if pingerr != nil {
		log.WithFields(QueryFields(r, startTime)).Error(pingerr)
	}
	
	rows, err := DBptr.Query(`select name from groups where gid=$1`, gid)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
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
				log.WithFields(QueryFields(r, startTime)).Error(jsonerr)
				}
			fmt.Fprintf(w,string(outline))
			idx++
			}
		if idx == 0 {
			log.WithFields(QueryFields(r, startTime)).Error("Group does not exist.")
			fmt.Fprintf(w, `{ "ferry_error": "Group does not exist." }`)
		} else {
			log.WithFields(QueryFields(r, startTime)).Info("Success!")
			fmt.Fprintf(w," ]")
		}		
	}
}
func lookupCertificateDN(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	type jsonerror struct {
		Error string `json:"ferry_error"`
	}
	var inputErr []jsonerror

	certdn := q.Get("certificatedn")

	if certdn == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No certificatedn name specified in http query.")
		inputErr = append(inputErr, jsonerror{"No certificatedn name specified."})
	}

	if len(inputErr) > 0 {
		jsonout, err := json.Marshal(inputErr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error(err)
		}
		fmt.Fprintf(w, string(jsonout))
		return
	}

	rows, err := DBptr.Query(`select u.uid, uname from user_certificates as uc left join users as u on uc.uid = u.uid where dn = $1;`, certdn)
	if err != nil {	
		defer log.WithFields(QueryFields(r, startTime)).Error(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error in DB query.\" }")
		return
	}	
	defer rows.Close()

	type jsonentry struct {
		Uid  string `json:"uid"`
		Uname  string `json:"uname"`
	}
	var Entry jsonentry
	var Out []jsonentry

	for rows.Next() {
		var tmpUid, tmpUname sql.NullString
		rows.Scan(&tmpUid, &tmpUname)

		if tmpUid.Valid {
			Entry.Uid = tmpUid.String
			Entry.Uname = tmpUname.String
			Out = append(Out, Entry)
		}
	}

	var output interface{}
	if len(Out) == 0 {
		var queryErr []jsonerror
		log.WithFields(QueryFields(r, startTime)).Error("Certificate DN does not exist.")
		queryErr = append(queryErr, jsonerror{"Certificate DN does not exist."})
		output = queryErr
	} else {
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		output = Out
	}
	jsonout, err := json.Marshal(output)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
	}
	fmt.Fprintf(w, string(jsonout))
}
func getMappedGidFile(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

	rows, err := DBptr.Query(`select fqan, mapped_user, gid from grid_fqan as gf left join groups as g on g.name = gf.mapped_group`)

	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Error(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error in DB query.\" }")
		return
	}
	defer rows.Close()

	type jsonentry struct {
		Fqan string `json:"fqan"`
		User string `json:"mapped_uname"`
		Gid string `json:"mapped_gid"`
	}
	var Entry jsonentry
	var Out []jsonentry

	for rows.Next() {
		var tmpFqan, tmpUser, tmpGid sql.NullString
		rows.Scan(&tmpFqan, &tmpUser, &tmpGid)

		if tmpFqan.Valid {
			Entry = jsonentry{tmpFqan.String, tmpUser.String, tmpGid.String}
			Out = append(Out, Entry)
		}
	}

	var output interface{}
	if len(Out) == 0 {
		type jsonerror struct {Error string `json:"ferry_error"`}
		var Err jsonerror
		Err = jsonerror{"Something went wrong."}
		log.WithFields(QueryFields(r, startTime)).Error("Something went wrong.")
		output = Err
	} else {
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		output = Out
	}
	jsonout, err := json.Marshal(output)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
	}
	fmt.Fprintf(w, string(jsonout))
}
func getStorageAuthzDBFile(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

	rows, err := DBptr.Query(`select u.uname, u.uid, g.gid from users as u
							  right join user_group as ug on u.uid = ug.uid
							  left join groups as g on ug.groupid = g.groupid
							  order by u.uname;`)

	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Error(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error in DB query.\" }")
		return
	}
	defer rows.Close()

	type jsonentry struct {
		Decision string `json:"decision"`
		User string `json:"username"`
		Privileges string `json:"privileges"`
		Uid string `json:"uid"`
		Gid []string `json:"gid"`
		Home string `json:"home"`
		Root string `json:"root"`
		FsPath string `json:"fs_path"`
	}
	var Entry jsonentry
	var Out []jsonentry

	prevUser := ""
	for rows.Next() {
		var tmpUser, tmpUid, tmpGid sql.NullString
		rows.Scan(&tmpUser, &tmpUid, &tmpGid)

		if tmpUser.Valid {
			if prevUser == "" {
				Entry.Decision = "authorize"
				Entry.User = tmpUser.String
				Entry.Privileges = "read-write"
				Entry.Uid = tmpUid.String
				Entry.Gid = append(Entry.Gid, tmpGid.String)
				Entry.Home = "/"
				Entry.Root = "/pnfs/fnal.gov/usr"
				Entry.FsPath = "/"
			} else if prevUser != tmpUser.String {
				Out = append(Out, Entry)
				Entry.Decision = "authorize"
				Entry.User = tmpUser.String
				Entry.Privileges = "read-write"
				Entry.Uid = tmpUid.String
				Entry.Gid = nil
				Entry.Gid = append(Entry.Gid, tmpGid.String)
				Entry.Home = "/"
				Entry.Root = "/pnfs/fnal.gov/usr"
				Entry.FsPath = "/"
			} else {
				Entry.Gid = append(Entry.Gid, tmpGid.String)
			}
			prevUser = tmpUser.String
		}
	}
	Out = append(Out, Entry)

	var output interface{}
	if len(Out) == 0 {
		type jsonerror struct {Error string `json:"ferry_error"`}
		var Err jsonerror
		Err = jsonerror{"Something went wrong."}
		log.WithFields(QueryFields(r, startTime)).Error("Something went wrong.")
		output = Err
	} else {
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		output = Out
	}
	jsonout, err := json.Marshal(output)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
	}
	fmt.Fprintf(w, string(jsonout))
}
func getAffiliationMembersRoles(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	
	unit := q.Get("experimentname")
	role := q.Get("rolename")

	if unit == "" {
		unit = "%"
	}
	if role == "" {
		role = "%"
	} else {
		role = "%" + role + "%"
	}

	rows, err := DBptr.Query(`select t.name, t.fqan, t.uname, t.full_name, unit_exists, fqan_exists from (
								select 1 as key, au.name, gf.fqan, u.uname, u.full_name
								from grid_access as ga
								left join grid_fqan as gf on ga.fqanid = gf.fqanid
								left join users as u on ga.uid = u.uid
								left join affiliation_units as au on ga.unitid = au.unitid
								where au.name like $1 and gf.fqan like $2
							) as t right join (
								select 1 as key,
								$1 in (select name from affiliation_units) as unit_exists,
								$2 in (select fqan from grid_fqan) as fqan_exists
							) as c on t.key = c.key;`, unit, role)

	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Error(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error in DB query.\" }")
		return
	}
	defer rows.Close()

	var unitExists bool
	var roleExists bool

	type jsonentry struct {
		Fqan string `json:"fqan"`
		User string `json:"username"`
		Name string `json:"commonname"`
	}
	Out := make(map[string][]jsonentry)

	for rows.Next() {
		var tmpUnit, tmpFqan, tmpUser, tmpName sql.NullString
		rows.Scan(&tmpUnit, &tmpFqan, &tmpUser, &tmpName, &unitExists, &roleExists)

		if tmpFqan.Valid {
			Out[tmpUnit.String] = append(Out[tmpUnit.String], jsonentry{tmpFqan.String, tmpUser.String, tmpName.String})
		}
	}

	var output interface{}
	if len(Out) == 0 {
		type jsonerror struct {Error []string `json:"ferry_error"`}
		var Err jsonerror
		if !unitExists {
			Err.Error = append(Err.Error, "Experiment does not exist.")
			log.WithFields(QueryFields(r, startTime)).Error("Experiment does not exist.")
		}
		if !roleExists {
			Err.Error = append(Err.Error, "Role does not exist.")
			log.WithFields(QueryFields(r, startTime)).Error("Role does not exist.")
		}
		if len(Err.Error) == 0 {
			Err.Error = append(Err.Error, "No roles were found")
			log.WithFields(QueryFields(r, startTime)).Error("No roles were found")
		}
		output = Err
	} else {
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		output = Out
	}
	jsonout, err := json.Marshal(output)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
	}
	fmt.Fprintf(w, string(jsonout))
}
func getStorageAccessLists(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	
	resource := q.Get("resourcename")

	if resource == "" {
		resource = "%"
	}
	/*if resource == "" {
		log.WithFields(QueryFields(r, startTime)).Error("No resourcename specified in http query.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No resourcename specified.\" }")
		return
	}*/

	rows, err := DBptr.Query(`select server, volume, access_level, host from nas_storage where server like $1;`, resource)

	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Error(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error in DB query.\" }")
		return
	}
	defer rows.Close()

	type jsonhost struct {
		Host string `json:"host"`
		Access string `json:"accesslevel"`
	}
	Out := make(map[string][]map[string][]jsonhost)
	Entry := make(map[string][]jsonhost)

	prevServer := ""
	for rows.Next() {
		var tmpServer, tmpVolume, tmpAccess, tmpHost sql.NullString
		rows.Scan(&tmpServer, &tmpVolume, &tmpAccess, &tmpHost)

		if tmpVolume.Valid {
			if prevServer != "" && prevServer != tmpServer.String {
				Out[prevServer] = append(Out[prevServer], Entry)
				Entry = make(map[string][]jsonhost)
			}
			Entry[tmpVolume.String] = append(Entry[tmpVolume.String], jsonhost{tmpHost.String, tmpAccess.String})
		}
		prevServer = tmpServer.String
	}
	Out[prevServer] = append(Out[prevServer], Entry)

	var output interface{}
	if prevServer == "" {
		type jsonerror struct {Error string `json:"ferry_error"`}
		var Err jsonerror
		Err = jsonerror{"Storage resource does not exist."}
		log.WithFields(QueryFields(r, startTime)).Error("Storage resource does not exist.")
		output = Err
	} else {
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		output = Out
	}
	jsonout, err := json.Marshal(output)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
	}
	fmt.Fprintf(w, string(jsonout))
}

func createComputeResource(w http.ResponseWriter, r *http.Request) {
	
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	
	rName := q.Get("resourcename")
	unitName := q.Get("unitname")
	rType := q.Get("type")
	shell := q.Get("default_shell")
	homedir := q.Get("default_home_dir")
	var nullshell,nullhomedir sql.NullString
	if rName == "" {
		log.WithFields(QueryFields(r, startTime)).Print("No resource name specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No resourcename specified.\" }")
		return
	}
	if rType == "" {
		log.WithFields(QueryFields(r, startTime)).Print("No resource type specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No type specified.\" }")
		return	
	} else if strings.ToUpper(rType) == "NULL" {
		log.WithFields(QueryFields(r, startTime)).Print("'NULL' is an invalid resource type.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"Resource type of NULL is not allowed.\" }")
		return	
	}
//	if unitName == "" {
//		unitName = "NULL"
	//	}
	if shell == "" || strings.ToUpper(strings.TrimSpace(shell)) == "NULL" {
		nullshell.Valid = false
	} else {
		nullshell.Valid = true
		nullshell.String = shell 
	}
	if homedir == "" ||  strings.ToUpper(strings.TrimSpace(homedir)) == "NULL" {
		nullhomedir.Valid=false
	} else {
		nullhomedir.Valid = true
		nullhomedir.String = homedir
	}

	//require auth	
	authorized,authout := authorize(r,AuthorizedDNs)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w,"{ \"ferry_error\": \"" + authout + "not authorized.\" }")
		return
	}

	var unitID sql.NullInt64

	var newcompId int
	// select the highest existing value of compid
	compiderr := DBptr.QueryRow(`select compid from compute_resources order by compid desc limit 1`).Scan(&newcompId)
	if compiderr != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error selecting compids: " + compiderr.Error())
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error determining new compid.\" }")
		return
	}
	//add one to the new compid
	newcompId += 1

	//figure out the unitID if we need it
	
	if unitName != "" {
		uniterr := DBptr.QueryRow(`select unitid from affiliation_units where name=$1`,unitName).Scan(&unitID)
		if uniterr != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Error determining unitid for " + unitName + ": " + uniterr.Error())
			fmt.Fprintf(w,"{ \"ferry_error\": \"Error determining unitID for " + unitName + ". You cannot add a unit name that does not already exist in affiliation_units.\" }")
			return
		}
	}
	
	//now, make sure the the resource does not already exist. If it does, bail out. If it does not, do the insertion

	var compId int;	
	checkerr := DBptr.QueryRow(`select compid from  compute_resources where name=$1`,rName).Scan(&compId)
	
	switch {
	case checkerr == sql.ErrNoRows:
		// OK, it does not already exist, so we start a transaction
		cKey, err := DBtx.Start(DBptr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Error starting DB transaction: " + err.Error())
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w,"{ \"ferry_error\": \"Error starting database transaction.\" }")
			return
		}
		
		//	addstr := fmt.Sprintf(`do declare cmpid bigint;  begin select compid into cmpid from compute_resources order by compid desc limit 1; if exists (select name from compute_resources where name=$1) then raise 'resource already exists'; else insert into compute_resources (compid, name, default_shell, unitid, last_updated, default_home_dir, type) values (cmpid+1,$1,$2,$3,NOW(),$4,$5); end if ;  end ;`)
		addstr := fmt.Sprintf(`insert into compute_resources (compid, name, default_shell, unitid, last_updated, default_home_dir, type) values ($1,$2,$3,$4,NOW(),$5,$6)`)
		
		
		//	err = DBtx.tx.QueryRow("do $$ declare cmpid bigint;  begin select compid into cmpid from compute_resources order by compid desc limit 1; if exists (select name from compute_resources where name=$1) then raise 'resource already exists'; else insert into compute_resources (compid, name, default_shell, unitid, last_updated, default_home_dir, type) values (cmpid+1,$1,$2,$3,NOW(),$4,$5) returning cmpid+1; end if ;  end $$ ;",rName,nullshell,unitID,nullhomedir,rType).Scan(&compId)
		
		_, err = DBtx.tx.Exec(addstr,newcompId,rName,nullshell,unitID,nullhomedir,rType)
		
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Error starting DB transaction: " + err.Error())
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w,"{ \"ferry_error\": \"Error in database transaction.\" }")
			//	DBtx.Rollback()
			return
		} else {
			DBtx.Commit(cKey)
			log.WithFields(QueryFields(r, startTime)).Error("Added " + rName + " to compute_resources.")
			fmt.Fprintf(w,"{ \"result\": \"success.\" }")
			return		
		}
		
	case checkerr != nil:
		//some other error, exit with status 500
		log.WithFields(QueryFields(r, startTime)).Error("Error in DB query: " + checkerr.Error())
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error in database check.\" }")
		return
	default:
		// if we get here, it means that the unit already exists. Bail out.
		log.WithFields(QueryFields(r, startTime)).Error("Resource " + rName + " already exists.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"Resource already exists.\" }")
		return	
	}

}

func setComputeResourceInfo(w http.ResponseWriter, r *http.Request) {
	
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	
	rName := q.Get("resourcename")
	unitName := q.Get("unitname")
	rType := q.Get("type")
	shell := q.Get("default_shell")
	homedir := q.Get("default_home_dir")

	if rName == "" {
		log.WithFields(QueryFields(r, startTime)).Print("No resource name specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No resourcename specified.\" }")
		return
	}
	if strings.ToUpper(strings.TrimSpace(rType)) == "NULL" {
		log.WithFields(QueryFields(r, startTime)).Print("'NULL' is an invalid resource type.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"Resource type of NULL is not allowed.\" }")
		return	
	}
	
	//require auth	
	authorized,authout := authorize(r,AuthorizedDNs)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w,"{ \"ferry_error\": \"" + authout + "not authorized.\" }")
		return
	}

	var ( 
		nullshell,nullhomedir sql.NullString
		unitID sql.NullInt64
		currentType string
		compid  int
	)

	// check if resource exists and grab existing values of everything if so
	err := DBptr.QueryRow(`select distinct compid, default_shell, unitid, default_home_dir, type from compute_resources where name=$1`,rName).Scan(&compid,&nullshell,&unitID,&nullhomedir,&currentType)
	switch {
	case err == sql.ErrNoRows:
		// nothing returned from the select, so the resource does not exist.
		log.WithFields(QueryFields(r, startTime)).Print("compute resource with name " + rName + " not found in compute_resources table. Exiting.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"resource does not exist. Use createComputeResource to add a new resource.\" }")
		return	
	case err != nil:
		w.WriteHeader(http.StatusNotFound)
		log.WithFields(QueryFields(r, startTime)).Print("Error in DQ query: " + err.Error())
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
		return	
	default:
		
		//actually change stuff
		// if you specfied a new type in the API call (not NULL, as checked for earlier), change it here. Otherwise we keep the existing one
		if rType != "" { 
			currentType = rType
		}
		// if you are changing the shell type, do it here. Variations of "NULL" as the string will assume you want it to be null in the database. If you did not specify shell in the query, then we keep the existing value.
		if shell != "" {
			if strings.ToUpper(strings.TrimSpace(shell)) != "NULL" {
				nullshell.Valid = true
				nullshell.String = shell 
			} else {
				nullshell.Valid = false
				nullshell.String = ""
			}
		}

		// and the same for default_home_dir, following the same rule as shell.
		if homedir != "" {
			if strings.ToUpper(strings.TrimSpace(homedir)) != "NULL" {
				nullhomedir.Valid = true
				nullhomedir.String = homedir
			} else {
				nullhomedir.Valid = false
				nullhomedir.String = ""
			}
		}
		
		// if you specified a new affiliation unit, find the new ID and change it. Otherwise keep whatever the select returned, even if it is null
		if unitName != "" {
			if strings.ToUpper(strings.TrimSpace(unitName)) != "NULL" {
				var tmpunitid sql.NullInt64
				iderr := DBptr.QueryRow(`select unitid from affiliation_units where name=$1`,unitName).Scan(&tmpunitid)
				// FIX THIS
				if iderr != nil && iderr != sql.ErrNoRows {
					//some error selecting the new unit ID. Keep the old one!
				} else {
					unitID = tmpunitid
				}
			} else {
				//ah, so the "new" unitName is some variation of NULL, so that means you want to set unitid to null in the DB. Do that by setting unitID.Valid to false
				unitID.Valid = false
			}
		} // end if unitName != ""
		
		//transaction start, and update command
		cKey, err := DBtx.Start(DBptr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Error starting DB transaction: " + err.Error())
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w,"{ \"ferry_error\": \"Error starting database transaction.\" }")
			return
		}
		
		_, commerr := DBtx.Exec(`update compute_resources set default_shell=$1, unitid=$2, last_updated=NOW(), default_home_dir=$3, type=$4 where name=$5`, nullshell, unitID, nullhomedir, currentType, rName)
		if commerr != nil {
			w.WriteHeader(http.StatusNotFound)
			log.WithFields(QueryFields(r, startTime)).Error("Error during DB update " + commerr.Error())
			fmt.Fprintf(w,"{ \"ferry_error\": \"Database error during update.\" }")
			return
		} else {
			// if no error, commit and all that
			DBtx.Commit(cKey)
			log.WithFields(QueryFields(r, startTime)).Info("Successfully updated " + unitName + ".")
			fmt.Fprintf(w,"{ \"ferry_status\": \"success.\" }")
		}
	} //end switch
}

func createStorageResource(w http.ResponseWriter, r *http.Request) {
	
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	
	rName := strings.TrimSpace(q.Get("resourcename"))
	defunit := strings.TrimSpace(strings.ToUpper(q.Get("default_unit")))
	rType := strings.TrimSpace(strings.ToLower(q.Get("type")))
	
	defpath := strings.TrimSpace(q.Get("default_path"))
	defquota := strings.TrimSpace(q.Get("default_quota"))

	if rName == "" {
		log.WithFields(QueryFields(r, startTime)).Print("No resource name specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No resourcename specified.\" }")
		return
	}
	if rType == "" {
		log.WithFields(QueryFields(r, startTime)).Print("No resource type specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No type specified.\" }")
		return	
	} else if strings.ToUpper(strings.TrimSpace(rType)) == "NULL" {
		log.WithFields(QueryFields(r, startTime)).Print("'NULL' is an invalid resource type.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"Resource type of NULL is not allowed.\" }")
		return	
	}
	var(
		nullpath,nullunit sql.NullString
		nullquota sql.NullInt64
	)
	if defpath != "" && strings.ToUpper(defpath) != "NULL" {
		nullpath.Valid = true
		nullpath.String = defpath
	}
	if defquota != "" && strings.ToUpper(defquota) != "NULL" {
		nullquota.Valid = true
		convquota,converr := strconv.ParseInt(defquota,10,64)
		if converr != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Error converting default_quota to int: " + converr.Error())
			fmt.Fprintf(w,"{ \"ferry_error\": \"Error converting default_quota to int. Check format.\" }")
			return
		}
		nullquota.Int64 = convquota
	}
	if defpath != "" && strings.ToUpper(defpath) != "NULL" {
		nullpath.Valid = true
		nullpath.String = defpath
	}
	if defunit != "" && strings.ToUpper(defunit) != "NULL" {
		nullunit.Valid = true
		nullunit.String = defunit
	}

	// CHECK IF UNIT already exists; add if not
	var storageid int
	checkerr := DBptr.QueryRow(`select storageid from storage_resources where name=$1`,rName).Scan(&storageid)
	switch {
	case checkerr == sql.ErrNoRows:
			// OK, it does not already exist, so we start a transaction
		cKey, err := DBtx.Start(DBptr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Error starting DB transaction: " + err.Error())
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w,"{ \"ferry_error\": \"Error starting database transaction.\" }")
			return
		}
		_, inserterr := DBtx.tx.Exec(`insert into storage_resources (name, default_path, default_quota, last_updated, default_unit, type) values ($1,$2,$3,NOW(),$4,$5)`, rName, nullpath, nullquota, nullunit, rType)
		
		if inserterr != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Error in DB insertionn: " + inserterr.Error())
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w,"{ \"ferry_error\": \"Error in database transaction.\" }")
			//	DBtx.Rollback()
			return
		} else {
			DBtx.Commit(cKey)
			log.WithFields(QueryFields(r, startTime)).Error("Added " + rName + " to compute_resources.")
			fmt.Fprintf(w,"{ \"result\": \"success.\" }")
			return		
		}		
	case checkerr != nil:
		//some other error, exit with status 500
		log.WithFields(QueryFields(r, startTime)).Error("Error in DB query: " + checkerr.Error())
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error in database check.\" }")
		return
	default:
		// if we get here, it means that the unit already exists. Bail out.
		log.WithFields(QueryFields(r, startTime)).Error("Resource " + rName + " already exists.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"Resource already exists.\" }")
		return	
	}
}

func setStorageResourceInfo(w http.ResponseWriter, r *http.Request) {
	
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	
	rName := strings.TrimSpace(q.Get("resourcename"))
	defunit := strings.TrimSpace(strings.ToUpper(q.Get("default_unit")))
	rType := strings.TrimSpace(strings.ToLower(q.Get("type")))
	
	defpath := strings.TrimSpace(q.Get("default_path"))
	defquota := strings.TrimSpace(q.Get("default_quota"))
	
	if rName == "" {
		log.WithFields(QueryFields(r, startTime)).Print("No resource name specified in http query.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"No resourcename specified.\" }")
		return
	}
	if rType == "null" {
		log.WithFields(QueryFields(r, startTime)).Print("'null' is an invalid resource type.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"Resource type of null is not allowed.\" }")
		return	
	}
	
	//require auth	
	authorized,authout := authorize(r,AuthorizedDNs)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w,"{ \"ferry_error\": \"" + authout + "not authorized.\" }")
		return
	}
	
	var ( 
		nullpath,nullunit sql.NullString
		nullquota sql.NullInt64
		currentType string
		storageid  int
	)
	// check if resource exists and grab existing values of everything if so
	err := DBptr.QueryRow(`select distinct storageid, default_path, default_quota, default_unit, type from storage_resources where name=$1`,rName).Scan(&storageid,&nullpath,&nullquota,&nullunit,&currentType)
	
	switch {
	case err == sql.ErrNoRows:
		// nothing returned from the select, so the resource does not exist.
		log.WithFields(QueryFields(r, startTime)).Print("Storage resource with name " + rName + " not found in compute_resources table. Exiting.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"resource does not exist. Use createStorageResource to add a new resource.\" }")
		return	
	case err != nil:
		w.WriteHeader(http.StatusNotFound)
		log.WithFields(QueryFields(r, startTime)).Print("Error in DB query: " + err.Error())
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error in DB query.\" }")
		return	
	default:
		
		//actually change stuff
		// if you specfied a new type in the API call (not NULL, as checked for earlier), change it here. Otherwise we keep the existing one
		if rType != "" { 
			currentType = rType
		}
		// if you are changing the default path, do it here. Variations of "NULL" as the string will assume you want it to be null in the database. If you did not specify shell in the query, then we keep the existing value.
		if defpath != "" {
			if strings.ToUpper(defpath) != "NULL" {
				nullpath.Valid = true
				nullpath.String = defpath
			} else {
				nullpath.Valid = false
				nullpath.String = ""
			}
		}

		// and the same for default_home_dir, following the same rule as path.
		if defquota != "" {
			if strings.ToUpper(defquota) != "NULL" {
				nullquota.Valid = true
				convquota,converr := strconv.ParseInt(defquota,10,64)
				if converr != nil {
					log.WithFields(QueryFields(r, startTime)).Error("Error converting default_quota to int: " + converr.Error())
					fmt.Fprintf(w,"{ \"ferry_error\": \"Error converting default_quota to int. Check format.\" }")
					return
				}
				nullquota.Int64 = convquota
			} else {
				nullquota.Valid = false
			}
		}
		
		// if you specified a new default unit, change it, following the same rule as path.
		if defunit != "" {
			if strings.ToUpper(defunit) != "NULL" {
				nullunit.Valid = true
				nullunit.String = defunit
			} else {
				nullunit.Valid = false
				nullunit.String = ""
			}
		} // end if unitName != ""
		
		//transaction start, and update command
		cKey, err := DBtx.Start(DBptr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Error starting DB transaction: " + err.Error())
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w,"{ \"ferry_error\": \"Error starting database transaction.\" }")
			return
		}
		
		_, commerr := DBtx.Exec(`update storage_resources set default_path=$1, default_quota=$2, last_updated=NOW(), default_unit=$3, type=$4 where name=$5`, nullpath, nullquota, nullunit, currentType, rName)
		if commerr != nil {
			w.WriteHeader(http.StatusNotFound)
			log.WithFields(QueryFields(r, startTime)).Error("Error during DB update: " + commerr.Error())
			fmt.Fprintf(w,"{ \"ferry_error\": \"Database error during update.\" }")
			return
		} else {
			// if no error, commit and all that
			DBtx.Commit(cKey)
			log.WithFields(QueryFields(r, startTime)).Info("Successfully updated " + rName + ".")
			fmt.Fprintf(w,"{ \"ferry_status\": \"success.\" }")
		}
	} //end switch

}

func getAllCAs(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	
	rows, err := DBptr.Query(`select distinct issuer_ca from user_certificates;`)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		log.WithFields(QueryFields(r, startTime)).Error("Error in DB query: " + err.Error())
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error in DB query.\" }")
		return
	}
	defer rows.Close()
	
	var tmpCA sql.NullString
	var Out []string
	
	for rows.Next() {
		rows.Scan(&tmpCA)
		Out = append(Out, tmpCA.String)
	}
	
	var output interface{}	
	if len(Out) == 0 {
		type jsonerror struct {
			Error string `json:"ferry_error"`
		}
		var queryErr []jsonerror
		queryErr = append(queryErr, jsonerror{"Query returned no users."})
		log.WithFields(QueryFields(r, startTime)).Error("Query returned no users.")
		output = queryErr
	} else {
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		output = Out
	}
	jsonoutput, err := json.Marshal(output)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err.Error())
	}
	fmt.Fprintf(w, string(jsonoutput))
}
