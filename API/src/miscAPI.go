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

// build parameters
var (
        release_ver  = "unknown"
        build_date = "unknown"
)

func NotDoneYet(w http.ResponseWriter, r *http.Request, t time.Time) {
	fmt.Fprintf(w, `{"ferry_error": "This function is not done yet!"}`)
	log.WithFields(QueryFields(r, t)).Error("This function is not done yet!")
}

func getPasswdFile(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	
	unit := strings.TrimSpace(q.Get("unitname"))
	comp := strings.TrimSpace(q.Get("resourcename"))
	
	lastupdate, parserr :=  stringToParsedTime(strings.TrimSpace(q.Get("last_updated")))
	if parserr != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error parsing provided update time: " + parserr.Error())
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error parsing last_updated time. Check ferry logs. If provided, it should be an integer representing an epoch time.\"}")
		return
	}

	rows, err := DBptr.Query(`select aname, rname, uname, uid, gid, full_name, home_dir, shell, unit_exists, comp_exists, last_updated from (
                                                              select 1 as key, au.name as aname, u.uname, u.uid, g.gid, u.full_name, ca.home_dir, ca.shell, cr.name as rname, cag.last_updated as last_updated
                                                              from compute_access_group as cag 
                                                              left join compute_access as ca using (compid, uid) 
                                                              join groups as g on g.groupid=cag.groupid 
                                                              join compute_resources as cr on cr.compid=cag.compid 
                                                              left join affiliation_units as au on au.unitid=cr.unitid 
                                                              join users as u on u.uid=cag.uid
                                                              where  cag.is_primary = true and (au.name = $1 or $3) and (cr.name = $2 or $4) and (ca.last_updated>=$5 or u.last_updated>=$5 or au.last_updated>=$5 or cr.last_updated>=$5 or g.last_updated>=$5 or $5 is null) order by au.name, cr.name
							) as t
								right join (select 1 as key,
								$1 in (select name from affiliation_units) as unit_exists,
								$2 in (select name from compute_resources) as comp_exists
							) as c on t.key = c.key;`, unit, comp, unit == "", comp == "", lastupdate)



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
		log.WithFields(QueryFields(r, startTime)).Debugln(tmpAname.String  + " " + tmpRname.String + " " + tmpUname.String)
		
		if ! tmpAname.Valid {
			tmpAname.Valid = true
			tmpAname.String = "null"
		}		
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
				if tmpAname.Valid { 
					prevAname = tmpAname.String
				} else {
					prevAname = "null"
				}
				
			}
			if tmpTime.Valid {
				log.WithFields(QueryFields(r, startTime)).Debugln("tmpTime is valid" + tmpTime.String)
				
				parseTime,parserr := time.Parse(time.RFC3339,tmpTime.String)
				if parserr != nil {
					log.WithFields(QueryFields(r, startTime)).Error("Error parsing last updated time of " + tmpTime.String)
				} else {
					if lasttime == 0  || ( parseTime.Unix() > lasttime) {
						lasttime = parseTime.Unix()
					}
				}
			} else {
				log.WithFields(QueryFields(r, startTime)).Debugln("tmpTime is not valid")
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
		if !unitExists && unit != "" {
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
	
	unit := strings.TrimSpace(q.Get("unitname"))
	comp := strings.TrimSpace(q.Get("resourcename"))

	if comp == "" {
		comp = "%"
	}
	lastupdate, parserr := stringToParsedTime(strings.TrimSpace(q.Get("last_updated")))
	if parserr != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error parsing provided update time: " + parserr.Error())
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error parsing last_updated time. Check ferry logs. If provided, it should be an integer representing an epoch time.\"}")
		return
	}

//	rows, err := DBptr.Query(`select gname, gid, uname, unit_exists, comp_exists, last_updated, is_primary from (
//								select 1 as key, g.name as gname, g.gid as gid, u.uname as uname, cag.last_updated, cag.is_primary
//								from affiliation_unit_group as aug
//								join affiliation_units as au using (unitid)
//								join groups as g using (groupid)
//								left join compute_resources as cr using (unitid)
//                                                                left join compute_access_group as cag using (groupid,compid)
//								left join users as u using (uid)
//								where (au.name = $1 or $4) and g.type = 'UnixGroup' and (cr.name like $2) and (g.last_updated>=$3 or u.last_updated>=$3 or cag.last_updated>=$3 or au.last_updated>=$3 or $3 is null)
//                                                                order by g.name,u.uname
//							) as t
//								right join (select 1 as key,
//								$1 in (select name from affiliation_units) as unit_exists,
//                                                   		$2 in (select name from compute_resources) as comp_exists
// 							) as c on t.key = c.key;`, unit, comp, lastupdate, unit=="")
//
	rows, err := DBptr.Query(`select gname, gid, uname, unit_exists, comp_exists, last_updated, is_primary from (
								select 1 as key, g.name as gname, g.gid as gid, u.uname as uname, cag.last_updated, cag.is_primary
                                        			from compute_access_group cag
								join compute_resources as cr using (compid)
								left join affiliation_unit_group aug using (groupid)
								left join affiliation_units as au on au.unitid=cr.unitid
								join groups as g on cag.groupid=g.groupid		
								join users as u on cag.uid=u.uid
								where (au.name = $1 or $4) and g.type = 'UnixGroup' and (cr.name like $2) and (g.last_updated>=$3 or u.last_updated>=$3 or cag.last_updated>=$3 or au.last_updated>=$3 or $3 is null)
                                                                order by g.name,u.uname
							) as t
								right join (select 1 as key,
								$1 in (select name from affiliation_units) as unit_exists,
                                                   		$2 in (select name from compute_resources) as comp_exists
 							) as c on t.key = c.key;`, unit, comp, lastupdate, unit=="")



	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Error(err)
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error in DB query.\" }")
		return
	}
	defer rows.Close()

	var unitExists bool
	var compExists bool

	type jsonentry struct {
		Gname string `json:"groupname"`
		Gid int64 `json:"gid"`
		Lasttime int64 `json:"last_updated"`
		Unames []string `json:"unames"`
	}
	var Entry jsonentry
	var Out []jsonentry
	var tmpGname, tmpUname, tmpTime sql.NullString
	var tmpGid sql.NullInt64
	var tmpPrimary sql.NullBool
	prevGname := ""
	prevUname := ""
	for rows.Next() {

		rows.Scan(&tmpGname, &tmpGid, &tmpUname, &unitExists, &compExists, &tmpTime, &tmpPrimary)
		if tmpGname.Valid {
			if prevGname == "" {
				Entry.Gname = tmpGname.String
				Entry.Gid = tmpGid.Int64
				if tmpPrimary.Valid && tmpPrimary.Bool == false && tmpUname.Valid && tmpUname.String != prevUname {
					Entry.Unames = append(Entry.Unames, tmpUname.String)
					prevUname = tmpUname.String
				}
			} else if prevGname != tmpGname.String {
				Out = append(Out, Entry)
				Entry.Gname = tmpGname.String
				Entry.Gid = tmpGid.Int64
				Entry.Unames = nil
				prevUname = ""
				if tmpPrimary.Valid && tmpPrimary.Bool == false && tmpUname.Valid && tmpUname.String != prevUname {
					Entry.Unames = append(Entry.Unames, tmpUname.String)
					prevUname = tmpUname.String
				}
			} else {
				if tmpPrimary.Valid && tmpPrimary.Bool == false && tmpUname.Valid && tmpUname.String != prevUname {
					Entry.Unames = append(Entry.Unames, tmpUname.String)
					prevUname = tmpUname.String
				}
			}
			prevGname = tmpGname.String
			if tmpTime.Valid {
				log.WithFields(QueryFields(r, startTime)).Debugln("tmpTime is valid" + tmpTime.String)
				
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
				log.WithFields(QueryFields(r, startTime)).Debugln("tmpTime is not valid")
			}
		}
	}
	Out = append(Out, Entry)
	log.WithFields(QueryFields(r, startTime)).Debugln("Length: " + fmt.Sprintf("%d",len(Out)))	
	var output interface{}
	if prevGname == "" {
		type jsonerror struct {Error string `json:"ferry_error"`}
		var Err []jsonerror
		if !unitExists && unit != "" {
			Err = append(Err, jsonerror{"Affiliation unit does not exist."})
			log.WithFields(QueryFields(r, startTime)).Error("Affiliation unit does not exist.")
		}
		if !compExists && comp != "%" {
			Err = append(Err, jsonerror{"Resource does not exist."})
			log.WithFields(QueryFields(r, startTime)).Error("Resource does not exist.")
		}
		if len(Out) == 1 && Out[0].Gname == "" {
			Err = append(Err, jsonerror{"No users or groups with the non-primary GID for this unit and resource(s). Nothing to do."})
			log.WithFields(QueryFields(r, startTime)).Error("No users with the non-primary GID for this unit and resource(s).")
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
	unit := strings.TrimSpace(q.Get("unitname"))
	if unit == "" {
		unit = "%"
	}
	rName := strings.TrimSpace(q.Get("resourcename"))

	lastupdate, parserr :=  stringToParsedTime(strings.TrimSpace(q.Get("last_updated")))
	if parserr != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error parsing provided update time: " + parserr.Error())
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error parsing last_updated time. Check ferry logs. If provided, it should be an integer representing an epoch time.\"}")
		return
	}

	var unitExists,resourceExists bool

	rows, err := DBptr.Query(`select distinct dn, uname, unit_exists, resource_exists from 
							 (select 1 as key, au.name, uc.dn, us.uname from  affiliation_unit_user_certificate as ac
								left join user_certificates as uc on ac.dnid = uc.dnid
								left join users as us on uc.uid = us.uid
								left join compute_access as ca on us.uid = ca.uid
								left join affiliation_units as au on ac.unitid = au.unitid
								left join compute_resources as cr on ca.compid = cr.compid
								where au.name like $1 and ( ac.last_updated>=$2 or uc.last_updated>=$2 or us.last_updated>=$2 or au.last_updated>=$2 or $2 is null) and (cr.name = $3 or $4)) as t
	 						  right join (select 1 as key, $1 in (select name from affiliation_units) as unit_exists, ($3 in (select name from compute_resources) or $4) as resource_exists) as c on t.key = c.key`, unit, lastupdate, rName, rName=="")
	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Error(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error in DB query.\" }")
		return
	}
	defer rows.Close()

	type jsonentry struct {
		DN string `json:"userdn"`
		Uname string `json:"mapped_uname"`
	}
	var dnmap jsonentry
	var Out[] jsonentry

	for rows.Next() {
		var tmpDN, tmpUname sql.NullString
		rows.Scan(&tmpDN, &tmpUname, &unitExists, &resourceExists)
		if tmpDN.Valid {
			dnmap.DN, dnmap.Uname = tmpDN.String, tmpUname.String
			Out = append(Out, dnmap)
		}
	}

	var output interface{}
	if len(Out) == 0 {
		type jsonerror struct {Error []string `json:"ferry_error"`}
		var Err jsonerror

		if !unitExists && unit != "%" {
			Err.Error = append(Err.Error, "Experiment does not exist.")
			log.WithFields(QueryFields(r, startTime)).Error("Experiment does not exist.")
		} else if !resourceExists && rName != "" {
			Err.Error = append(Err.Error, "Resource " + rName + " does not exist.")
			log.WithFields(QueryFields(r, startTime)).Error("Resource " +  rName + " does not exist.")
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

func getGridMapFileByVO(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	unit := strings.TrimSpace(q.Get("unitname"))
	if unit == "" {
		unit = "%"
	}
	
	lastupdate, parserr :=  stringToParsedTime(strings.TrimSpace(q.Get("last_updated")))
	if parserr != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error parsing provided update time: " + parserr.Error())
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error parsing last_updated time. Check ferry logs. If provided, it should be an integer representing an epoch time.\"}")
		return
	}

	var unitExists bool

	rows, err := DBptr.Query(`select name, dn, uname, unit_exists from 
							 (select 1 as key, au.name, uc.dn, us.uname from  affiliation_unit_user_certificate as ac
								left join user_certificates as uc on ac.dnid = uc.dnid
								left join users as us on uc.uid = us.uid
								left join affiliation_units as au on ac.unitid = au.unitid
								where au.name like $1 and ( ac.last_updated>=$2 or uc.last_updated>=$2 or us.last_updated>=$2 or au.last_updated>=$2 or $2 is null)) as t
	 						  right join (select 1 as key, $1 in (select name from affiliation_units) as unit_exists) as c on t.key = c.key`, unit, lastupdate)
	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Error(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error in DB query.\" }")
		return
	}
	defer rows.Close()

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

		if !unitExists && unit != "%" {
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

	type jsonerror struct {Error []string `json:"ferry_error"`}
	var inputErr jsonerror
	
	rName := strings.TrimSpace(q.Get("resourcename"))

	lastupdate, parserr :=  stringToParsedTime(strings.TrimSpace(q.Get("last_updated")))
	if parserr != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error parsing provided update time: " + parserr.Error())
		inputErr.Error = append(inputErr.Error, "Error parsing last_updated time. Check ferry logs. If provided, it should be an integer representing an epoch time.")
		return
	}

	if len(inputErr.Error) > 0 {
		jsonout, err := json.Marshal(inputErr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error(err)
		}
		fmt.Fprintf(w, string(jsonout))
		return
	}

	rows, err := DBptr.Query(`select distinct t.fqan, t.uname, t.name, c.resource_exists from
							  (select 1 as key, fqan, uname, au.name from grid_fqan as gf
							   join users as u on gf.mapped_user = u.uid
							   join compute_access_group as cag on (cag.groupid=gf.mapped_group and gf.mapped_user=cag.uid)
							   join compute_resources as cr on cag.compid=cr.compid
							   left join affiliation_units as au on gf.unitid = au.unitid
							   where ($1 or cr.name=$2) and (gf.last_updated >= $3 or u.last_updated >= $3 or $3 is null)
							  ) as t
							  right join (
							   select 1 as key, $2 in (select name from compute_resources) as resource_exists
							  ) as c on t.key = c.key order by t.fqan`, rName == "", rName, lastupdate)
	if err != nil {
		defer log.WithFields(QueryFields(r, startTime)).Error(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error in DB query.\" }")
		return
	}
	defer rows.Close()

	var resourceExists bool

	type jsonentry struct {
		DN string `json:"fqan"`
		Uname string `json:"mapped_uname"`
		Aname string `json:"unitname"`
	}
	var Out []jsonentry

	for rows.Next() {
		var tmpFQAN, tmpUname, tmpAname sql.NullString
		rows.Scan(&tmpFQAN, &tmpUname, &tmpAname, &resourceExists)
		if tmpFQAN.Valid {
			Out = append(Out, jsonentry{tmpFQAN.String, tmpUname.String, tmpAname.String})
		}
	}

	var output interface{}
	if len(Out) == 0 || (!resourceExists && rName != "" ) {
		var queryErr jsonerror
		if !resourceExists && rName != "" {
			queryErr.Error = append(queryErr.Error, "Resource does not exist.")
			log.WithFields(QueryFields(r, startTime)).Error("Resource does not exist.")
		} else {
			queryErr.Error = append(queryErr.Error, "No FQANs found.")
			log.WithFields(QueryFields(r, startTime)).Error("No FQANs found.")
		}
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
	
	rows, err := DBptr.Query(`select groupid, gid from groups where name=$1 and type = 'UnixGroup'`, gName)
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

	certdn := strings.TrimSpace(q.Get("certificatedn"))

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
	
	rows, err := DBptr.Query(`select fqan, uname, gid from grid_fqan as gf
							  left join groups as g on g.groupid = gf.mapped_group
							  left join users as u on u.uid = gf.mapped_user;`)

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
	q := r.URL.Query()
	lastupdate, parserr :=  stringToParsedTime(strings.TrimSpace(q.Get("last_updated")))
	if parserr != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error parsing provided update time: " + parserr.Error())
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error parsing last_updated time. Check ferry logs. If provided, it should be an integer representing an epoch time.\"}")
		return
	}
	rows, err := DBptr.Query(`select u.uname, u.uid, g.gid from users as u
							  join user_group as ug on u.uid = ug.uid
							  join groups as g on ug.groupid = g.groupid
                              where g.type = 'UnixGroup' and (ug.last_updated>=$1 or $1 is null)
							  order by u.uname;`, lastupdate)

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

		if tmpGid.Valid {
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
	
	unit := q.Get("unitname")
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
								left join affiliation_units as au on gf.unitid = au.unitid
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
	shell := q.Get("defaultshell")
	homedir := q.Get("defaulthomedir")

	var nullhomedir sql.NullString
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
//		}
	if shell == "" || strings.ToUpper(strings.TrimSpace(shell)) == "NULL" {
		shell = "default"
	} 
	if homedir == "" ||  strings.ToUpper(strings.TrimSpace(homedir)) == "NULL" {
		nullhomedir.Valid=false
		log.WithFields(QueryFields(r, startTime)).Print("No defaulthomedir specified in http query.")
		fmt.Fprintf(w,"{ \"ferry_error\": \"No defaulthomedir specified.\" }")
		return
	} else {
		nullhomedir.Valid = true
		nullhomedir.String = homedir
	}

	//require auth	
	authorized,authout := authorize(r)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w,"{ \"ferry_error\": \"" + authout + "not authorized.\" }")
		return
	}
	
	var unitID sql.NullInt64
	
	DBtx, cKey, err := LoadTransaction(r, DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error starting DB transaction: " + err.Error())
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error starting database transaction.\" }")
		return
	}
	defer DBtx.Rollback(cKey)

	//figure out the unitID if we need it
	
	if unitName != "" {
		uniterr := DBtx.tx.QueryRow(`select unitid from affiliation_units where name=$1`,unitName).Scan(&unitID)
		if uniterr != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Error determining unitid for " + unitName + ": " + uniterr.Error())
			if cKey != 0 {
				fmt.Fprintf(w,"{ \"ferry_error\": \"Error determining unitID for " + unitName + ". You cannot add a unit name that does not already exist in affiliation_units.\" }") 
			}
			DBtx.Report("Error determining unitID for " + unitName + ".")
			return
		}
	}
	
	//now, make sure the the resource does not already exist. If it does, bail out. If it does not, do the insertion
	
	var compId int;	
	checkerr := DBtx.tx.QueryRow(`select compid from  compute_resources where name=$1`,rName).Scan(&compId)
	
	switch {
	case checkerr == sql.ErrNoRows:
		// OK, it does not already exist, so we start a transaction
	
		
		//	addstr := fmt.Sprintf(`do declare cmpid bigint;  begin select compid into cmpid from compute_resources order by compid desc limit 1; if exists (select name from compute_resources where name=$1) then raise 'resource already exists'; else insert into compute_resources (compid, name, default_shell, unitid, last_updated, default_home_dir, type) values (cmpid+1,$1,$2,$3,NOW(),$4,$5); end if ;  end ;`)
		var addstr string
		if shell == "default" {
			addstr = fmt.Sprintf(`insert into compute_resources (name, default_shell, unitid, last_updated, default_home_dir, type) values ($1, '/sbin/nologin', $2, NOW(), $3, $4)`)
			_, err = DBtx.Exec(addstr, rName, unitID, nullhomedir, rType)
		} else {
			addstr = fmt.Sprintf(`insert into compute_resources (name, default_shell, unitid, last_updated, default_home_dir, type) values ($1, $2, $3, NOW(), $4, $5)`)
			_, err = DBtx.Exec(addstr, rName, shell, unitID, nullhomedir, rType)
		}
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Error starting DB transaction: " + err.Error())
			w.WriteHeader(http.StatusNotFound)
			if cKey != 0 {
				fmt.Fprintf(w,"{ \"ferry_error\": \"Error in database transaction.\" }")
			}
			return
		} else {
			if cKey != 0 {
				DBtx.Commit(cKey)
			}
			log.WithFields(QueryFields(r, startTime)).Error("Added " + rName + " to compute_resources.")
			if cKey != 0 {
				fmt.Fprintf(w,"{ \"ferry_status\": \"success.\" }")
			}
			return
		}
		
	case checkerr != nil:
		//some other error, exit with status 500
		log.WithFields(QueryFields(r, startTime)).Error("Error in DB query: " + checkerr.Error())
		w.WriteHeader(http.StatusNotFound)
		if cKey != 0 {
			fmt.Fprintf(w,"{ \"ferry_error\": \"Error in database check.\" }")
		}
		return
	default:
		// if we get here, it means that the unit already exists. Bail out.
		log.WithFields(QueryFields(r, startTime)).Error("Resource " + rName + " already exists.")
		if cKey != 0 {
			fmt.Fprintf(w,"{ \"ferry_error\": \"Resource already exists.\" }")
		}
		DBtx.Report("Resource already exists.")
		return	
	}

}

func setComputeResourceInfo(w http.ResponseWriter, r *http.Request) {
	
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	
	rName := strings.TrimSpace(q.Get("resourcename"))
	unitName := strings.TrimSpace(q.Get("unitname"))
	rType := strings.TrimSpace(q.Get("type"))
	shell := strings.TrimSpace(q.Get("defaultshell"))
	homedir := strings.TrimSpace(q.Get("defaulthomedir"))

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
	authorized,authout := authorize(r)
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
	
	//transaction start, and update command
	DBtx, cKey, err := LoadTransaction(r, DBptr)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error starting DB transaction: " + err.Error())
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error starting database transaction.\" }")
		return
	}
	defer DBtx.Rollback(cKey)

	// check if resource exists and grab existing values of everything if so
	err = DBtx.tx.QueryRow(`select distinct compid, default_shell, unitid, default_home_dir, type from compute_resources where name=$1`,rName).Scan(&compid,&nullshell,&unitID,&nullhomedir,&currentType)
	switch {
	case err == sql.ErrNoRows:
		// nothing returned from the select, so the resource does not exist.
		log.WithFields(QueryFields(r, startTime)).Print("compute resource with name " + rName + " not found in compute_resources table. Exiting.")
		fmt.Fprintf(w, "{ \"ferry_error\": \"resource does not exist. Use createComputeResource to add a new resource.\" }")
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
				iderr := DBtx.tx.QueryRow(`select unitid from affiliation_units where name=$1`,unitName).Scan(&tmpunitid)
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
		
	
		_, commerr := DBtx.Exec(`update compute_resources set default_shell=$1, unitid=$2, last_updated=NOW(), default_home_dir=$3, type=$4 where name=$5`, nullshell, unitID, nullhomedir, currentType, rName)
		if commerr != nil {
			w.WriteHeader(http.StatusNotFound)
			log.WithFields(QueryFields(r, startTime)).Error("Error during DB update " + commerr.Error())
			fmt.Fprintf(w,"{ \"ferry_error\": \"Database error during update.\" }")
			return
		} else {
			// if no error, commit and all that. If this is being called as part of a wrapper, however, cKey will be 0. So only commit if cKey is non-zero
		if cKey != 0 {	DBtx.Commit(cKey) }
			log.WithFields(QueryFields(r, startTime)).Info("Successfully updated " + unitName + ".")
			fmt.Fprintf(w,"{ \"ferry_status\": \"success.\" }")
		}
	} //end switch
}

func createStorageResource(w http.ResponseWriter, r *http.Request) {
	
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()
	
	rName := strings.TrimSpace(strings.ToUpper(q.Get("resourcename")))
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
		
		if checkUnits(defunit) {
			nullunit.Valid = true
			nullunit.String = defunit
		} else {
			log.WithFields(QueryFields(r, startTime)).Error("Invalid value for default unit. Allowed values are B,KB,KIB,MB,MIB,GB,GIB,TB,TIB.)")
			fmt.Fprintf(w,"{ \"ferry_error\": \"Invalid value for default_unit.\" }")
			return	
		}
	}

	//require auth	
	authorized,authout := authorize(r)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w,"{ \"ferry_error\": \"" + authout + "not authorized.\" }")
		return
	}

	// CHECK IF UNIT already exists; add if not
	var storageid int
	checkerr := DBptr.QueryRow(`select storageid from storage_resources where name=$1`,rName).Scan(&storageid)
	switch {
	case checkerr == sql.ErrNoRows:
			// OK, it does not already exist, so we start a transaction
		DBtx, cKey, err := LoadTransaction(r, DBptr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Error starting DB transaction: " + err.Error())
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w,"{ \"ferry_error\": \"Error starting database transaction.\" }")
			return
		}
		defer DBtx.Rollback(cKey)
		_, inserterr := DBtx.tx.Exec(`insert into storage_resources (name, default_path, default_quota, last_updated, default_unit, type) values ($1,$2,$3,NOW(),$4,$5)`, rName, nullpath, nullquota, nullunit, rType)
		
		if inserterr != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Error in DB insertionn: " + inserterr.Error())
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w,"{ \"ferry_error\": \"Error in database transaction.\" }")
			//	DBtx.Rollback(cKey)
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
	authorized,authout := authorize(r)
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
				if checkUnits(defunit) {
					nullunit.Valid = true
					nullunit.String = strings.ToUpper(defunit)
				} else {
					log.WithFields(QueryFields(r, startTime)).Error("Invalid value for default unit. Allowed values are B,KB,KIB,MB,MIB,GB,GIB,TB,TIB.)")
					fmt.Fprintf(w,"{ \"ferry_error\": \"Invalid value for default_unit.\" }")
					return	
				}
			} else {
				nullunit.Valid = false
				nullunit.String = ""
			}
		} // end if unitName != ""
		
		//transaction start, and update command
		DBtx, cKey, err := LoadTransaction(r, DBptr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error("Error starting DB transaction: " + err.Error())
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w,"{ \"ferry_error\": \"Error starting database transaction.\" }")
			return
		}
		defer DBtx.Rollback(cKey)
		
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

func getAllComputeResources(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	lastupdate, parserr :=  stringToParsedTime(strings.TrimSpace(q.Get("last_updated")))
	if parserr != nil {
		log.WithFields(QueryFields(r, startTime)).Error("Error parsing provided update time: " + parserr.Error())
		fmt.Fprintf(w, "{ \"ferry_error\": \"Error parsing last_updated time. Check ferry logs. If provided, it should be an integer representing an epoch time.\"}")
		return
	}
	
	rows, err := DBptr.Query(`select cr.name, cr.default_shell, cr.default_home_dir, cr.type, au.name as affiliation_unit
							  from compute_resources as cr left join affiliation_units as au on cr.unitid = au.unitid where (cr.last_updated>=$1 or $1 is null);`, lastupdate)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		log.WithFields(QueryFields(r, startTime)).Error("Error in DB query: " + err.Error())
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error in DB query.\" }")
		return
	}
	defer rows.Close()

	type computeResource struct {
		Name string `json:"name"`
		Shell string `json:"default_shell"`
		Home string `json:"default_home_dir"`
		Type string `json:"type"`
		Unit string `json:"affiliation_unit"`
	}
	var Out []computeResource
	var tmpName, tmpShell, tmpHome, tmpType, tmpUnit sql.NullString
	
	for rows.Next() {
		rows.Scan(&tmpName, &tmpShell, &tmpHome, &tmpType, &tmpUnit)
		Out = append(Out, computeResource{tmpName.String, tmpShell.String, tmpHome.String,
										  tmpType.String, tmpUnit.String})
	}
	
	var output interface{}	
	if len(Out) == 0 {
		type jsonerror struct {
			Error string `json:"ferry_error"`
		}
		var queryErr []jsonerror
		queryErr = append(queryErr, jsonerror{"Query returned no compute resources."})
		log.WithFields(QueryFields(r, startTime)).Error("Query returned no compute resources.")
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

func ping(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	fmt.Fprintf(w,`[{ "ferry_status": "success."}, { "release_version" : "` + release_ver + `"}, {"build_date" : "` + build_date + `"}]`)
	return
}

func getVOUserMap(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	q := r.URL.Query()

	user := strings.TrimSpace(q.Get("username"))
	unit := strings.TrimSpace(q.Get("unitname"))
	fqan := strings.TrimSpace(q.Get("fqan"))

	if user == "" {
		user = "%"
	}
	if unit == "" {
		unit = "%"
	}
	if fqan == "" {
		fqan = "%"
	}

	var userExists, unitExists, fqanExists bool
	err := DBptr.QueryRow(`SELECT
							$1 IN (SELECT uname FROM users),
							$2 IN (SELECT name FROM affiliation_units),
							$3 IN (SELECT fqan FROM grid_fqan)`,
						  user, unit, fqan).Scan(&userExists, &unitExists, &fqanExists)
	if err != nil {	
		defer log.WithFields(QueryFields(r, startTime)).Error(err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error in DB query.\" }")
		return
	}

	type jsonerror struct {
		Error string `json:"ferry_error"`
	}
	var inputErr []jsonerror

	if !userExists && user != "%" {
		log.WithFields(QueryFields(r, startTime)).Error("User does not exist.")
		inputErr = append(inputErr, jsonerror{"User does not exist."})
	}
	if !unitExists && unit != "%" {
		log.WithFields(QueryFields(r, startTime)).Error("Affiliation unit does not exist.")
		inputErr = append(inputErr, jsonerror{"Affiliation unit does not exist."})
	}
	if !fqanExists && fqan != "%" {
		log.WithFields(QueryFields(r, startTime)).Error("FQAN does not exist.")
		inputErr = append(inputErr, jsonerror{"FQAN does not exist."})
	}

	if len(inputErr) > 0 {
		jsonout, err := json.Marshal(inputErr)
		if err != nil {
			log.WithFields(QueryFields(r, startTime)).Error(err)
		}
		fmt.Fprintf(w, string(jsonout))
		return
	}

	rows, err := DBptr.Query(`SELECT DISTINCT
								u1.uname AS username,
								au.name AS unitname,
								gf.fqan AS fqan,
								COALESCE(u2.uname, u1.uname) AS mapped_user
							  FROM     
								grid_access g 
								INNER JOIN users u1 ON g.uid = u1.uid 
								INNER JOIN grid_fqan gf ON gf.fqanid = g.fqanid 
								INNER JOIN affiliation_units au ON au.unitid = gf.unitid
								LEFT OUTER JOIN users u2 ON gf.mapped_user = u2.uid 
							  WHERE 
								u1.uname like $1 
								AND au.name like $2
								AND gf.fqan like $3
							  ORDER BY u1.uname`, user, unit, fqan)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		log.WithFields(QueryFields(r, startTime)).Error("Error in DB query: " + err.Error())
		fmt.Fprintf(w,"{ \"ferry_error\": \"Error in DB query.\" }")
		return
	}
	defer rows.Close()

	voUserMap := make(map[string]map[string]map[string]string)

	for rows.Next() {
		var tmpUser, tmpUnit, tmpFQAN, tmpMappedUser sql.NullString
		rows.Scan(&tmpUser, &tmpUnit, &tmpFQAN, &tmpMappedUser)

		if tmpUser.Valid {
			if _, ok := voUserMap[tmpUser.String]; !ok {
				voUserMap[tmpUser.String] = make(map[string]map[string]string)
			}
			if _, ok := voUserMap[tmpUser.String][tmpUnit.String]; !ok {
				voUserMap[tmpUser.String][tmpUnit.String] = make(map[string]string)
			}
			voUserMap[tmpUser.String][tmpUnit.String][tmpFQAN.String] = tmpMappedUser.String
		}
	}

	var output interface{}
	if len(voUserMap) == 0 {
		var queryErr []jsonerror
		log.WithFields(QueryFields(r, startTime)).Error("No mappings found.")
		queryErr = append(queryErr, jsonerror{"No mappings found."})
		output = queryErr
	} else {
		log.WithFields(QueryFields(r, startTime)).Info("Success!")
		output = voUserMap
	}
	jsonout, err := json.Marshal(output)
	if err != nil {
		log.WithFields(QueryFields(r, startTime)).Error(err)
	}
	fmt.Fprintf(w, string(jsonout))
}
