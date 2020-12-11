package main

import (
	"fmt"
	"regexp"
	"database/sql"
	"time"
	"strings"
	"strconv"
	"errors"
	log "github.com/sirupsen/logrus"
)

func stringToParsedTime( intime string ) (sql.NullString, error) {
// convert a string representing a epoch time (so it should possible to parse as an integer) to a string in RFC3339 format. 
// This is useful for things like gettign DB entries updated only after a certain time. The returned time will always
// be set to UTC. Returns a sql.NullString and an error

	var updatetime sql.NullString

//if the input string is empty, just return the default sql.NullString (false and empty string) and a nil error.
	if intime == "" {
		return updatetime, nil
	}
// if not empty, try to parse ine input string as an integer. Bail out if it fails
	if unixtime, interr := strconv.ParseInt(intime,10,64) ; interr == nil {
		parsedtime, marshalerr := ((time.Unix(unixtime,0)).UTC()).MarshalText()
		if marshalerr == nil {
			updatetime.Valid = true
			updatetime.String = string(parsedtime)
			return updatetime, nil
		} else {
			return updatetime, marshalerr
		}		
	} else {
		return updatetime, interr
	}
}

func convertValue(value interface{}, inunit string, outunit string) (float64, error) {

	// given a value and input unit, return a value units specified by outunit

	var outval, infactor, outfactor float64
	var myerror error
	myerror = nil
	switch value.(type) {
	case float64:
		outval = value.(float64)
	case int64:
		outval = float64(value.(int64))
	case string:
		outval, myerror = strconv.ParseFloat(value.(string), 64)
		if myerror != nil {
			return outval, myerror
		}
	}

	switch (strings.ToUpper(inunit)) {
		
	case "B":
		infactor = 1
	case "KB":
		infactor = 1000
	case "KIB":
		infactor = 1024
	case "MB":
		infactor = 1000000
	case "MIB":
		infactor = 1048576 //1024^2
	case"GB":
		infactor = 1000000000
	case "GIB":
		infactor = 1073741824 //1024^3
	case "TB":
		infactor = 1000000000000
	case "TIB":
		infactor = 1099511627776 //1024^4
	default:
		myerror = errors.New("Invalid value for unit. valid values are (case-insensivite) b, kb, kib, mb, mib, gb, gib, tb, tib.")
		return 0, myerror
	}
	
	switch (strings.ToUpper(outunit)) {
		
	case "B":
		outfactor = 1
	case "KB":
		outfactor = 1000
	case "KIB":
		outfactor = 1024
	case "MB":
		outfactor = 1000000
	case "MIB":
		outfactor = 1048576 //1024^2
	case"GB":
		outfactor = 1000000000
	case "GIB":
		outfactor = 1073741824 // 1024^3
	case "TB":
		outfactor = 1000000000000
	case "TIB":
		outfactor = 1099511627776 //1024^4
	default:
		myerror = errors.New("Invalid value for unit. valid values are (case-insensivite) b, kb, kib, mb, mib, gb, gib, tb, tib.")
		return 0, myerror
	}
	outval = outval * infactor / outfactor
	return outval, nil
}

func checkUnits(inunit string) bool {
// check whether a storage unit is one of the acceptable units
// (B, MB, MiB, GB, GiB, TB, TiB) and return a bool.
// Convert everything to upper case.

	valid_units := [9]string{"B", "KB", "KIB", "MB", "MIB", "GB", "GIB", "TB", "TIB"}
	
	for x := range valid_units {
		if strings.ToUpper(inunit) == valid_units[x] {
			return true
		}
	}
	return false
}

// ExtractDN extracts a DN from string.
func ExtractDN(dn string) (string, error) {
	re := regexp.MustCompile(`\/[^\/]+\=.*[^\s]`)
	parsedDN := re.FindString(dn)

	var err error
	if parsedDN == "" {
		err = errors.New("malformed dn")
	}

	return parsedDN, err
}
// FExtractDN extracts a DN from string and validates it against a CA.
func ExtractValidDN(dn string) (string, error) {
	formatedDN, err := ExtractDN(dn)
	if err != nil {
		return formatedDN, err
	}
	ca, err := ValidCAs.MatchCA(formatedDN)
	if err != nil {
		return formatedDN, err
	}
	log.Debug(fmt.Printf("Matched DN \"%s\" to CA \"%s\"\n", formatedDN, ca["subjectdn"]))
	return formatedDN, nil
}
