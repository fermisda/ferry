package main

import (
	"database/sql"
	"time"
	"strings"
	"strconv"
	"errors"
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

func convertValue(value int64, inunit string, outunit string) (int64, error) {

	// given a value and input unit, return a value units specified by outunit
	
	var infactor, outfactor int64
	infactor = 1
	outfactor = 1
	var myerror error
	myerror = nil

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
		infactor = 1048576
	case"GB":
		infactor = 1000000000
	case "GIB":
		infactor = 1073741824
	case "TB":
		infactor = 1000000000000
	case "TIB":
		infactor = 1099511627776
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
		outfactor = 1048576
	case"GB":
		outfactor = 1000000000
	case "GIB":
		outfactor = 1073741824
	case "TB":
		outfactor = 1000000000000
	case "TIB":
		outfactor = 1099511627776
	default:
		myerror = errors.New("Invalid value for unit. valid values are (case-insensivite) b, kb, kib, mb, mib, gb, gib, tb, tib.")
		return 0, myerror
	}
	outval := int64(float64(value * infactor)/float64(outfactor))
	return outval, nil
}

