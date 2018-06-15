package main

import (
	"database/sql"
	"time"
	"strconv"
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
