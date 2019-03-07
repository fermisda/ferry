package main

import (
	"strconv"
	"database/sql/driver"
	"errors"
	"time"
	"encoding/json"
	"fmt"
	"net/http"
	log "github.com/sirupsen/logrus"
)

// BaseAPI is a basic type to build APIs
type BaseAPI struct {
	InputModel InputModel
	QueryFunction func(APIContext, Input) (interface{}, error)
}

// Run the API
func (b BaseAPI) Run(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	var context APIContext
	context.StartTime = time.Now()
	context.W = w
	context.R = r

	var output Output
	defer output.Parse(context)

	authorized, authout := authorize(r)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		output.Err = append(output.Err, fmt.Errorf("%s not authorized", authout))
		return
	}

	DBtx, ckey, err := LoadTransaction(r, DBptr)
	if err != nil {
		err := errors.New("error starting database transaction")
		output.Err = append(output.Err, err)
		log.WithFields(QueryFields(r, context.StartTime)).Error(err)
		return
	}
	context.DBtx = DBtx
	context.Ckey = ckey

	parsedInput, parseErr := b.InputModel.Parse(context)
	if parseErr != nil {
		output.Err = parseErr
		return
	}

	out, queryErr := b.QueryFunction(context, parsedInput)
	if queryErr != nil {
		output.Err = append(output.Err, queryErr)
		context.DBtx.Rollback(context.Ckey)
		log.WithFields(QueryFields(r, context.StartTime)).Error(queryErr)
		return
	}

	output.Status = true
	output.Out = out
}

// InputModel describes all parameters used by an API
type InputModel []Parameter

// Add a parameter to the InputModel object
func (i *InputModel) Add(attribute Attribute, optional bool) {
	*i = append(*i, Parameter{attribute, optional})
}

// Parse an http.Request and returns a ParsedInput
func (i InputModel) Parse(c APIContext) (Input, []error) {
	var errs []error
	input := make(Input)
	q := c.R.URL.Query()
	
	for _, p := range i {
		var parsedValue interface{}
		value := q.Get(string(p.Attribute))
		q.Del(string(p.Attribute))

		errString := "required parameter '%s' not provided"
		if value == "" && p.Required {
			errs = append(errs, fmt.Errorf(errString, p.Attribute))
			log.WithFields(QueryFields(c.R, c.StartTime)).Error(errs[len(errs) - 1])
			continue
		}

		errString = "parameter '%s' requires a %s value"
		if value != "" {
			if v, ok := p.Attribute.Type().ParseString(value); ok {
				parsedValue = v
			} else {
				errs = append(errs, fmt.Errorf(errString, p.Attribute, p.Attribute.Type()))
				log.WithFields(QueryFields(c.R, c.StartTime)).Error(errs[len(errs) - 1])
				continue
			}
		}

		input.Add(p.Attribute, parsedValue)
	}

	for p := range q {
		errs = append(errs, fmt.Errorf("'%s' is not a valid parameter for this api", p))
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(errs[len(errs) - 1])
	}

	return input, errs
}

// Input is a dictionary of parsed parameters for an API
type Input map[Attribute]interface{}

// Add a parsed parameter to Input
func (i Input) Add(attribute Attribute, value interface{}) {
	i[attribute] = value
}

// Output is the default structure for APIs to return information
type Output struct {
	Status bool
	Err []error
	Out interface{}
}

// Parse the Output and writes to an http.ResponseWriter
func (o *Output) Parse(c APIContext) () {
	type jsonOutput struct {
		Status string	`json:"ferry_status"`
		Err []string	`json:"ferry_error"`
		Out interface{}	`json:"ferry_output"`
	}

	var out jsonOutput

	if o.Status {
		out.Status = "success"
	} else {
		out.Status = "failure"
	}

	for _, err := range o.Err {
		out.Err = append (out.Err, err.Error())
	}

	out.Out = o.Out

	parsedOut, err := json.Marshal(out)
	if err != nil {
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(err.Error())
	}
	fmt.Fprintf(c.W, string(parsedOut))
}

// Parameter for basic APIs
type Parameter struct {
	Attribute Attribute
	Required bool
}

// Attribute represents a valid parameter names to be used by an API
type Attribute string

// List of valid Attribute names
const (
	UserName 		Attribute = "username"
	GroupName		Attribute = "groupname"
	UnitName		Attribute = "unitname"
	FullName 		Attribute = "fullname"
	UID				Attribute = "uid"
	GID				Attribute = "gid"
	Status			Attribute = "status"
	GroupAccount  	Attribute = "groupaccount"
	ExpirationDate	Attribute = "expirationdate"
)

// Type returns the type of the Attribute
func (a Attribute) Type() (AttributeType) {
	AttributeType := map[Attribute]AttributeType{
		UserName:		TypeString,
		GroupName:		TypeString,
		UnitName: 		TypeString,
		FullName:		TypeString,
		UID:			TypeInt,
		GID:			TypeInt,
		Status:			TypeBool,
		GroupAccount:	TypeBool,
		ExpirationDate:	TypeDate,
	}

	return AttributeType[a]
}

// AttributeType represents a valid parameter types to be used by an API
type AttributeType string

// List of valid parameter types
const (
	TypeInt 	AttributeType = "integer"
	TypeUint	AttributeType = "unsigned integer"
	TypeFloat	AttributeType = "float"
	TypeBool	AttributeType = "boolean"
	TypeString	AttributeType = "string"
	TypeDate	AttributeType = "date"
)

// DateFormat represents the default date format
const DateFormat = "2006-01-02"

// Parse parses an interface into the AttributeType primitive type
func (at AttributeType) Parse(value interface{}) (interface{}, bool) {
	var parsedValue interface{}
	var valid bool

	switch at {
	case TypeString:
		parsedValue, valid = value.(string)
	case TypeInt:
		parsedValue, valid = value.(int64)
	case TypeUint:
		parsedValue, valid = value.(uint)
	case TypeFloat:
		parsedValue, valid = value.(float64)
	case TypeBool:
		parsedValue, valid = value.(bool)
	case TypeDate:
		parsedValue, valid = value.(time.Time)
	}
	
	return parsedValue, valid
}

// ParseString parses a string into the AttributeType primitive type
func (at AttributeType) ParseString(value string) (interface{}, bool) {
	var parsedValue interface{}
	var valid bool
	var err error

	switch at {
	case TypeString:
		parsedValue, valid = value, true
	case TypeInt:
		parsedValue, err = strconv.ParseInt(value, 10, 64)
		valid = (err == nil)
	case TypeUint:
		parsedValue, err = strconv.ParseUint(value, 10, 64)
		valid = (err == nil)
	case TypeFloat:
		parsedValue, err = strconv.ParseFloat(value, 64)
		valid = (err == nil)
	case TypeBool:
		parsedValue, err = strconv.ParseBool(value)
		valid = (err == nil)
	case TypeDate:
		parsedValue, err = time.Parse(DateFormat, value)
		valid = (err == nil)
	}
	
	return parsedValue, valid
}

// NullAttribute represents a BaseAPI attribute that may be null. NullAttribute implements the
// sql.Scanner interface so it can be used as a scan destination, similar to
// sql.NullString.
type NullAttribute struct {
	Attribute Attribute
	Data interface{}
	Valid bool // Valid is true if Value matches Attribute.Type
}

// Scan implements the Scanner interface.
func (na *NullAttribute) Scan(value interface{}) error {
	if stringValue, ok := value.(string); ok {
		na.Data, na.Valid = na.Attribute.Type().ParseString(stringValue)
	} else {
		na.Data, na.Valid = na.Attribute.Type().Parse(value)
	}
	return nil
}

// Value implements the driver Valuer interface.
func (na NullAttribute) Value() (driver.Value, error) {
	if !na.Valid {
		return nil, nil
	}
	return na.Value, nil
}

// APIContext stores metadata used through the API execution
type APIContext struct {
	W http.ResponseWriter
	R *http.Request
	StartTime time.Time
	DBtx *Transaction
	Ckey int64
}

// APICollection aggregates a collection of APIs to be called from a function
type APICollection map[string]*BaseAPI

// Add a BaseAPI to the collection
func (c APICollection) Add(name string, api *BaseAPI) {
	c[name] = api
}

// MapNullAttribute builds a map of Attribute to NullAttribute
func MapNullAttribute(attributes ...Attribute) map[Attribute]*NullAttribute {
	mapNullAttribute := make(map[Attribute]*NullAttribute)

	for _, attribute := range attributes {
		mapNullAttribute[attribute] = &NullAttribute{attribute, nil, false}
	}

	return mapNullAttribute
}