package main

import (
	"strings"
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
	QueryFunction func(APIContext, Input) (interface{}, []APIError)
}

// Run the API
func (b BaseAPI) Run(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	var context APIContext
	context.StartTime = time.Now()
	context.R = r

	var output Output
	defer output.Parse(context, w)

	authorized, authout := authorize(r)
	if authorized == false {
		w.WriteHeader(http.StatusUnauthorized)
		output.Err = append(output.Err, fmt.Errorf("%s not authorized", authout))
		return
	}

	var err error
	context.DBtx, context.Ckey, err = LoadTransaction(r, DBptr)
	if err != nil {
		err := errors.New("error starting database transaction")
		output.Err = append(output.Err, err)
		log.WithFields(QueryFields(r, context.StartTime)).Error(err)
		return
	}
	defer context.DBtx.Rollback(context.Ckey)

	input := make(Input)
	parseErr := input.Parse(context, b.InputModel)
	if parseErr != nil {
		output.Err = parseErr
		return
	}

	out, queryErr := b.QueryFunction(context, input)
	if len(queryErr) > 0 {
		var errType ErrorType
		for _, err := range queryErr {
			log.WithFields(QueryFields(r, context.StartTime)).Error(err.Error)
			output.Err = append(output.Err, err.Error)
			if err.Type > errType {
				errType = err.Type
			}
		}
		
		switch {
		case errType > HTTP500:
			w.WriteHeader(http.StatusInternalServerError)
		}
		
		return
	}

	context.DBtx.Commit(context.Ckey)
	output.Status = true
	output.Out = out
}

// InputModel describes all parameters used by an API
type InputModel []Parameter

// Add a parameter to the InputModel object
func (i *InputModel) Add(attribute Attribute, optional bool) {
	*i = append(*i, Parameter{attribute, optional})
}

// Input is a dictionary of parsed parameters for an API
type Input map[Attribute]NullAttribute

// Parse an http.Request and returns a ParsedInput
func (i Input) Parse(c APIContext, m InputModel) ([]error) {
	var errs []error
	q := c.R.URL.Query()
	
	for _, p := range m {
		parsedAttribute := NewNullAttribute(p.Attribute)
		value := q.Get(string(p.Attribute))
		q.Del(string(p.Attribute))

		errString := "parameter %s requires a %s value"
		if value != "" {
			parsedAttribute.Scan(value)
			if !parsedAttribute.Valid && !parsedAttribute.AbsoluteNull {
				errs = append(errs, fmt.Errorf(errString, p.Attribute, p.Attribute.Type()))
				log.WithFields(QueryFields(c.R, c.StartTime)).Error(errs[len(errs) - 1])
				continue
			}
		}

		errString = "required parameter %s not provided"
		if !parsedAttribute.Valid && p.Required {
			errs = append(errs, fmt.Errorf(errString, p.Attribute))
			log.WithFields(QueryFields(c.R, c.StartTime)).Error(errs[len(errs) - 1])
			continue
		}

		i.Add(parsedAttribute)
	}

	for p := range q {
		errs = append(errs, fmt.Errorf("%s is not a valid parameter for this api", p))
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(errs[len(errs) - 1])
	}

	return errs
}

// Add a parsed attribute to Input
func (i Input) Add(attribute NullAttribute) {
	i[attribute.Attribute] = attribute
}

// AddValue a parsed parameter to Input
func (i Input) AddValue(attribute Attribute, value interface{}) {
	v := NewNullAttribute(attribute)
	v.Scan(value)
	i[attribute] = v
}

// Output is the default structure for APIs to return information
type Output struct {
	Status bool
	Err []error
	Out interface{}
}

type jsonOutput struct {
	Status string	`json:"ferry_status"`
	Err []string	`json:"ferry_error"`
	Out interface{}	`json:"ferry_output"`
}

// Parse the Output and writes to an http.ResponseWriter
func (o *Output) Parse(c APIContext, w http.ResponseWriter) () {
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
	fmt.Fprintf(w, string(parsedOut))
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
	ResourceName	Attribute = "resourcename"
	DN				Attribute = "dn"
	UserAttribute	Attribute = "attribute"
	Value			Attribute = "value"
	QuotaUnit		Attribute = "quotaunit"
	Path			Attribute = "path"
	Shell			Attribute = "shell"
	HomeDir			Attribute = "homedir"
	FQAN			Attribute = "fqan"
	Role			Attribute = "role"
	UID				Attribute = "uid"
	GID				Attribute = "gid"
	GroupID			Attribute = "groupid"
	DNID			Attribute = "dnid"
	FQANID			Attribute = "fqanid"
	UnitID			Attribute = "unitid"
	ResourceID		Attribute = "compid"
	Quota			Attribute = "quota"
	Status			Attribute = "status"
	Primary			Attribute = "primary"
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
		ResourceName:	TypeString,
		DN:				TypeString,
		UserAttribute:	TypeString,
		Value:			TypeString,
		QuotaUnit:		TypeString,
		Path:			TypeString,
		Shell:			TypeString,
		HomeDir:		TypeString,
		FQAN:			TypeString,
		Role:			TypeString,
		UID:			TypeInt,
		GID:			TypeInt,
		GroupID:		TypeInt,
		DNID:			TypeInt,
		FQANID:			TypeInt,
		UnitID:			TypeInt,
		ResourceID:		TypeInt,
		Quota:			TypeFloat,
		Status:			TypeBool,
		Primary:		TypeBool,
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
	AbsoluteNull bool // Valid is true if Scan receives is the string "NULL"
}

// Scan implements the Scanner interface.
func (na *NullAttribute) Scan(value interface{}) error {	
	if stringValue, ok := value.(string); ok {
		if strings.ToLower(stringValue) == "null" {
			na.AbsoluteNull = true
			return nil
		}
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
	return na.Data, nil
}

// Default returns a copy of the NullAttribute replacing Data with value
// if Valid is false and is not an AbsoluteNull
func (na NullAttribute) Default(value interface{}) NullAttribute {
	if !na.Valid && !na.AbsoluteNull {
		na.Scan(value)
		return na
	}
	return na
}

// APIError is returned by a BaseAPI
type APIError struct {
	Error error
	Type ErrorType
}

// ErrorType is a type of APIError
type ErrorType int

// List of ErrorType
const (
	HTTP200 ErrorType = iota
	ErrorDataNotFound
	ErrorInvalidData
	ErrorAPIRequirement
	HTTP500
	ErrorDbQuery
)

// DefaultMessage for BaseAPI errors
func (t ErrorType) DefaultMessage() string {
	messageMap := map[ErrorType]string {
		ErrorDbQuery:		"error while querying the database",
		ErrorDataNotFound:	"%s not found",
		ErrorInvalidData:	"%s is invalid",
	}
	return messageMap[t]
}

// DefaultAPIError makes an APIError using the default message for the ErrorType
// and takes interfaces to complete it when necessary
func DefaultAPIError(t ErrorType, a interface{}) APIError {
	if a != nil {
		return APIError{fmt.Errorf(t.DefaultMessage(), a), t}
	}
	return APIError{errors.New(t.DefaultMessage()), t}
}

// APIContext stores metadata used through the API execution
type APIContext struct {
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

// NewMapNullAttribute builds a map of Attribute to NullAttribute
func NewMapNullAttribute(attributes ...Attribute) map[Attribute]*NullAttribute {
	mapNullAttribute := make(map[Attribute]*NullAttribute)

	for _, attribute := range attributes {
		mapNullAttribute[attribute] = &NullAttribute{attribute, nil, false, false}
	}

	return mapNullAttribute
}

// NewNullAttribute builds a NullAttribute of type Attribute
func NewNullAttribute(attribute Attribute) NullAttribute {
	return NullAttribute{attribute, nil, false, false}
}