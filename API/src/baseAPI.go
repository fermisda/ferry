package main

import (
	"errors"
	"strconv"
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
func (i *InputModel) Add(name ParameterName, ptype ParameterType, optional bool) {
	*i = append(*i, Parameter{name, ptype, optional})
}

// Parse an http.Request and returns a ParsedInput
func (i InputModel) Parse(c APIContext) (Input, []error) {
	var errs []error
	input := make(Input)
	q := c.R.URL.Query()
	
	for _, parameter := range i {
		var parsedValue interface{}
		value := q.Get(string(parameter.Name))
		q.Del(string(parameter.Name))

		errString := "required parameter '%s' not provided"
		if value == "" && !parameter.Optional {
			errs = append(errs, fmt.Errorf(errString, parameter.Name))
			log.WithFields(QueryFields(c.R, c.StartTime)).Error(errs[len(errs) - 1])
			continue
		}

		errString = "parameter '%s' requires a %s value"
		if value != "" {
			switch parameter.Type {
			case TypeString:
				parsedValue = value
			case TypeInt:
				if v, err := strconv.ParseInt(value, 10, 64); err == nil {
					parsedValue = v
				} else {
					errs = append(errs, fmt.Errorf(errString, parameter.Name, TypeInt))
					log.WithFields(QueryFields(c.R, c.StartTime)).Error(errs[len(errs) - 1])
					continue
				}
			case TypeUint:
				if v, err := strconv.ParseUint(value, 10, 64); err == nil {
					parsedValue = v
				} else {
					errs = append(errs, fmt.Errorf(errString, parameter.Name, TypeUint))
					log.WithFields(QueryFields(c.R, c.StartTime)).Error(errs[len(errs) - 1])
					continue
				}
			case TypeFloat:
				if v, err := strconv.ParseFloat(value, 64); err == nil {
					parsedValue = v
				} else {
					errs = append(errs, fmt.Errorf(errString, parameter.Name, TypeFloat))
					log.WithFields(QueryFields(c.R, c.StartTime)).Error(errs[len(errs) - 1])
					continue
				}
			case TypeBool:
				if v, err := strconv.ParseBool(value); err == nil {
					parsedValue = v
				} else {
					errs = append(errs, fmt.Errorf(errString, parameter.Name, TypeBool))
					log.WithFields(QueryFields(c.R, c.StartTime)).Error(errs[len(errs) - 1])
					continue
				}
			}
		}

		input.Add(parameter.Name, parsedValue)
	}

	for p := range q {
		errs = append(errs, fmt.Errorf("'%s' is not a valid parameter for this api", p))
		log.WithFields(QueryFields(c.R, c.StartTime)).Error(errs[len(errs) - 1])
	}

	return input, errs
}

// Input is a dictionary of parsed parameters for an API
type Input map[ParameterName]interface{}

// Add a parsed parameter to Input
func (i Input) Add(name ParameterName, value interface{}) {
	i[name] = value
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
	Name ParameterName
	Type ParameterType
	Optional bool
}

// ParameterName represents a valid parameter names to be used by an API
type ParameterName string

// List of valid parameter names
const (
	UserName 		ParameterName = "username"
	GroupName		ParameterName = "groupname"
	UnitName		ParameterName = "unitname"
	FullName 		ParameterName = "fullname"
	UID				ParameterName = "uid"
	GID				ParameterName = "gid"
	Status			ParameterName = "status"
	GroupAccount  	ParameterName = "groupaccount"
	ExpirationDate	ParameterName = "expirationdate"
)

// ParameterType represents a valid parameter types to be used by an API
type ParameterType string

// List of valid parameter types
const (
	TypeInt 	ParameterType = "integer"
	TypeUint	ParameterType = "unsigned integer"
	TypeFloat	ParameterType = "float"
	TypeBool	ParameterType = "boolean"
	TypeString	ParameterType = "string"
)

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