package main

import (
	"net/http"
)

// BaseAPI is a basic type to build APIs
type BaseAPI struct {
	InputModel InputModel
	QueryFunction func(Input) Output
}

// Run the API
func (b BaseAPI) Run(w http.ResponseWriter, r *http.Request) {
	parsedInput, _ := b.InputModel.Parse(r)
	b.QueryFunction(parsedInput)
}

// InputModel describes all parameters used by an API
type InputModel []Parameter

// Add a parameter to the InputModel object
func (i *InputModel) Add(name ParameterName, optional bool) {
	*i = append(*i, Parameter{name, optional})
}

// Parse the API's Input and returns a ParsedInput
func (i InputModel) Parse(r *http.Request) (Input, error) {
	input := make(Input)
	q := r.URL.Query()
	
	for _, parameter := range i {
		input.Add(parameter.Name, q.Get(parameter.Name.String()))
	}

	return input, nil
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

// Parameter for basic APIs
type Parameter struct {
	Name ParameterName
	Optional bool
}

// ParameterName represents a valid parameter name to be used by an API
type ParameterName int

// List of valid parameter names
const (
	UserName ParameterName = iota
	GroupName
	UnitName
)

func (s ParameterName) String() string {
	var parameterNameStrings = [...]string {
		"username",
		"groupname",
		"unitname",
	}
	return parameterNameStrings[s]
}

// APICollection aggregates a collection of APIs to be called from a function
type APICollection map[string]*BaseAPI

// Add a BaseAPI to the collection
func (c APICollection) Add(name string, api *BaseAPI) {
	c[name] = api
}