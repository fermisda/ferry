package main

import (
	"errors"
	"strings"
	"io/ioutil"
	"regexp"
	"path/filepath"
)

// CA stores CA attributes
type CA map[string]string

// CAs stores multiple CA objects
type CAs map[string]CA

// FetchCAs reads CA namespaces from a directory and return CAs
func FetchCAs(caDir string) (CAs, error) {
	namespaces := make(map[string]string)
	reAliases := regexp.MustCompile(`TO Issuer \"(.*)\".*\n.*PERMIT Subject \"(.*)\"`)
	pathList, _ := filepath.Glob(caDir + "/*.namespaces")
	for _, path := range pathList {
		file, err := ioutil.ReadFile(path)
		if err != nil {
			return nil, err
		}
		aliases := reAliases.FindAllStringSubmatch(string(file), -1)
		for _, alias := range aliases {
			namespaces[alias[1]] = alias[2]
		}
	}

	cas := make(CAs)
	reAttributes := regexp.MustCompile(`([\w\.]+)\s=[\s\t]\"?([\w\.\:\-\@\/\= ]+)\"?`)
	pathList, _ = filepath.Glob(caDir + "/*.info")
	for _, path := range pathList {
		name := strings.Split(filepath.Base(path), ".")[0]
		if strings.Contains(name, "policy-igtf") || strings.Contains(name, "cilogon-silver") {
			continue
		}
		file, err := ioutil.ReadFile(path)
		if err != nil {
			return nil, err
		}
		attributes := reAttributes.FindAllStringSubmatch(string(file), -1)
		cas[name] = make(CA)
		for _, attribute := range attributes {
			cas[name][attribute[1]] = attribute[2]
		}
		cas[name]["regex"] = namespaces[cas[name]["subjectdn"]]
	}

	return cas, nil
}

// MatchCA tries to match a subject with a CA in CAs
func (cas CAs) MatchCA(subject string) (CA, error) {
	var bestCA CA
	for _, ca := range cas {
		match, err := regexp.MatchString(".*" + ca["regex"], subject)
		if err != nil {
			return nil, err
		}
		if match {
			if bestCA == nil || len(bestCA["regex"]) < len(ca["regex"]) {
				bestCA = ca
			}
		}
	}

	if bestCA == nil {
		return nil, errors.New("could not match subject to a CA")
	}

	return bestCA, nil
}
