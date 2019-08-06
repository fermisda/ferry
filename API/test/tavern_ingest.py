import urllib.request
import json
import ssl
import configparser
import logging
import pytest
import os
import argparse
import sys
import datetime
import copy
import subprocess
from tavern.core import run
import yaml 
import string

#connects to ferry and gets parameters for an API
def jsonImportHelp(api):
    target = dict(config.items("ferry"))
    url = target["hostname"] + api + "?help"
    logging.info("action: %s" % url)
    try:
        jOut = json.loads(urllib.request.urlopen(url, context=ferryContext).read().decode())
        #valid calls with help should return empty lists for "ferry_error", len was required for some reason
        if jOut["ferry_error"]:
            return None
    except:
        return None
    return jOut

#finds all combinations of length n in list lst    
def combo2(lst,n):
    if n==0:
        return [[]]
    l=[]
    for i in range(0,len(lst)):
        m=lst[i]
        remLst=lst[i+1:]
        for p in combo2(remLst,n-1):
            l.append([m]+p)
    return l


def testUrlCreator(url, jOut):
    #saved url to check if it has changed
    savedUrl = url
    notRequiredAttributes = []
    notRequiredCount, testCount, expectedNumberTests = 0, 0, 0
    #checks base call without parameters, whether error checking or valid
    if not yamlWriter(url):
            testCount += 1
    expectedNumberTests += 1
    for attribute in jOut['ferry_output']:
        strCastAttribute = str(attribute)
        if jOut['ferry_output'][strCastAttribute]['required'] == "true":
            atrbType = str(jOut['ferry_output'][strCastAttribute]['type'])
            #if first parameter, add '?' delimeter
            if savedUrl == url:
                url += "?"
            else:
                url += "&"
            #checks every combination of potentially wrong parameters to verify error checking, yamlWriter returns 0 on success
            if not yamlWriter(url + strCastAttribute + "=True"):
                testCount += 1
            if not yamlWriter(url + strCastAttribute + "=12345678"):
                testCount += 1  
            if not yamlWriter(url + strCastAttribute + "=2027-07-11"):
                testCount += 1
            if not yamlWriter(url + strCastAttribute + "=test"):
                testCount += 1
            if not yamlWriter(url + strCastAttribute + "=9.001"):
                testCount += 1
            expectedNumberTests += 5
            #correct parameter appending
            if atrbType == "string":
                url += strCastAttribute + "=test"
            elif atrbType == "boolean":
                url += strCastAttribute + "=True"
            elif atrbType == "date":
                url += strCastAttribute + "=2027-07-11"
            elif atrbType == "integer":
                url += strCastAttribute + "=12345678"
            elif atrbType == "float":
                url += strCastAttribute + "=9.001"
        #creation of array with each non-required attribute, also a count of these
        else:
            notRequiredAttributes.append(strCastAttribute)
            notRequiredCount += 1

    #number of tests computed with 2^n, where n is number of non-required attributes + # of already performed tests, -1 to account for overlap
    expectedNumberTests = (2 ** notRequiredCount) + expectedNumberTests - 1
    #if no non-required attributes, they should be equal (2^0 - 1 = 0)
    if testCount == expectedNumberTests:
        return 0
    #second url variable to account for the initial changes from required parameters appended above
    savedUrl2 = url
    #calls combination function, creates url and test for each combination
    for i in range(1, len(notRequiredAttributes)+1):
        attributeCombos =  combo2(notRequiredAttributes, i)
        #loop for separate combinations
        for j in range(0, len(attributeCombos)):
            #loop is for separate attributes within the combination
            for k in range(0, len(attributeCombos[j])):
                #same operation of appending to url as above
                atrbType = str(jOut['ferry_output'][attributeCombos[j][k]]['type'])
                if savedUrl == url:
                    url += "?"
                else:
                    url += "&"
                if atrbType == "string":
                    url += str(attributeCombos[j][k]) + "=test"
                elif atrbType == "boolean":
                    url += str(attributeCombos[j][k]) + "=True"
                elif atrbType == "date":
                    url += str(attributeCombos[j][k]) + "=2027-07-11"
                elif atrbType == "integer":
                    url += str(attributeCombos[j][k]) + "=12345678"
                elif atrbType == "float":
                    url += str(attributeCombos[j][k]) + "=9.001"
            if not yamlWriter(url):
                testCount += 1
            url = savedUrl2
    logging.info("Testing for %s completed for %d/%d tests" % (url, testCount, expectedNumberTests))
    return 0

def order_dict(dictionary):
    result = {}
    for k, v in sorted(dictionary.items()):
        if isinstance(v, dict):
            result[k] = order_dict(v)
        else:
            result[k] = v
    return result
#creates a custom tavern.yaml file for a set an api and a combination of its parameters
def yamlWriter(urlparameters):
    target = dict(config.items("ferry"))
    #creating the separate URLs for test API and reference API
    hostUrl = target["hostname"] + urlparameters
    testHostUrl = target["testhost"] + urlparameters
    jOut = json.loads(urllib.request.urlopen(hostUrl, context=ferryContext).read().decode())

    if len(jOut) == 0:
        logging.error("Something went wrong loading the output from %s" % hostUrl)
        return -1
    #yaml dictionary creation
    testFileName = "test_" + urlparameters + ".tavern.yaml"
    tavernTest = open(testFileName, "w")
    tavernYaml = {}
    tavernYaml["test_name"] = testFileName
    tavernYaml["stages"] = []
    tavernYaml["stages"].append({})
    tavernYaml["stages"][0]["name"] = urlparameters
    tavernYaml["stages"][0]["request"] = {}
    tavernYaml["stages"][0]["request"]["url"] = testHostUrl
    tavernYaml["stages"][0]["request"]["method"] = "GET"
    tavernYaml["stages"][0]["request"]["cert"] = []
    tavernYaml["stages"][0]["request"]["cert"].append(target["cert"])
    tavernYaml["stages"][0]["request"]["cert"].append(target["key"])
    tavernYaml["stages"][0]["request"]["verify"] = False

    tavernYaml["stages"][0]["response"] = {}
    tavernYaml["stages"][0]["response"]["status_code"] =  200
    tavernYaml["stages"][0]["response"]["body"] =  {}
    tavernYaml["stages"][0]["response"]["body"]["ferry_status"] = jOut["ferry_status"]

    #hiring someone else to read a dictionary
    tavernYaml["stages"][0]["response"]["body"]["ferry_error"] = copy.deepcopy(jOut['ferry_error'])
    tavernYaml["stages"][0]["response"]["body"]["ferry_output"] = copy.deepcopy(jOut['ferry_output'])
    order_dict(tavernYaml)
    yaml.dump(tavernYaml, tavernTest)
    logging.info("Testing %s" % urlparameters)
    #tavern run function returns 0 on success
    return run(testFileName)
    
def logFaultOnly():
    fd = open("concise_output", 'r')
    filterLog = open("fault_output", 'w')
    filterLog.truncate(0)
    filterLog.writelines([line for line in fd if 'ERROR' in line or 'CRITICAL' in line])
    fd.close()
        
def logCleaner():
    if config.has_option("log", "file"):
        fd = open(config.get("log", "file"), 'r')
        filterOutput = open("concise_output", 'w')
        filterOutput.truncate(0)
        filterOutput.writelines([ line for line in fd if 'Testing' in line or 'ERROR' in line or 'CRITICAL' in line])
        fd.close()
        logFaultOnly()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = "Script to test an API")
    parser.add_argument("-c", "--config", metavar = "PATH", action = "store", help = "path to configuration file")
    parser.add_argument("-n", "--api", action = "store", help = "passed api name/all apis")
    opts = parser.parse_args()
    try:
        config = configparser.ConfigParser()
        if opts.config:
            configpath = opts.config
        else:
            configpath = os.path.dirname(os.path.realpath(__file__)) + "/test.config"
        config.read_file(open(configpath))
    except:
        logging.error("could not find configuration file")
        exit(1)

    if "FERRY_API_HOST" in os.environ:
        config.set("ferry", "hostname", os.environ["FERRY_API_HOST"])

    logArgs = {
        "format": "[%(asctime)s][%(levelname)s] %(message)s",
        "datefmt": "%m/%d/%Y %H:%M:%S"
    }
    if config.has_option("log", "level"):
        logArgs["level"] = getattr(logging, config.get("log", "level"))
    else:
        logArgs["level"] = logging.DEBUG
    if config.has_option("log", "file"):
        fd = open(config.get("log", "file"), 'w')
        logArgs["filename"] = config.get("log", "file")
    else:
        logArgs["stream"] = sys.stdout
    logging.basicConfig(**logArgs)

    #sets up ferry connection
    ferryContext = ssl.SSLContext(protocol=ssl.PROTOCOL_TLSv1_2)
    ferryContext.verify_mode = ssl.CERT_REQUIRED
    ferryContext.load_cert_chain(config.get("ferry", "cert"), config.get("ferry", "key"))
    ferryContext.load_verify_locations(capath=config.get("ferry", "ca"))
    current_dir = os.path.dirname(os.path.abspath(__file__)) 
    subprocess.call("./db_connect_test_cleanup.sh", shell = True)
    #reads in all API names
    if opts.api == "all":
        with open('api_names') as openfileobject:
            subprocess.call("./db_refresh.sh", shell = True)
            for line in openfileobject:
                line = line.translate({ord(c): None for c in string.whitespace})
                jOut = jsonImportHelp(line)
                if jOut == None:
                    logging.error("%s help details could not be acquired" % line)
                    continue
                else:
                    testUrlCreator(line, jOut)
                    logCleaner()
                    subprocess.call("./db_refresh.sh", shell = True)
                    
    else:
        jOut = jsonImportHelp(opts.api)
        if jOut == None:
            logging.error("%s help details could not be acquired" % opts.api)
            exit(2)
        else:
            testUrlCreator(opts.api, jOut)
            logCleaner()
        
