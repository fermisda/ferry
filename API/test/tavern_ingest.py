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
#connects to ferry and gets parameters for an API
def jsonImportHelp(api):
    target = dict(config.items("ferry"))
    url = target["hostname"] + api + "?help"
    try:
        jOut = json.loads(urllib.request.urlopen(url, context=ferryContext).read().decode())
    except:
        return None
    logging.info("action: %s" %api)
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
    tmpIdx = 0
    testCount = 0
    expectedNumberTests = 0
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
            if not yamlWriter(url + strCastAttribute + "=900000"):
                testCount += 1  
            if not yamlWriter(url + strCastAttribute + "=2027-07-11"):
                testCount += 1
            if not yamlWriter(url + strCastAttribute + "=test"):
                testCount += 1
            expectedNumberTests += 4
            #makes the correct url parameter addition otherwise
            if atrbType == "string":
                url += strCastAttribute + "=test"
            elif atrbType == "boolean":
                url += strCastAttribute + "=True"
            elif atrbType == "date":
                url += strCastAttribute + "=2027-07-11"
            elif atrbType == "integer":
                url += strCastAttribute + "=9001"
            else:
                url += strCastAttribute + "=-1"
            #tests with all combinations of parameters (or lack of parameters), makes sure API error checking is correct
        #creation of array with each non-required attribute, also a count of these
        else:
            notRequiredAttributes.append(strCastAttribute)
            tmpIdx += 1

    #number of tests computed with 2^n, where n is number of non-required attributes + # of already performed tests, -1 to account for overlap
    expectedNumberTests = (2 ** tmpIdx) + expectedNumberTests - 1
    #if no non-required attributes, they should be equal (2^0 - 1 = 0)
    if testCount == expectedNumberTests:
        return 
    #second url variable to account for the initial changes wrought by required parameters
    savedUrl2 = url
    #calls combination function, creates url and test for each combination
    for i in range(1, len(notRequiredAttributes)+1):
        attributeCombos =  combo2(notRequiredAttributes, i)
        #this for loop refers to separate combinations
        for j in range(0, len(attributeCombos)):
            #this for loop is for separate attributes within the combination
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
                    url += str(attributeCombos[j][k]) + "=9001"
            if not yamlWriter(url):
                testCount += 1
            url = savedUrl2
    logging.critical("%d/%d tests were successfully performed for %s" % (testCount, expectedNumberTests, url))
        
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
    
    yaml.dump(tavernYaml, tavernTest)
    logging.info("Testing %s" % urlparameters)
    #tavern run function returns 0 on success
    return run(testFileName)
    
    
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
            for line in openfileobject:
                subprocess.call("./db_refresh.sh", shell = True)
                jOut = jsonImportHelp(line)
                if jOut == None:
                    logging.error("%s help details could not be acquired" % line)
                    continue
                testUrlCreator(line, jOut)
    else:
        jOut = jsonImportHelp(opts.api)
        if jOut == None:
            logging.error("%s help details could not be acquired" % opts.api)
            exit(2)
        testUrlCreator(opts.api, jOut)
    #cleans up test output
    if config.has_option("log", "file"):
        fd = open(config.get("log", "file"), 'r')
        filterOutput = open("concise_output", 'w')
        filterOutput.truncate(0)
        filterOutput.writelines([ line for line in fd if 'Testing' in line or 'ERROR' in line or 'CRITICAL' in line])
        fd.close()
