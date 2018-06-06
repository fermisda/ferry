#!/usr/bin/env python

""" This script is fetches CA information and matches them to DNs
"""

import glob
import re

def fetchCAs(CADir):
    pathList = glob.glob(CADir + "/*.namespaces")
    namespaces = {}
    for path in pathList:
        f = open(path)
        aliases = re.findall("TO Issuer \"(.*)\".*\n.*PERMIT Subject \"(.*)\"", f.read())
        for alias in aliases:
            namespaces[alias[0]] = alias[1]

    pathList = glob.glob(CADir + "/*.info")
    CAs = {}
    for path in pathList:
        f = open(path)
        fileName = path.split("/")[-1].split(".")[0]
        if 'policy-igtf' in fileName:
            continue
        CAs[fileName] = {}
        for line in f.readlines():
            if not line.startswith("#"):
                line = re.split(" =[ \t]", line)
                CAs[fileName][line[0]] = line[1].strip().strip('"')
        CAs[fileName]["regex"] = namespaces[CAs[fileName]["subjectdn"]]

    return CAs

def matchCA(CAs, subject):
    bestCAs = []
    for CA in CAs:
            if re.match(".*" + CAs[CA]["regex"], subject):
                bestCAs.append(CAs[CA])

    return bestCAs
