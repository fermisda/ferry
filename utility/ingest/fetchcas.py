#!/usr/bin/env python

""" This script is fetches CA information and matches them to DNs
"""

import glob
import os
import re

from collections import defaultdict


def parseNamespaces(file):
    namespaces = defaultdict(list)

    aliases = re.findall(r"TO Issuer \"(.*)\".*\n.*PERMIT Subject \"(.*)\"", file.read())
    for alias in aliases:
            namespaces[alias[0]].append(alias[1])

    return namespaces


def parseSigningPolicy(file):
    namespaces = defaultdict(list)

    content = file.read()
    subject = re.findall(r"access_id_CA\s+X509\s+\'(.*)\'", content)[0]
    aliases = re.findall(r"cond_subjects\s+globus\s+\'(.*)\'", content)[0].strip("\"").split("\" \"")
    namespaces[subject] = aliases
   
    return namespaces


def parseInfo(file):
    info = {}

    for line in file.readlines():
        if not line.startswith("#"):
            line = re.split(" =[ \t]", line)
            info[line[0]] = line[1].strip().strip('"')
   
    return info


def fetchCAs(CADir):
    CAs = defaultdict(dict)

    pathList = glob.glob(CADir + "/*.pem")
    for path in pathList:
        nmsPath = path.replace(".pem", ".namespaces")
        sgpPath = path.replace(".pem", ".signing_policy")
        infPath = path.replace(".pem", ".info")
        crlPath = path.replace(".pem", ".crl_url")

        namespaces = {}
        if os.path.isfile(nmsPath):
            namespaces = parseNamespaces(open(nmsPath))
        elif os.path.isfile(sgpPath):
            namespaces = parseSigningPolicy(open(sgpPath))

        info = {}
        if os.path.isfile(infPath):
            info = parseInfo(open(infPath))
        
        crl = []
        if os.path.isfile(crlPath):
            crl = [url.strip() for url in open(crlPath).readlines()]
        
        fileName = path.split("/")[-1].split(".")[0]

        CAs[fileName].update(info)
        CAs[fileName]["crl"] = crl
        CAs[fileName]["namespaces"] = namespaces

    return CAs


def matchCA(CAs, subject):
    bestCAs = []
    for CA in CAs.values():
        for namespace in CA["namespaces"].values():
            for regex in namespace:
                if re.match(".*" + regex, subject):
                    bestCAs.append(CA)

    return bestCAs
