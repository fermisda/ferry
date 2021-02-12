#!/usr/bin/python3

import sys
import os
import glob
import logging
import argparse
import configparser
import ssl
import json
import subprocess
from urllib import request

# Set script arguments
parser = argparse.ArgumentParser(description = "Script to hold jobs of users suspended in FERRY")
parser.add_argument("-c", "--config", metavar = "PATH", action = "store", help = "path to configuration file")
parser.add_argument("-d", "--dry-run", action = "store_true", help = "runs the script without touching the jobs")
opts = parser.parse_args()

# Load config file
try:
    config = configparser.ConfigParser()
    if opts.config:
        configpath = opts.config
    else:
        root = os.path.dirname(os.path.realpath(__file__))
        files = glob.glob(root + "/*.config")
        if len(files) == 1:
            configpath = files[0]
    config.read(configpath)
except Exception as error:
    logging.error("could not find configuration file")
    exit(1)

# Set log arguments
logArgs = {
    "format": "[%(asctime)s][%(levelname)s] %(message)s",
    "datefmt": "%m/%d/%Y %H:%M:%S"
}
if config.has_option("log", "level"):
    logArgs["level"] = getattr(logging, str.upper(config.get("log", "level")))
else:
    logArgs["level"] = logging.DEBUG
if config.has_option("log", "file"):
    logArgs["filename"] = config.get("log", "file")
else:
    logArgs["stream"] = sys.stdout

logging.basicConfig(**logArgs)

# FERRY SSL settings
try:
    ferryContext = ssl.SSLContext(protocol=ssl.PROTOCOL_TLSv1_2)
    ferryContext.verify_mode = ssl.CERT_REQUIRED
    ferryContext.load_cert_chain(config.get("ssl", "cert"), config.get("ssl", "key"))
    ferryContext.load_verify_locations(capath=config.get("ssl", "cadir"))
except Exception as error:
    logging.error(error)
    exit(2)

# Fetch suspended users and create constraints
url = config.get("ferry", "hostname") + config.get("ferry", "api")
ferryOut = request.urlopen(url, context=ferryContext).read().decode()
if not ferryOut:
    logging.error("could not contact ferry")
    exit(1)
if "Query returned no FQANs." in ferryOut:
    logging.debug("no suspended users")
    exit(0)

suspendedUsers = json.loads(ferryOut)["ferry_output"]
constraints = []
for user, items in suspendedUsers.items():
    for item in items:
        constraint = '(Owner=="%s" && Jobsub_Group=="%s")' % (user, item["unitname"])
        if constraint not in constraints:
            logging.info("holding jobs from %s at %s" % (user, item["unitname"]))
            constraints.append(constraint)

if len(constraints) == 0:
    exit(0)

# Hold jobs
command = f"{config.get('condor', 'hold_command')} '{' || '.join(constraints)}'"

if not opts.dry_run:
    logging.debug(command)
    
    out = open(".tmpout", "w")
    err = open(".tmperr", "w")

    p = subprocess.Popen(command, stdout=out, stderr=err, shell=True)
    p.wait()
    out.close()
    err.close()
    
    out = open(".tmpout", "r")
    err = open(".tmperr", "r")
    for line in out.readlines():
        if line != "\n":
            logging.info(line.strip())
    for line in err.readlines():
        if line != "\n":
            if "Couldn't find/hold all jobs matching constraint" in line:
                logging.debug(line.strip())
            else:
                logging.error(line.strip())
    
    os.remove(".tmpout")
    os.remove(".tmperr")