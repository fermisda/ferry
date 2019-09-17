#!/usr/bin/env python3

import os
import sys
import configparser
import argparse
import difflib
import re
import urllib.request
import ssl
import datetime
import json
import logging
from threading import Thread

class User:
    def __init__(self, uid, uname, full_name = None, status = False, expiration_date = ""):
        self.uid = uid
        self.uname = uname
        self.full_name = full_name
        self.status = status
        self.expiration_date = expiration_date
        self.gid = ""
        self.certificates = []
        self.fqans = []
        self.compute_access = {}
    
    def __str__(self):
        self.string = "User object:\n\
                        \tuid: %s\n\
                        \tuname: %s\n\
                        \tfull_name: %s\n\
                        \tstatus: %s\n\
                        \texpiration_date: %s"
        return self.string % (self.uid, self.uname, self.full_name, self.status, self.expiration_date)

    def diff(self, user):
        diff = []
        if self.uid != user.uid:
            diff.append("uid")
        if self.uname != user.uname:
            diff.append("uname")
        if self.full_name != user.full_name:
            diff.append("full_name")
        if self.status != user.status:
            diff.append("status")
        if self.expiration_date != user.expiration_date:
            diff.append("expiration_date")
        self.certificates.sort()
        user.certificates.sort()
        if self.certificates != user.certificates:
            diff.append("certificates")
        self.fqans.sort()
        user.fqans.sort()
        if self.fqans != user.fqans:
            diff.append("fqans")
        if self.compute_access != user.compute_access:
            diff.append("compute_access")
        return diff

    def addComputeAccess(self, accessString):
        accessString = accessString.replace("$USER", self.uname)
        resource, group, home, shell, primary = re.findall(r"(\w+):([\w$]+),([\w\/\$\-]+),([\w\/\$\-]+),(true|false)", accessString)[0]
        self.compute_access[resource] = {
            "username": self.uname,
            "groupname": group,
            "resourcename": resource,
            "shell": shell,
            "homedir": home,
            "primary": primary
        }

class Group:
    def __init__(self, gid, name, type = "UnixGroup"):
        self.gid = gid
        self.name = name
        self.type = type
        self.members = []

    def __str__(self):
        self.string = "Group object:\n\
                        \tgid: %s\n\
                        \tname: %s\n\
                        \ttype: %s\n\
                        \tmembers: %s"
        return self.string % (self.gid, self.name, self.type, self.members)

    def diff(self, group):
        diff = []
        if self.gid != group.gid:
            diff.append("gid")
        if self.name != group.name:
            diff.append("name")
        if self.type != group.type:
            diff.append("type")
        self.members.sort()
        group.members.sort()
        if self.members != group.members:
            diff.append("members")
        return diff

def postToSlack(messages, action):
    if not isinstance(messages, list):
        messages = [messages]
    for message in messages:
        if "slack" not in config:
            return
        target = dict(config.items("slack"))
        url = target["url"]
        data = {"payload": target["payload"]}
        data["payload"] = data["payload"].replace("$MSG", message)
        data["payload"] = data["payload"].replace("$ACT", action)
        data = bytes(urllib.parse.urlencode(data).encode())
        openURL(url, data=data)

def resources(resourcesString):
    resoruces = []
    for resource, _ in re.findall(r"(\w+):[\w$]+,[\w\/\$\-]+,[\w\/\$\-]+,(true|false);", resourcesString):
        resoruces.append(resource)
    return resoruces

def readFromFerry(action, params = None):
    source = dict(config.items("ferry"))
    url = source["hostname"] + source[action]
    if params:
        url += "?" + urllib.parse.urlencode(params)
    logging.debug("reading from url: %s" % url)
    tmpOut = openURL(url, context=ferryContext)
    if not tmpOut:
        exit(15)
    jOut = json.loads(tmpOut)
    if len(jOut["ferry_error"]) > 0:
        return None
    return jOut["ferry_output"]

def writeToFerry(action, params = None):
    for item in ["username", "groupname"]:
        if params and item in params and params[item] in skipList:
            return
    target = dict(config.items("ferry"))
    url = target["hostname"] + target[action]
    if params:
        url += "?" + urllib.parse.urlencode(params)
    if not opts.dry_run:
        logging.debug(url)
        tmpOut = openURL(url, context=ferryContext)
        if not tmpOut:
            exit(14)
        jOut = json.loads(tmpOut)
        logging.debug(jOut)
        if len(jOut["ferry_error"]) == 0:
            if not params:
                params = ""
            logging.info("action: %s(%s)" % (action, params))
            return True
        for error in jOut["ferry_error"]:
            logging.error("message: %s action: %s(%s)" % (error, action, params))
            postToSlack(error, "action: %s(%s)" % (action, params))
            if error in target["skip_user_errors"]:
                skipList.append(params["username"])
                break
            if error in target["skip_group_errors"]:
                skipList.append(params["groupname"])
                break
        return False
    else:
        params = ", ".join(["%s=%s" % (x, y) for x, y in params.items()])
        logging.info("action: %s(%s)" % (action, params))
        return True

def dateSwitcher(date):
    if not date or date == "EXPIRED":
        return ""
    if date == "No Expiration date":
        return "2038-01-01"
    return "%s" % date.split("T")[0]

# Downloads necessary files from UserDB to memory
def fetch_userdb():
    # Parses dates to Ferry format
    files = {}
    totalChangeRatio = 0

    threads = []
    def work(f, url):
        out = openURL(url)
        if out:
            if os.path.isfile(files[f]):
                os.rename(files[f], files[f] + ".cache")
            outFile = open(files[f], "w")
            outFile.write(out)
            outFile.close()

    for f, url in config.items("userdb"):
        if f not in ["uid.lis", "gid.lis", "services-users.csv"]:
            logging.error("invalid userdb file %s" % f)
            exit(2)
        files[f] = "%s/%s" % (cacheDir, f)
        if os.path.isfile(files[f] + ".error"):
            logging.error("bad %s file detected on a previous cycle" % f)
            postToSlack("Update Script Halted!", "Bad %s file detected on a previous cycle." % f)
            exit(5)
        logging.debug("Downloading %s: %s" % (f, url))
        thread = Thread(target=work, args=[f, url])
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    for f, url in config.items("userdb"):
        if not os.path.isfile(files[f]):
            exit(8)
        if os.path.isfile(files[f] + ".cache"):
            userdbLines = open(files[f], "r").readlines()
            cacheLines = open(files[f] + ".cache", "r").readlines()
            userdbLines.sort()
            cacheLines.sort()
            s = difflib.SequenceMatcher(None, userdbLines, cacheLines)
            changeRatio = 1 - s.ratio()
            totalChangeRatio += changeRatio
            if changeRatio > float(config.get("general", "cache_max_diff")):
                logging.error("file %s has too many changes" % f)
                postToSlack("Update Script Halted!", "File %s has too many changes." % f)
                os.rename(files[f], files[f] + ".error")
                os.rename(files[f] + ".cache", files[f])
                exit(4)
            os.remove(files[f] + ".cache")
        else:
            totalChangeRatio += 1

    if len(files) < 3:
        logging.error("not enough userdb files")
        exit(3)

    if totalChangeRatio == 0:
        logging.info("No changes detected on UserDB")
        exit(0)

    users = {}
    groups = {}
    
    unameUid = {}

    logging.debug("Reading gid.lis")
    gidLines = re.findall(r"(\d+)\t(.+)\t\t.+", open(files["gid.lis"], "r").read())
    for line in gidLines:
        gid, name = line
        gid = gid
        name = name.strip().lower()
        groups[gid] = Group(gid, name)

    logging.debug("Reading uid.lis")
    uidLines = re.findall(r"(\d+)\t\t(\d+)\t\t(.+)\t\t(.+)\t\t(.+)", open(files["uid.lis"], "r").read())
    for line in uidLines:
        uid, gid, last_name, first_name, uname = line
        uid = uid
        gid = gid
        uname = uname.lower().strip()
        full_name = " ".join([first_name.strip().capitalize(), last_name.strip().capitalize()]).strip()
        if uid not in users:
            users[uid] = User(uid, uname, full_name)
            users[uid].gid = gid
        if uname not in unameUid:
            unameUid[uname] = []
        if uid not in unameUid[uname]:
            unameUid[uname].append(uid)
        groups[gid].members.append(uid)
    # Ignores duplicated unames
    unameToDelete = []
    for uname in unameUid.keys():
        if len(unameUid[uname]) == 1:
            unameUid[uname] = unameUid[uname][0]
        else:
            for uid in unameUid[uname]:
                users.__delitem__(uid)
                for gid in groups.keys():
                    if uid in groups[gid].members:
                            groups[gid].members.remove(uid)
            unameToDelete.append(uname)
    for uname in unameToDelete:
        unameUid.__delitem__(uname)

    logging.debug("Reading services-users.csv")
    fileText = open(files["services-users.csv"], "r").read()
    servicesUsersLines = re.findall(r"(\w+)\,(\".+\"),(No\sExpiration\sdate|\d{4}-\d{2}-\d{2}|EXPIRED)", fileText)
    # Check number of users
    complete = True
    complete = complete and bool(re.search(r"# SERVICES active users list made on  [A-z]{3} [A-z]{3} \d{2} \d{2}:\d{2} \d{4}", fileText))
    loadedUsers =                re.search(r"# (\d+) username loaded from Active Directory", fileText)
    complete = complete and bool(re.search(r"# \d+ users output to list", fileText))
    complete = complete and bool(re.search(r"# SERVICES active users list completed on  [A-z]{3} [A-z]{3} \d{2} \d{2}:\d{2} \d{4}", fileText))
    complete = complete and bool(loadedUsers)
    if not complete:
        logging.error("file services-users.csv seem truncated")
        postToSlack("Update Script Halted!", "File services-users.csv seem truncated")
        os.rename(files["services-users.csv"], files["services-users.csv"] + ".error")
        os.rename(files["services-users.csv"] + ".cache", files[f])
        exit(6)
    if int(loadedUsers.group(1)) != len(servicesUsersLines):
        logging.error("file services-users.csv is missing users")
        postToSlack("Update Script Halted!", "File services-users.csv seem truncated")
        os.rename(files["services-users.csv"], files["services-users.csv"] + ".error")
        os.rename(files["services-users.csv"] + ".cache", files[f])
        exit(7)
    for line in servicesUsersLines:
        uname, full_name, expiration_date = line
        full_name = full_name.strip("\"")
        expiration_date = dateSwitcher(expiration_date)
        dn = config.get("general", "dn_template") % (full_name, uname)
        if uname in unameUid:
            users[unameUid[uname]].full_name = full_name
            users[unameUid[uname]].expiration_date = expiration_date
            users[unameUid[uname]].certificates.append(dn)
            users[unameUid[uname]].fqans = [tuple(i.split(":")) for i in config.get("ferry", "fqans").split(",")]
            for resoruce in config.get("ferry", "compute_resources").split(";"):
                users[unameUid[uname]].addComputeAccess(resoruce)
            if expiration_date != "":
                users[unameUid[uname]].status = True

    return users, groups

# Downloads necessary data from Ferry to memory
def fetch_ferry():
    source = dict(config.items("ferry"))

    users = {}
    groups = {}

    unameUid = {}

    threads = []
    ferryOut = {}
    def work(action, params = None, id = None):
        if not id:
            id = action
        ferryOut[id] = readFromFerry(action, params)
    
    threads.append(Thread(target=work, args=["api_get_users"]))
    threads.append(Thread(target=work, args=["api_get_certificates"]))
    threads.append(Thread(target=work, args=["api_get_groups"]))
    threads.append(Thread(target=work, args=["api_get_group_members"]))
    threads.append(Thread(target=work, args=["api_get_users_fqans"]))

    for accessString in source["compute_resources"].split(";"):
        resource = accessString.split(":")[0]
        logging.debug("Fetching Ferry users access to %s" % resource)
        threads.append(Thread(target=work, args=["api_get_passwd_file", {"resourcename": resource}, resource]))

    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    logging.debug("Fetching Ferry users")
    if not ferryOut["api_get_users"]:
            exit(9)
    for jUser in ferryOut["api_get_users"]:
        users[str(jUser["uid"])] = User(str(jUser["uid"]), jUser["username"], jUser["fullname"], jUser["status"], dateSwitcher(jUser["expirationdate"]))
        unameUid[jUser["username"]] = str(jUser["uid"])

    logging.debug("Fetching Ferry certificates")
    if not ferryOut["api_get_certificates"]:
            exit(10)
    for jUser in ferryOut["api_get_certificates"]:
        for jCert in jUser["certificates"]:
            users[unameUid[jUser["username"]]].certificates.append(jCert["dn"])

    logging.debug("Fetching Ferry groups")
    if not ferryOut["api_get_groups"]:
            exit(11)
    for jGroup in ferryOut["api_get_groups"]:
        groups[str(jGroup["gid"])] = Group(str(jGroup["gid"]), jGroup["groupname"], jGroup["grouptype"])

    logging.debug("Fetching Ferry group members")
    if not ferryOut["api_get_group_members"]:
            exit(12)
    for jGroup in ferryOut["api_get_group_members"]:
        if jGroup["members"] != None:
            for jUser in jGroup["members"]:
                if not jGroup["gid"]:
                    continue
                groups[str(jGroup["gid"])].members.append(str(jUser["uid"]))

    if not ferryOut["api_get_users_fqans"]:
            exit(13)
    for uname, items in ferryOut["api_get_users_fqans"].items():
        for item in items:
            users[unameUid[uname]].fqans.append((item["fqan"], item["unitname"]))

    for accessString in source["compute_resources"].split(";"):
        resource = accessString.split(":")[0]
        logging.debug("Fetching Ferry users access to %s" % resource)
        jPasswd = ferryOut[resource]
        if not jPasswd:
            logging.debug("Resorce %s not found." % resource)
            continue
        jPasswd = list(jPasswd.values())[0] # get first affiliation unit
        jPasswd = jPasswd["resources"][resource]
        for access in jPasswd:
            users[str(access["uid"])].addComputeAccess(accessString)

    return users, groups

# Checks to see if urls can be accessed successfully
def openURL(url, data = None, context = None):
    try:
        return (urllib.request.urlopen(url, data=data, context=context).read().decode())
    except:
        logging.error("Failed to access remote server: %s", url)
        return None


# Updates users with data from uid.lis and services-users.csv
def update_users():
    changes = False
    for user in userdbUsers.values():
        if user.uid not in ferryUsers.keys():
            if user.gid not in ferryGroups.keys():
                params = {
                    "gid": user.gid,
                    "groupname": userdbGroups[user.gid].name,
                    "grouptype": userdbGroups[user.gid].type
                }
                if writeToFerry("api_create_group", params):
                    ferryGroups[user.gid] = Group(user.gid, userdbGroups[user.gid].name)
                changes = True
            params = {
                "uid": user.uid,
                "username": user.uname,
                "fullname":  user.full_name,
                "status": str(user.status),
                "expirationdate": user.expiration_date,
                "groupname": userdbGroups[user.gid].name
            }
            if user.expiration_date == "":
                params.__delitem__("expirationdate")
            if writeToFerry("api_create_user", params):
                ferryUsers[user.uid] = User(user.uid, user.uname, user.full_name, user.status, user.expiration_date)
                ferryGroups[user.gid].members.append(user.uid)
            changes = True
        else:
            diff = user.diff(ferryUsers[user.uid])
            if "full_name" in diff or "expiration_date" in diff or "status" in diff:
                auxUser = ferryUsers[user.uid]
                params = {"username": user.uname}
                if "full_name" in diff:
                    params["fullname"] = user.full_name
                    auxUser.full_name = user.full_name
                if "expiration_date" in diff:
                    if user.expiration_date == "":
                        params["expirationdate"] = "null"
                    else:
                        params["expirationdate"] = user.expiration_date
                    auxUser.expiration_date = user.expiration_date
                if "status" in diff:
                    params["status"] = str(user.status)
                    auxUser.status = user.status
                if writeToFerry("api_set_user_info", params):
                    ferryUsers[user.uid] = auxUser
                changes = True
    if not changes:
        logging.info("Users are up to date")

# Updates groups with data from gid.lis
def update_groups():
    changes = False
    for group in userdbGroups.values():
        if group.gid not in ferryGroups.keys():
            params = {
                "gid": group.gid,
                "groupname": group.name,
                "grouptype": group.type
            }
            if writeToFerry("api_create_group", params):
                ferryGroups[group.gid] = Group(group.gid, group.name)
            changes = True

        diff = group.diff(ferryGroups[group.gid])
        if "groupname" in diff:
            logging.warning("Group %s name has changed from %s to %s" % (group.gid, ferryGroups[group.gid].name, group.name))
        if "members" in diff:
            for member in group.members:
                if member not in ferryGroups[group.gid].members:
                    params = {
                        "username": userdbUsers[member].uname,
                        "groupname": group.name,
                        "grouptype": group.type
                    }
                    if writeToFerry("api_add_group_member", params):
                        ferryGroups[group.gid].members.append(member)
                    changes = True
    if not changes:
        logging.info("Groups are up to date")

# Updates users certificates with data from services-users.csv
def update_certificates():
    changes = False
    for user in userdbUsers.values():
        if user.uid not in ferryUsers:
            continue
        diff = user.diff(ferryUsers[user.uid])
        if "certificates" in diff:
            for certificate in user.certificates:
                if certificate not in ferryUsers[user.uid].certificates:
                    if any(c in "," for c in certificate):
                        logging.warning("Certificate \"%s\" contains illegal characters" % certificate)
                        continue
                    jUnits = readFromFerry("api_get_user_affiliations", {"username": user.uname})
                    if not jUnits:
                        logging.debug("could not fetch affiliation units for %s" % user.uname)
                        jUnits = []
                    if len(jUnits) == 0:
                        jUnits.append({
                            "alternativename": "",
                            "unitname": "fermilab"
                        })
                    for unit in jUnits:
                        params = {
                            "username": user.uname,
                            "unitname": unit["unitname"],
                            "dn": certificate
                        }
                        if writeToFerry("api_add_user_certificate", params):
                            ferryUsers[user.uid].certificates.append(certificate)
                    changes = True
    if not changes:
        logging.info("Certificates are up to date")

# Updates users FQANs
def update_fqans():
    changes = False
    for user in userdbUsers.values():
        if user.uid not in ferryUsers:
            continue
        diff = user.diff(ferryUsers[user.uid])
        if "fqans" in diff:
            for fqan in user.fqans:
                if fqan not in ferryUsers[user.uid].fqans:
                    params = {
                        "username": user.uname,
                        "fqan": fqan[0],
                        "unitname": fqan[1]
                    }
                    if writeToFerry("api_set_user_fqan", params):
                        ferryUsers[user.uid].fqans.append(fqan)
                    changes = True
    if not changes:
        logging.info("FQANs are up to date")

# Updates users access to Fermilab batch resources
def update_compute_access():
    changes = False
    for user in userdbUsers.values():
        if user.uid not in ferryUsers:
            continue
        diff = user.diff(ferryUsers[user.uid])
        if "compute_access" in diff:
            for resource, params in user.compute_access.items():
                if resource not in ferryUsers[user.uid].compute_access.keys():
                    if writeToFerry("api_set_compute_access", params):
                        ferryUsers[user.uid].compute_access[resource] = params
                    changes = True
    if not changes:
        logging.info("Compute access is up to date")

# Cleans expired storage quotas
def clean_storage_quotas():
    if writeToFerry("api_clean_storage_quotas"):
        logging.info("Done")

# Cleans expired condor quotas
def clean_condor_quotas():
    if writeToFerry("api_clean_condor_quotas"):
        logging.info("Done")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = "Script to update Ferry with data from UserDB")
    parser.add_argument("-c", "--config", metavar = "PATH", action = "store", help = "path to configuration file")
    parser.add_argument("-d", "--dry-run", action = "store_true", help = "runs the script without touching the database")
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
    if "FERRY_SLACK_HOOK" in os.environ:
        config.set("slack", "url", os.environ["FERRY_SLACK_HOOK"])

    rootDir = os.path.dirname(os.path.realpath(__file__))
    if config.has_option("general", "cache_dir"):
        cacheDir = config.get("general", "cache_dir")
    else:
        cacheDir = rootDir + "/cache"
    os.makedirs(cacheDir, 0o755, True)


    logArgs = {
        "format": "[%(asctime)s][%(levelname)s] %(message)s",
        "datefmt": "%m/%d/%Y %H:%M:%S"
    }
    if config.has_option("log", "level"):
        logArgs["level"] = getattr(logging, config.get("log", "level"))
    else:
        logArgs["level"] = logging.DEBUG
    if config.has_option("log", "file"):
        logArgs["filename"] = config.get("log", "file")
    else:
        logArgs["stream"] = sys.stdout

    logging.basicConfig(**logArgs)

    ferryContext = ssl.SSLContext(protocol=ssl.PROTOCOL_TLSv1_2)
    ferryContext.verify_mode = ssl.CERT_REQUIRED
    ferryContext.load_cert_chain(config.get("ferry", "cert"), config.get("ferry", "key"))
    ferryContext.load_verify_locations(capath=config.get("ferry", "ca"))

    skipList = []

    logging.info("Starting Ferry User Update")

    logging.info("Fetching UserDB files...")
    userdbUsers, userdbGroups = fetch_userdb()

    logging.info("Fetching Ferry data...")
    ferryUsers, ferryGroups = fetch_ferry()

    logging.info("Updating users...")
    update_users()

    logging.info("Updating groups...")
    update_groups()

    logging.info("Updating certificates...")
    update_certificates()

    logging.info("Updating FQANs...")
    update_fqans()

    logging.info("Updating compute access...")
    update_compute_access()

    logging.info("Cleaning storage quotas...")
    clean_storage_quotas()

    logging.info("Cleaning Condor quotas...")
    clean_condor_quotas()