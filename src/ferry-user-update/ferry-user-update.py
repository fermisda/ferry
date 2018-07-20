#!/usr/bin/env python3

import os
import sys
import configparser
import argparse
import re
import urllib.request
import ssl
import datetime
import json
import logging

class User:
    def __init__(self, uid, uname, full_name = None, status = False, expiration_date = ""):
        self.uid = uid
        self.uname = uname
        self.full_name = full_name
        self.status = status
        self.expiration_date = expiration_date
        self.certificates = []
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
        if self.compute_access != user.compute_access:
            diff.append("compute_access")
        return diff

    def addComputeAccess(self, accessString):
        accessString = accessString.replace("$USER", self.uname)
        resource, group, home, shell, primary = re.findall(r"(\w+):([\w$]+),([\w\/\$]+),([\w\/\$]+),(true|false)", accessString)[0]
        self.compute_access[resource] = {
            "username": self.uname,
            "groupname": group,
            "resourcename": resource,
            "shell": shell,
            "home_dir": home,
            "is_primary": primary
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

def resources(resourcesString):
    resoruces = []
    for resource, _ in re.findall(r"(\w+):[\w$]+,[\w\/\$]+,[\w\/\$]+,(true|false);", resourcesString):
        resoruces.append(resource)
    return resoruces

def writeToFerry(action, params):
    target = dict(config.items("ferry"))
    urlParams = {}
    for key, value in params.items():
        urlParams[key] = value.replace("'", "''")
    urlParams = urllib.parse.urlencode(urlParams)
    url = target["hostname"] + target[action] + "?" + urlParams
    if not opts.dry_run:
        logging.debug(url)
        jOut = json.loads(urllib.request.urlopen(url, context=ferryContext).read().decode())
        logging.debug(jOut)
        if not "ferry_error" in jOut:
            logging.info("action: %s(%s)" % (action, params))
            return True
        logging.error(jOut["ferry_error"])
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
    for f, u in config.items("userdb"):
        if f not in ["uid.lis", "gid.lis", "services-users.csv"]:
            logging.error("invalid userdb file %s" % f)
            exit(2)
        logging.debug("Downloading %s: %s" % (f, u))
        files[f] = urllib.request.urlopen(u).read().decode()

    if len(files) < 3:
        logging.error("not enough userdb files")
        exit(3)

    users = {}
    groups = {}
    
    unameUid = {}

    logging.debug("Reading gid.lis")
    gidLines = re.findall(r"(\d+)\t(.+)\t\t.+", files["gid.lis"])
    for line in gidLines:
        gid, name = line
        gid = gid
        name = name.strip().lower()
        groups[gid] = Group(gid, name)

    logging.debug("Reading uid.lis")
    uidLines = re.findall(r"(\d+)\t\t(\d+)\t\t(.+)\t\t(.+)\t\t(.+)", files["uid.lis"])
    for line in uidLines:
        uid, gid, last_name, first_name, uname = line
        uid = uid
        gid = gid
        uname = uname.lower().strip()
        full_name = " ".join([first_name.strip().capitalize(), last_name.strip().capitalize()]).strip()
        users[uid] = User(uid, uname, full_name)
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
    servicesUsersLines = re.findall(r"(\w+)\,(\".+\"),(No\sExpiration\sdate|\d{4}-\d{2}-\d{2}|EXPIRED)", files["services-users.csv"])
    for line in servicesUsersLines:
        uname, full_name, expiration_date = line
        full_name = full_name.strip("\"")
        expiration_date = dateSwitcher(expiration_date)
        dn = config.get("general", "dnTemplate") % (full_name, uname)
        if uname in unameUid:
            users[unameUid[uname]].full_name = full_name
            users[unameUid[uname]].expiration_date = expiration_date
            users[unameUid[uname]].certificates.append(dn)
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

    url = source["hostname"] + source["api_get_users"]
    logging.debug("Fetching Ferry users: %s" % url)
    jUsers = json.loads(urllib.request.urlopen(url, context=ferryContext).read().decode())
    for jUser in jUsers:
        users[str(jUser["uid"])] = User(str(jUser["uid"]), jUser["username"], jUser["full_name"], jUser["status"], dateSwitcher(jUser["expiration_date"]))
        unameUid[jUser["username"]] = str(jUser["uid"])

    url = source["hostname"] + source["api_get_certificates"]
    logging.debug("Fetching Ferry certificates: %s" % url)
    jCerts = json.loads(urllib.request.urlopen(url, context=ferryContext).read().decode())
    for jUser in jCerts:
        for jCert in jUser["certificates"]:
            users[unameUid[jUser["username"]]].certificates.append(jCert["dn"])

    url = source["hostname"] + source["api_get_groups"]
    logging.debug("Fetching Ferry groups: %s" % url)
    jGroups = json.loads(urllib.request.urlopen(url, context=ferryContext).read().decode())
    for jGroup in jGroups:
        groups[str(jGroup["gid"])] = Group(str(jGroup["gid"]), jGroup["name"], jGroup["type"])

    url = source["hostname"] + source["api_get_group_members"]
    logging.debug("Fetching Ferry group members: %s" % url)
    jGroups = json.loads(urllib.request.urlopen(url, context=ferryContext).read().decode())
    for jGroup in jGroups:
        if jGroup["members"] != None:
            for jUser in jGroup["members"]:
                groups[str(jGroup["gid"])].members.append(jUser["uid"])

    for accessString in source["compute_resources"].split(";"):
        resource = accessString.split(":")[0]
        url = source["hostname"] + source["api_get_passwd_file"] + "?resourcename=%s" % resource
        logging.debug("Fetching Ferry users access to %s: %s" % (resource, url))
        jPasswd = json.loads(urllib.request.urlopen(url, context=ferryContext).read().decode())
        jPasswd = list(jPasswd.values())[0] # get first affiliation unit
        jPasswd = jPasswd["resources"][resource]
        for access in jPasswd:
            users[access["uid"]].addComputeAccess(accessString)

    return users, groups

# Updates users with data from uid.lis and services-users.csv
def update_users(userdb, ferry):
    changes = False
    for user in userdb.values():
        if user.uid not in ferry.keys():
            if len(user.full_name.split(" ", 1)) > 1:
                firstname = user.full_name.split(" ", 1)[0]
                lastname = user.full_name.split(" ", 1)[1]
            else:
                firstname = user.full_name.split(" ", 1)
                lastname = ""
            params = {
                "uid": user.uid,
                "username": user.uname,
                "firstname": firstname,
                "lastname": lastname,
                "status": str(user.status),
                "expirationdate": user.expiration_date
            }
            if user.expiration_date == "":
                params.__delitem__("expirationdate")
            writeToFerry("api_create_user", params)
            changes = True
        else:
            diff = user.diff(ferry[user.uid])
            if "full_name" in diff or "expiration_date" in diff or "status" in diff:
                params = {"uid": user.uid, "username": user.uname}
                if "full_name" in diff:
                    params["fullname"] = user.full_name
                if "expiration_date" in diff:
                    params["expiration_date"] = user.expiration_date
                if "status" in diff:
                    params["status"] = str(user.status)
                writeToFerry("api_set_user_info", params)
                changes = True
    if not changes:
        logging.info("Users are up to date")

# Updates groups with data from gid.lis
def update_groups(userdb, ferry, users):
    changes = False
    for group in userdb.values():
        if group.gid not in ferry.keys():
            params = {
                "gid": group.gid,
                "groupname": group.name,
                "grouptype": "UnixGroup"
            }
            writeToFerry("api_create_group", params)
            changes = True
        else:
            diff = group.diff(ferry[group.gid])
            if "groupname" in diff:
                logging.warning("Group %s name has changed from %s to %s" % (group.gid, ferry[group.gid].name, group.name))
            if "members" in diff:
                for member in group.members:
                    if member not in ferry[group.gid].members:
                        params = {
                            "username": users[member].uname,
                            "groupname": group.name,
                            "grouptype": "UnixGroup"
                        }
                        writeToFerry("api_add_group_member", params)
                        changes = True
    if not changes:
        logging.info("Groups are up to date")

# Updates users certificates with data from services-users.csv
def update_certificates(userdb, ferry):
    changes = False
    for user in userdb.values():
        diff = user.diff(ferry[user.uid])
        if "certificates" in diff:
            for certificate in user.certificates:
                if certificate not in ferry[user.uid].certificates:
                    if any(c in "," for c in certificate):
                        logging.warning("Certificate \"%s\" contains illegal characters" % certificate)
                        continue
                    params = {
                        "username": user.uname,
                        "unitname": "fermilab",
                        "dn": certificate
                    }
                    writeToFerry("api_add_user_certificate", params)
            changes = True
    if not changes:
        logging.info("Certificates are up to date")

# Updates users access to Fermilab batch resources
def update_compute_access(userdb, ferry):
    changes = False
    for user in userdb.values():
        diff = user.diff(ferry[user.uid])
        if "compute_access" in diff:
            for resource, params in user.compute_access.items():
                if resource not in ferry[user.uid].compute_access:
                    writeToFerry("api_set_compute_access", params)
                    changes = True
    if not changes:
        logging.info("Compute access is up to date")

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

    if config.has_section("log"):
        logging.basicConfig(filename=config.get("log", "dir") + "/" + datetime.datetime.now().strftime("ferry_user_update.log"),
                            level=getattr(logging, config.get("log", "level")),
                            format="[%(asctime)s][%(levelname)s] %(message)s",
                            datefmt="%m/%d/%Y %H:%M:%S")
    else:
        logging.basicConfig(stream=sys.stdout,
                            level=logging.DEBUG,
                            format="[%(asctime)s][%(levelname)s] %(message)s",
                            datefmt="%m/%d/%Y %H:%M:%S")

    ferryContext = ssl.SSLContext(protocol=ssl.PROTOCOL_TLSv1_2)
    ferryContext.verify_mode = ssl.CERT_REQUIRED
    ferryContext.load_cert_chain(config.get("ferry", "cert"), config.get("ferry", "key"))
    ferryContext.load_verify_locations(config.get("ferry", "ca"))

    logging.info("Starting Ferry User Update")

    logging.info("Fetching UserDB files...")
    userdbUsers, userdbGroups = fetch_userdb()

    logging.info("Fetching Ferry data...")
    ferryUsers, ferryGroups = fetch_ferry()

    logging.info("Updating users...")
    update_users(userdbUsers, ferryUsers)

    logging.info("Updating groups...")
    update_groups(userdbGroups, ferryGroups, userdbUsers)

    logging.info("Updating certificates...")
    update_certificates(userdbUsers, ferryUsers)

    logging.info("Updating compute access...")
    update_compute_access(userdbUsers, ferryUsers)