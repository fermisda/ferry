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
from collections import defaultdict
from threading import Thread

class User:
    def __init__(self, uid, uname, full_name = None, status = False, expiration_date = "", banned= False):
        self.uid = uid
        self.uname = uname
        self.full_name = full_name
        self.status = status
        self.expiration_date = expiration_date
        self.gid = ""
        self.banned = banned
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
    #url = source["hostname"] + source[action]
    url = source["hostname"] + action
    if params:
        url += "?" + urllib.parse.urlencode(params)
    logging.debug("downloading from url: %s" % url)
    tmpOut = openURL(url, context=ferryContext)
    if not tmpOut:
        exit(15)
    jOut = json.loads(tmpOut)
    if len(jOut["ferry_error"]) > 0:
        return None
    return jOut["ferry_output"]

def writeToFerry(action, params=None):
    for item in ["username", "groupname"]:
        if params and item in params and params[item] in skipList:
            return
    target = dict(config.items("ferry"))
    url = target["hostname"] + action
    if params:
        url += "?" + urllib.parse.urlencode(params)
    if not opts.dry_run:
        logging.debug(url)
        tmpOut = openURL(url, context=ferryContext)
        if not tmpOut:
            logging.error("Oops! openURL '%s' returns error" % url)
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
        if params:
            params = ", ".join(["%s=%s" % (x, y) for x, y in params.items()])
        else:
            params = ""
        logging.info("action: %s(%s)" % (action, params))
        return True

def dateSwitcher(date):
    if not date or date == "EXPIRED":
        return ""
    if date == "No Expiration date":
        return "2038-01-01"
    return "%s" % date.split("T")[0]

def parseFile(fileName, fileType):
    fileData = open(fileName, 'r').readlines()
    gidRe = r"(\d+)\t(.+)\t(.*)?\n"
    uidRe = r"(\d+)\t(\d+)\t([^\t]+)\t([^\t]+)?\t([^\t]+)\n"
    serRe = r"(\w+)\,(\".+\"),(No\sExpiration\sdate|\d{4}-\d{2}-\d{2}|EXPIRED)\n"
    if fileType == "gid":
        comp = re.compile(gidRe)
    elif fileType == "uid":
        comp = re.compile(uidRe)
    elif fileType == "ser":
        comp = re.compile(serRe)
    else:
        exit(17)
    errMsgs = []
    lines = []
    for (i, l) in enumerate(fileData):
        if l[0] == "#":
            continue
        line = comp.match(l)
        if not line:
            errMsgs.append("RE error parsing line num %s   line: %s" % (i+1, l))
        else:
            lines.append(line.groups())
    return lines, errMsgs

# Downloads necessary files from UserDB to memory
def fetch_userdb(ferryUsers):
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

    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%m")
    for f, url in config.items("userdb"):
        if f not in ["uid.lis", "gid.lis", "services-users.csv"]:
            logging.error("invalid userdb file %s" % f)
            exit(2)
        logging.debug("downloading %s: %s" % (f, url))
        fparts = f.split(".")
        files[f] = "%s/%s-%s.%s" % (cacheDir, fparts[0], timestamp, fparts[1])
        if os.path.isfile(files[f] + ".error"):
            logging.error("bad %s file detected on a previous cycle" % f)
            postToSlack("Update Script Halted!", "Bad %s file detected on a previous cycle." % f)
            exit(5)
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

    logging.debug("reading gid.lis")
    gidLines, errMsgs = parseFile(files["gid.lis"], "gid")
    x = len(gidLines)
    for line in gidLines:
        gid, name, description = line
        gid = gid
        name = name.strip().lower()
        groups[gid] = Group(gid, name)

    logging.debug("reading uid.lis")
    uidLines, errMsgs = parseFile(files["uid.lis"], "uid")
    x = len(uidLines)
    for line in uidLines:
        uid, gid, last_name, first_name, uname = line
        #print ("uid: <%s>, gid: <%s>, last_name: <%s>, first_name: <%s>, uname: <%s>" % (uid, gid, last_name, first_name, uname))
        uid = uid
        gid = gid
        uname = uname.lower().strip()
        if first_name:
            first_name = first_name.strip().capitalize()
        else:
            first_name = ""
        if last_name:
            last_name = last_name.strip().capitalize()
        else:
            last_name = ""
        full_name = " ".join([first_name, last_name]).strip()
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
            #logging.debug("duplicated uname: %s", uname)     # DEBUG
            for uid in unameUid[uname][:-1]:
                # logging.debug("delete uid %s", uid)
                users.__delitem__(uid)
                for gid in groups.keys():
                    if uid in groups[gid].members:
                            groups[gid].members.remove(uid)
            #logging.debug("delete uid %s", ",".join(uid for uid in unameUid[uname][:-1]))
            unameUid[uname] = unameUid[uname][-1]
            # unameToDelete.append(uname)
    # for uname in unameToDelete:
    #     logging.debug("delete uname %s", uname)
    #     unameUid.__delitem__(uname)

    logging.debug("reading services-users.csv")
    fileText = open(files["services-users.csv"], "r").read()
    servicesUsersLines = re.findall(r"(\w+)\,(\".+\"),(No\sExpiration\sdate|\d{4}-\d{2}-\d{2}|EXPIRED)", fileText)
    # Check number of users
    complete = True
    re_error = None
    complete = complete and bool(re.search(r"# SERVICES active users list made on  [A-z]{3} [A-z]{3} \d{2} \d{2}:\d{2} \d{4}", fileText))
    if not complete:
        re_error = 1
    else:
        loadedUsers =                re.search(r"# (\d+) username loaded from Active Directory", fileText)
        complete = complete and bool(re.search(r"# \d+ users output to list", fileText))
        if not complete:
            re_error = 2
        else:
            complete = complete and bool(re.search(r"# SERVICES active users list completed on  [A-z]{3} [A-z]{3} \d{2} \d{2}:\d{2} \d{4}", fileText))
            if not complete:
                re_error = 3
            else:
                complete = complete and bool(loadedUsers)
                if not complete:
                    re_error = 4
    if not complete:
        logging.error("file services-users.csv seem truncated - RE Error %s", re_error)
        postToSlack("Update Script Halted!", "File services-users.csv seem truncated")
        os.rename(files["services-users.csv"], files["services-users.csv"] + ".error")
        os.rename(files["services-users.csv"] + ".cache", files[f])
        exit(6)
    ucnt = len(servicesUsersLines)
    if int(loadedUsers.group(1)) != ucnt:
        logging.error("file services-users.csv is missing users loadedUsers.group(1): %s - len(servicesUsersLines): %s",
                      loadedUsers.group(1), ucnt)
        postToSlack("Update Script Halted!", "File services-users.csv seem truncated - missing users")
        os.rename(files["services-users.csv"], files["services-users.csv"] + ".error")
        os.rename(files["services-users.csv"] + ".cache", files[f])
        exit(7)
    # Test to be sure the file we were given actually has users in it.  Processing an intact but empty services file will
    # cause everybody's status to be set to false - and drop their UUID from LDAP.  (Yes, it has happened.)
    changeAmt = (ferryUsers * float(config.get("general", "cache_max_diff")))
    if  changeAmt  > ucnt:
        msg = "services-users.csv appears empty or short! Procceding would exceed max allowed changes. Changes: " + str(changeAmt)
        logging.error(msg)
        postToSlack("Update Script Halted!", msg)
        os.rename(files["services-users.csv"], files["services-users.csv"] + ".error")
        os.rename(files["services-users.csv"] + ".cache", files[f])
        exit(16)
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
            for resource in config.get("ferry", "compute_resources").split(";"):
                users[unameUid[uname]].addComputeAccess(resource)
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
    ferryOut = defaultdict(str)
    def work(action, params = None, id = None):
        if not id:
            id = action
        ferryOut[id] = readFromFerry(action, params)

    if opts.single_thread:
        work("getAllUsers")
        work("getAllUsersCertificateDNs", {"unitname" : "fermilab"})
        work("getAllGroups")
        work("getAllGroupsMembers")
        work("getAllUsersFQANs")
    else:
        threads.append(Thread(target=work, args=["getAllUsers"]))
        threads.append(Thread(target=work, args=["getAllUsersCertificateDNs", {"unitname" : "fermilab"}]))
        threads.append(Thread(target=work, args=["getAllGroups"]))
        threads.append(Thread(target=work, args=["getAllGroupsMembers"]))
        threads.append(Thread(target=work, args=["getAllUsersFQANs"]))

        for accessString in source["compute_resources"].split(";"):
            resource = accessString.split(":")[0]
            threads.append(Thread(target=work, args=["getPasswdFile", {"resourcename": resource}, resource]))

        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

    logging.debug("reading ferry users")
    if not ferryOut["getAllUsers"]:
            exit(9)
    for jUser in ferryOut["getAllUsers"]:
        users[str(jUser["uid"])] = User(str(jUser["uid"]), jUser["username"], jUser["fullname"], jUser["status"], dateSwitcher(jUser["expirationdate"]), jUser["banned"])
        unameUid[jUser["username"]] = str(jUser["uid"])

    logging.debug("reading ferry certificates")
    if not ferryOut["getAllUsersCertificateDNs"]:
            exit(10)
    for jUser in ferryOut["getAllUsersCertificateDNs"]:
        for jCert in jUser["certificates"]:
            users[unameUid[jUser["username"]]].certificates.append(jCert["dn"])

    logging.debug("reading ferry groups")
    if not ferryOut["getAllGroups"]:
            exit(11)
    for jGroup in ferryOut["getAllGroups"]:
        groups[str(jGroup["gid"])] = Group(str(jGroup["gid"]), jGroup["groupname"], jGroup["grouptype"])

    logging.debug("reading ferry group members")
    if not ferryOut["getAllGroupsMembers"]:
            exit(12)
    for jGroup in ferryOut["getAllGroupsMembers"]:
        if jGroup["members"] != None:
            for jUser in jGroup["members"]:
                if not jGroup["gid"]:
                    continue
                groups[str(jGroup["gid"])].members.append(str(jUser["uid"]))

    if not ferryOut["getAllUsersFQANs"]:
            exit(13)
    for uname, items in ferryOut["getAllUsersFQANs"].items():
        for item in items:
            users[unameUid[uname]].fqans.append((item["fqan"], item["unitname"]))

    for accessString in source["compute_resources"].split(";"):
        resource = accessString.split(":")[0]
        logging.debug("reading ferry users access to %s" % resource)
        jPasswd = ferryOut[resource]
        if not jPasswd:
            logging.debug("Resource %s not found." % resource)
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
    except urllib.error.URLError as e:
        logging.error("Failed to access remote server: %s   error: %s", url, e.reason)
        return None
    except Exception as e:
        logging.error("Failed to access remove server: %s - general exception: %s", url, e)


# Updates users with data from uid.lis and services-users.csv
def update_users():
    changes = False
    for user in userdbUsers.values():
        # logging.debug("user: %s", user)     # DEBUG
        if user.uid not in ferryUsers.keys():
            if user.gid not in ferryGroups.keys():
                params = {
                    "gid": user.gid,
                    "groupname": userdbGroups[user.gid].name,
                    "grouptype": userdbGroups[user.gid].type
                }
                if writeToFerry("createGroup", params):
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
            if writeToFerry("createUser", params):
                ferryUsers[user.uid] = User(user.uid, user.uname, user.full_name, user.status, user.expiration_date)
                ferryGroups[user.gid].members.append(user.uid)
            changes = True
        else:
            diff = user.diff(ferryUsers[user.uid])
            if "uname" in diff:
                msg = "User %s username has changed from %s to %s" % (user.uid, ferryUsers[user.uid].uname, user.uname)
                logging.warning(msg)
                postToSlack("Username Has Changed", msg)
                continue
            if "full_name" in diff or "expiration_date" in diff or "status" in diff:
                auxUser = ferryUsers[user.uid]
                params = {"username": user.uname}
                if "full_name" in diff:
                    msg = "User %s full_name has changed from %s to %s" % (user.uid, ferryUsers[user.uid].full_name, user.full_name)
                    logging.warning(msg)
                    params["fullname"] = user.full_name
                    auxUser.full_name = user.full_name
                if "expiration_date" in diff:
                    msg = "User %s expiration_date has changed from %s to %s" % (user.uid, ferryUsers[user.uid].expiration_date, user.expiration_date)
                    logging.warning(msg)
                    if user.expiration_date == "":
                        params["expirationdate"] = "null"
                    else:
                        params["expirationdate"] = user.expiration_date
                    auxUser.expiration_date = user.expiration_date
                if "status" in diff:
                    msg = "User %s status has changed from %s to %s" % (user.uid, ferryUsers[user.uid].status, str(user.status))
                    logging.warning(msg)
                    params["status"] = str(user.status)
                    auxUser.status = user.status
                # Never change anything if the user has been banned as it will fail and you will ping #ferryalerts
                # every 30 min until the user is set to inactive in services-users.csv.
                if ferryUsers[user.uid].banned != True and writeToFerry("setUserInfo", params):
                    ferryUsers[user.uid] = auxUser
                changes = True
    if not changes:
        logging.info("Users are up to date")

# Updates users with data from uid.lis and services-users.csv
def cleanup_users():
    changes = False
    idx = 0
    logging.debug("ferryUsers len = %d:", len(ferryUsers))
    logging.debug("userdbUsers len = %d:", len(userdbUsers))
    for uid in ferryUsers:
        if uid not in userdbUsers:
            # logging.debug("uid type: %s", type(uid))
            changes = True
            idx += 1
            logging.debug("%d: user not in UserDb: '%s, %s, %s'",
                idx,
                uid, ferryUsers[uid].uname, ferryUsers[uid].full_name
            )     # DEBUG
            # logging.debug("similar: %s", [l for l in userdbUsers if ferryUsers[uid].uname in l[-1].lower()])
            params = {
                "uid": uid,
            }
            if writeToFerry("dropUser", params):
                logging.info("User '%s, %s, %s' %s dropped",
                             uid,
                             ferryUsers[uid].uname,
                             ferryUsers[uid].full_name,
                             "will be" if opts.dry_run else "",
                )
                pass
            else:
                logging.error("cannot drop user '%s, %s, %s'", uid, ferryUsers[uid].uname, ferryUsers[uid].full_name)
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
            if writeToFerry("createGroup", params):
                ferryGroups[group.gid] = Group(group.gid, group.name)
            changes = True

        diff = group.diff(ferryGroups[group.gid])
        if "name" in diff:
            msg = "Group %s name has changed from %s to %s" % (group.gid, ferryGroups[group.gid].name, group.name)
            logging.warning(msg)
            postToSlack("Group Name Has Changed", msg)
            continue
        if "members" in diff:
            for member in group.members:
                if member not in ferryGroups[group.gid].members:
                    params = {
                        "username": userdbUsers[member].uname,
                        "groupname": group.name,
                        "grouptype": group.type
                    }
                    if writeToFerry("addUserToGroup", params):
                        ferryGroups[group.gid].members.append(member)
                    changes = True
    if not changes:
        logging.info("Groups are up to date")

# Updates users certificates with data from services-users.csv
# It makes sure each user has a personal cert tied to their account.  If not it adds it.  It only checks for the
# user's own cert, it does not deal with all the others which may be tied to the user. (services, hosts)
def update_certificates():
    changes = False
    for user in userdbUsers.values():
        if user.uid not in ferryUsers:
            continue
        diff = user.diff(ferryUsers[user.uid])
        if "certificates" in diff:
            for certificate in user.certificates:
                if certificate not in ferryUsers[user.uid].certificates:
                    # if any(c in "," for c in certificate):
                    if "," in certificate:
                        logging.warning("Certificate \"%s\" contains illegal characters" % certificate)
                        continue
                    jUnits = readFromFerry("getMemberAffiliations", {"username": user.uname})
                    if not jUnits:
                        logging.debug("could not fetch affiliation units for %s" % user.uname)
                        jUnits = []
                    userInFermilab = False
                    for jUnit in jUnits:
                        if jUnit['unitname'] == 'fermilab':
                            userInFermilab = True
                            break
                    if userInFermilab == False:
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
                        if writeToFerry("addCertificateDNToUser", params):
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
                    if writeToFerry("setUserExperimentFQAN", params):
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
                    if writeToFerry("setUserAccessToComputeResource", params):
                        ferryUsers[user.uid].compute_access[resource] = params
                    changes = True
    if not changes:
        logging.info("Compute access is up to date")

# Cleans expired storage quotas
def clean_storage_quotas():
    if writeToFerry("cleanStorageQuotas"):
        logging.info("Done")

# Cleans expired condor quotas
def clean_condor_quotas():
    if writeToFerry("cleanCondorQuotas"):
        logging.info("Done")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = "Script to update Ferry with data from UserDB")
    parser.add_argument("-c", "--config", metavar = "PATH", action = "store", help = "path to configuration file")
    parser.add_argument("-d", "--dry-run", action = "store_true", help = "runs the script without touching the database")
    parser.add_argument("-i", "--ip-address", action = "store_true", help = "Validate by IP Address, not cert.")
    parser.add_argument("-s", "--single-thread", action = "store_true", help=" Run FERRY data requests in single threaded mode (helps when using a debugger).")
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
    if not opts.ip_address:
        # Skip these ferryContext lines out if you want to verify by IP address.
        ferryContext.verify_mode = ssl.CERT_REQUIRED
        #ferryContext.verify_mode = ssl.CERT_NONE   # Don't require a cert, clearly devel only.
        ferryContext.load_cert_chain(config.get("ferry", "cert"), config.get("ferry", "key"))
        ferryContext.load_verify_locations(capath=config.get("ferry", "ca"))

    skipList = []

    logging.info("Starting Ferry User Update")

    logging.info("Fetching Ferry data...")
    ferryUsers, ferryGroups = fetch_ferry()

    logging.info("Fetching UserDB files...")
    userdbUsers, userdbGroups = fetch_userdb(len(ferryUsers))
    if len(userdbUsers) < 20000:
        msg = "uid.lis file appears truncated, len(userdbUsers): %s" % len(userdbUsers)
        logging.error(msg)
        postToSlack("Update Script Halted!", msg)
        exit(15)
    if len(userdbGroups) < 1000:
        msg = "gid.lis file appears truncated, len(userdbUsers): %s" % len(userdbUsers)
        postToSlack("Update Script Halted!", msg)
        logging.error(msg)
        exit(16)

    logging.info("Cleanup users...")
    cleanup_users()

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
