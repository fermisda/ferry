#!/usr/bin/env python

""" This script is uploading data from various source to FERRY database
"""
import sys
from Configuration import Configuration
from MySQLUtils import MySQLUtils
from User import User, Certificate, ComputeAccess
from Resource import *
import xml.etree.ElementTree as ET
import psycopg2 as pg
import psycopg2.extras
import fetchcas as ca
import re


def read_uid(fname):
    """
    Reads data from uid.lis file obtained from userdb
    Args:
        fname: file name

    Returns: {uname:User,}

    """
    fd = open(fname)
    usrs = {}
    for line in fd.readlines():
        try:
            if not len(line[:-1])  or line.startswith("<"):
                continue
            tmp = line[:-1].split("\t\t")
            if not usrs.__contains__(tmp[4].strip().lower()):
                usrs[tmp[4].strip().lower()] = User(tmp[0].strip(), tmp[3].strip().capitalize() + " " +
                                                     tmp[2].strip().capitalize(), tmp[4].strip().lower())
            gname = list(gids.keys())[list(gids.values()).index(tmp[1].strip().lower())]
            usrs[tmp[4].strip().lower()].add_group(gname, (tmp[1].strip().lower()))
        except:
            print("Failed ", line, file=sys.stderr)
    return usrs

def read_gid(fname):
    """
    Reads data from gid.lis file obtained from userdb
    Args:
        fname: file name

    Returns: {gname:gid,}

    """
    fd = open(fname)
    groupids = {}
    for line in fd.readlines():
        if not len(line[:-1]) or line.startswith("<"):
            continue
        try:
            tmp = line[:-1].strip("\t").split("\t")
            groupids[tmp[1].strip().lower()] = tmp[0].strip()

        except:
            print("Failed reading group.lis (%s) file. Failed: %s" % (fname, line), file=sys.stderr)
    return groupids


def read_services_users(fname, users):
    """
    Reads data for service_user.csv with k5login info
    Args:
        fname: file name
        users: {uname:User,}
    Returns:

    """
    fd = open(fname)
    for line in fd.readlines():
        if line.startswith("#"):
            continue
        try:
            tmp = line[:-1].split(",")
            if users.__contains__(tmp[0]):
                if tmp[2] != "EXPIRED":
                    users[tmp[0]].set_status(True)
                users[tmp[0]].set_full_name(tmp[1].strip('"'))
                users[tmp[0]].set_expiration_date(tmp[2].strip())
                users[tmp[0]].is_k5login = True
        except:
            print("csv Failed ", line, file=sys.stderr)


def get_vos(config, vn, vurl, gids, vomss):
    """
    Read data from VOMS databases
    Args:
        config:
        vn:
        vname:
        vurl:
        gids:
    Returns:

    """
    mysql_client_cfg = MySQLUtils.createClientConfig("voms_db_%s" % (vn,), config)
    connect_str = MySQLUtils.getDbConnection("voms_db_%s" % (vn,), mysql_client_cfg, config)
    command = "select dn from groups"
    groups, return_code = MySQLUtils.RunQuery(command, connect_str, False)
    if return_code:
        print("Failed to select dn from table group from voms_db_%s %s" % (vn, groups), file=sys.stderr)
    for g in groups:
        exp = g[1:].strip().split('/')
        if exp[0] == 'fermilab':
            if len(exp) == 1:
                name = exp[0]
            else:
                name = exp[1]
        else:
            name = exp[0]
        try:
            if not vomss.__contains__(name):
                vomss[name] = VOMS(vurl, vn, name)
                if gids.__contains__(name):
                    vomss[name].add_voms_unix_group(name,gids[name])
            else:
                if vurl not in vomss[name].url:
                    vomss[name].add_voms_url(vurl)
        except:
            print("group is not defined ", name, file=sys.stderr)


def assign_vos(config, vn, vurl, rls, usrs, gums_map, collaborations):
    """
    From VOMS database tries to get information about each user, certificate and group and role affiliation
    Args:
        config:
        vn: VO name
        vurl: VO url
        rls:
        usrs:
        gums_map:

    Returns:

    """
    CADir = config._sections["path"]["cadir"]
    CAs = ca.fetchCAs(CADir)

    mysql_client_cfg = MySQLUtils.createClientConfig("voms_db_%s" % (vn,), config)
    connect_str = MySQLUtils.getDbConnection("voms_db_%s" % (vn,), mysql_client_cfg, config)

    # map all vo members
    members_list = {}
    command = "select unames.*, dns.subject_string, dns.ca from \
                          (select c.usr_id, c.subject_string, ca.subject_string as ca from \
                            certificate as c join ca on ca.cid = c.ca_id order by c.usr_id) as dns \
                left join (select distinct usr_id, substr(subject_string, position('UID:' in subject_string) + 4) from \
                            certificate where subject_string like '%UID:%') as unames \
                on dns.usr_id = unames.usr_id where unames.usr_id is not null order by 2;"
    table, return_code = MySQLUtils.RunQuery(command, connect_str)
    if return_code:
        print("Failed to extract information from VOMS %s" % (vn), file=sys.stderr)
        return
    else:
        for line in table:
            line = line.split("\t")
            if line[1] not in members_list:
                members_list[line[1]] = []
            members_list[line[1]].append((line[0], line[2], line[3]))

    # generates a map user: affiliations
    affiliation_list = {}
    command = "select distinct m.userid,g.dn from m m, groups  g where g.gid=m.gid;"
    table, return_code = MySQLUtils.RunQuery(command, connect_str)
    if return_code:
        print("Failed to extract information from VOMS %s m and g tables" % (vn), file=sys.stderr)
        return
    else:
        for line in table:
            userid, group = line.split("\t", 1)
            if userid not in affiliation_list:
                affiliation_list[userid] = []
            affiliation_list[userid].append(group)

    # generates a map user: roles
    group_role_list = {}
    command = "select m.userid,g.dn,r.role from m m, roles r, groups  g where  r.rid=m.rid and g.gid=m.gid and r.role!=\'VO-Admin\';"
    table, return_code = MySQLUtils.RunQuery(command, connect_str)
    if return_code:
        print("Failed to extract information from VOMS %s g and g tables" % (vn), file=sys.stderr)
        return
    else:
        for line in table:
            userid, role = line.split("\t", 1)
            if userid not in group_role_list:
                group_role_list[userid] = []
            group_role_list[userid].append(role)

    total = 0

    # for all users in user-services.csv
    for uname, user in usrs.items():
        #if uname!='kherner':
        #    continue
        # do we want to have expired users in VOMS?
        total += 1
        if not user.is_k5login:
            continue

        # user is not a member of VO
        if uname not in members_list:
            continue
        else:
            members = members_list[uname]

        mids = []

        # stores all voms uids
        for member in members:
            if member[0] not in mids:
                mids.append(member[0].strip())

        for mid in mids:
            # gets all groups and roles that this user is a member
            affiliation = affiliation_list[mid]
            if mid in group_role_list:
                affiliation += group_role_list[mid]

            for aff in affiliation:
                tmp = aff.split("\t")
                subgroup = tmp[0].strip()[1:]
                groups = subgroup.split('/')
                if len(groups) > 1 and groups[0] == 'fermilab':
                    group = "".join(groups[1:])
                    url = "%s/%s" % (vurl, group)
                else:
                    group = groups[0]
                    url = vurl
                user.add_to_vo(group, url)
                for member in members:
                    if member[0] == mid:
                        subject = member[1].strip()
                        issuer = member[2].strip()
                        for cu in collaborations:
                            if cu.name == group or cu.alt_name == group:
                                CA = ca.matchCA(CAs, subject)
                                if CA:
                                    index = collaborations.index(cu) +1
                                    user.add_cert(Certificate(index,subject, issuer))

                if len(tmp) > 1:
                    role = tmp[1].strip()
                else:
                    role = None

                for _, umap in gums_map.items():

                    if "/%s" % (subgroup,) == umap.group and role == umap.role:
                        if not umap.uname:
                            account_name = user.uname
                        else:
                            account_name = umap.uname
                        new_umap = VOUserGroup(umap.user_group, umap.server, umap.group, umap.role)
                        new_umap.uname = account_name
                        new_umap.gid = umap.gid
                        if not umap.gid:
                            print("disaster", new_umap.__dict__)
                            sys.exit(1)
                        if umap.gid not in user.groups:
                            gname = list(gids.keys())[list(gids.values()).index(umap.gid)]
                            user.add_group(gname, umap.gid)
                        if len(user.certificates) > 0:
                            user.add_to_vo_role(group, url, new_umap)

                        break
                if role not in rls:
                    rls.append(role)


def read_gums_config(config, usrs, grps):
    """

    Args:
        config:
        vname:

    Returns:

    """
    gums_fn = config.config.get("gums", "gums_config")
    tree = ET.parse(gums_fn)
    gums_mapping = {}
    vo_groups={}
    # key vomsUserGroup
    root = tree.getroot()
    for child in root:
        if child.tag == "userGroups":
            for element in child.getchildren():
                voms_user_group = element.attrib.get('name')   # this is an unique name of VOMSUserGroup
                voms_server = element.attrib.get('vomsServer')
                if voms_server not in ["fermilab", "dune", "des", "cdf", "dzero"]:
                    continue
                vo_group = element.attrib.get('voGroup')
                if not vo_group:
                    continue
                role = element.attrib.get('role')
                # print voms_user_group,vo_group, role
                if not vo_groups.__contains__(vo_group):
                    vo_groups[vo_group] = []
                vo_groups[vo_group].append(role)
                gums_mapping[voms_user_group] = VOUserGroup(voms_user_group, voms_server, vo_group, role)


    #for key,vug in vo_groups.items():
    #    if not None in vug:
    #        print key, vug, "needs role None"
    #        # it means that FQAN does not have a None Role, we need to create a new Group
    keysToRemove = []
    for key, gmap in gums_mapping.items():
        for child in root:
            if child.tag == "groupToAccountMappings":
                for element in child.getchildren():
                    if element.attrib.get('userGroups') == key:
                        #print "found UserGroups",key,element.attrib.get('accountMappers')
                        gmap.account_mappers = element.attrib.get('accountMappers')
                break

        for child in root:
            if child.tag == "accountMappers":
                for element in child.getchildren():
                    if element.attrib.get('name') == gmap.account_mappers:
                        if element.tag == 'accountPoolMapper':
                            print("Ignoring voms user group %s as it maps to a pool account" % key)
                            keysToRemove.append(key)
                            break
                        #print "found accountMappers", gmap.account_mappers,element.attrib.get('groupName')
                        gid = element.attrib.get('groupName')
                        if gid not in grps.values():
                            print("Group gid %s for %s id not in gid.lis" % (gid,key), file=sys.stderr)
                        else:
                            gmap.gid = gid
                        uname = element.attrib.get('accountName')

                        if uname and uname not in users.keys():
                            print("User uname %s for %s id not in uid.lis" % (uname,key), file=sys.stderr)
                        else:
                            gmap.uname = uname
                        break
    for key in keysToRemove:
        gums_mapping.__delitem__(key)

    return gums_mapping

def read_vulcan_user_group(config, users):
    """
    Reads Vulcan user related info from Vulcan db
    Args:
        config:
        users:

    Returns:

    """
    config = config.config._sections["vulcan"]
    conn_string = "host='%s' dbname='%s' user='%s' password='%s'" % \
                  (config["hostname"], config["database"], config["username"], config["password"])
    conn = pg.connect(conn_string)
    cursor = conn.cursor(cursor_factory=pg.extras.DictCursor)

    # fetch user_group from vulcan
    cursor.execute("select u.*, a.auth_string from user_group_t1 as u left join auth_tokens_t1 as a on u.userid = a.userid where a.auth_method = 'UNIX'")
    rows = cursor.fetchall()

    validGroups = {}
    for line in open(config["validgroups"], "r").readlines():
        validGroups[line.split()[0]] = line.split()[1]

    for row in rows:
        if row["auth_string"] in users:
            if str(row["groupid"]) in validGroups:
                users[row["auth_string"]].add_group(validGroups[str(row["groupid"])], row["groupid"])

    # ensure all Vulcan users are in us_cms (5063) group in Ferry
    cursor.execute("select u.userid, a.auth_string from users_t1 as u left join auth_tokens_t1 as a on u.userid = a.userid where a.auth_method = 'UNIX'")
    rows = cursor.fetchall()

    for row in rows:
        if row["auth_string"] in users:
            users[row["auth_string"]].add_group('us_cms','5063')
            users[row["auth_string"]].add_to_vo('cms', 'https://voms2.cern.ch:8443/voms/cms')


    # fetch group leadership from vulcan
    cursor.execute("select l.*, a.auth_string from leader_group_t1 as l left join auth_tokens_t1 as a on l.userid = a.userid where a.auth_method = 'UNIX'")
    rows = cursor.fetchall()

    for row in rows:
        if row["auth_string"] in users:
            if str(row["groupid"]) in users[row["auth_string"]].groups:
                users[row["auth_string"]].set_leader(str(row["groupid"]))
    return validGroups

def read_vulcan_certificates(config, users, vomss):
    """
    Reads Vulcan cerificate related info from Vulcan db

    Args:
        config:
        users:
        vomss:

    Returns:

    """
    # reads valid certificates from gums
    mysql_client_cfg = MySQLUtils.createClientConfig("gums", config.config)
    connect_str = MySQLUtils.getDbConnection("gums", mysql_client_cfg, config.config)
    command = "select distinct USERS.DN from USERS, MAPPING \
               where USERS.DN=MAPPING.DN and USERS.GROUP_NAME like 'cmsuser-null' \
               and MAPPING.ACCOUNT not like 'uscms%' and MAPPING.MAP='uscmsPool' \
               order by MAPPING.ACCOUNT"
    validDNs, return_code = MySQLUtils.RunQuery(command, connect_str, False)
    if return_code:
        print("Failed to read valid CMS certificates from GUMS", file=sys.stderr)

    cfg = config.config._sections["vulcan"]
    conn_string = "host='%s' dbname='%s' user='%s' password='%s'" % \
                  (cfg["hostname"], cfg["database"], cfg["username"], cfg["password"])
    conn = pg.connect(conn_string)
    cursor = conn.cursor(cursor_factory=pg.extras.DictCursor)

    url = cfg["cmsurl"]
    vo = cfg["cmsvo"]

    CADir = config.config._sections["path"]["cadir"]
    CAs = ca.fetchCAs(CADir)

    # add cms voms information
    vomss['cms'] = VOMS(url, vo, vo)

    # fetch certificates from vulcan
    cursor.execute("select u.auth_string as uname, c.* from auth_tokens_t1 as c left join auth_tokens_t1 as u on c.userid = u.userid \
                    where c.auth_method = 'X509' and u.auth_method = 'UNIX' \
                    and c.auth_string not like '%DC=doegrids%' \
                    and c.auth_string not like '%DC=fnal%' \
                    and c.auth_string not like '%DC=DigiCert%' \
                    and c.auth_string not like '%O=BEGRID%' \
                    and c.auth_string not like '%O=UNIANDES%'")
    rows = cursor.fetchall()

    for row in rows:
        if row["uname"] in users:
            CA = ca.matchCA(CAs, row["auth_string"])
            if CA:
                CA = CA[0]
                if row["auth_string"] in validDNs:
                    users[row["uname"]].add_cert(Certificate(len(vomss), row["auth_string"], CA["subjectdn"]))
                cernUser = re.findall(r"/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=(\w*)/CN=\d+/CN=[A-z\s]+", row["auth_string"])
                if len(cernUser) > 0:
                    users[row["uname"]].add_external_affiliation("cern_username", cernUser[0])

def build_collaborations(vomss, nis, groups):
    """
    Build collaboration unit
    Args:
        vomss:
        nis:
        groups:

    Returns:

    """
    collaborations = []
    # There are VOs that are collaboration units that have NIS domain
    # Let's find them
    for vo in vomss.values():
        #  We should ignore top level fermilab vo - it doesn't have NIS domain

        if vo.name == "fermilab":
            collaborations.append(vo)
            vo.set_id(len(collaborations))
            # now we need to figure out the group for this collaborative unit
            # and hope that the name is the gname
            if vo.name in groups.keys():
                collaborations[-1].groups = {vo.name:groups[vo.name]}
            continue
        found = False
        for domain,nis in nis_structure.items():
            if domain == vo.name or nis.alternative_name == vo.name:
                # build a collaboration unit from all the VO
                if nis.alternative_name:
                    vo.set_alt_name(domain)
                collaborations.append(vo)
                vo.set_id(len(collaborations))
                collaborations[-1].groups = nis.groups
                found = True
                break
        if not found:
            collaborations.append(vo)
            vo.set_id(len(collaborations))
            if vo.name in groups.keys():
                collaborations[-1].groups = {vo.name:groups[vo.name]}

    # we need to include in collaborations all NIS domains that don't have a corresponding VO
    for domain,nis in nis_structure.items():
        found = False
        for cu in collaborations:
            if cu.name == "fermilab":
                continue
            if domain == cu.name or domain == cu.alt_name:
                found = True
                break
        if not found:
            print("NIS domain doesn't exist for %s" % (vo.name), file=sys.stderr)
            c = CollaborationUnit(domain)
            c.groups = nis.groups
            collaborations.append(c)
            c.set_id(len(collaborations))

    return collaborations

def read_nis(primary_groups, dir_path, exclude_list, altnames, users, groups):
    """
    Read NIS domain and build passwd and group
    Args:
        dir_path:
        exclude_list:
        altnames:
        users:
        groups:
        cms_groups:

    Returns:

    """
    import os
    import ast

    nis = {}
    alt_names = ast.literal_eval(altnames)

    for dir in os.listdir(dir_path):
        if dir in exclude_list:
            # print("Skipping %s" % (dir,), file=sys.stderr)
            continue
        nis[dir] = ComputeResource(dir, dir)

        # dcarber:KERBEROS:52790:9467:Daniel Carber: / nashome / d / dcarber: / bin / bash
        lines = open("%s/%s/passwd" % (dir_path,dir)).readlines()
        for l  in lines:
            if l.startswith("#") or not len(l.strip()):
                continue

            tmp = l[:-1].split(":")
            uname = tmp[0]
            uid = tmp[2]
            gid = tmp[3]
            home_dir = tmp[5]
            shell = tmp[6]
            if uname not in users.keys():
                print("Domain: %s User %s in not in userdb!" % (dir,uname,), file=sys.stderr)
                continue
            if uid != users[uname].uid:
                for u in users.values():
                    if u.uid == uid:
                        print("Domain: %s user %s, %s has different uid (%s) in userdb! This userdb " \
                                             "uid (%s) is mapped to %s." % (dir, uname,uid, users[uname].uid, uid, u.uname), file=sys.stderr)
                        print("Assume that uid is correct, using %s" % (users[uname].uid,), file=sys.stderr)

            if gid not in groups.values():
                print("Domain: %s group %s doesn\'t exist in userdb!" % (dir,gid,), file=sys.stderr)
                continue
            users[uname].compute_access[dir] = ComputeAccess(dir, gid, home_dir, shell)
            if gid not in users[uname].groups:
                users[uname].add_group(list(groups.keys())[list(groups.values()).index(gid)], gid, False)
            nis[dir].users[uname] = users[uname]
            nis[dir].groups[list(groups.keys())[list(groups.values()).index(gid)]] = gid

            if dir in alt_names.keys():
                nis[dir].alternative_name = alt_names[dir]
        # numix:x:9276:zwaska,tjyang,tianxc,
        lines = open("%s/%s/group" % (dir_path,dir)).readlines()
        for l  in lines:
            if l.startswith("#") or not len(l.strip()) :
                    continue
            tmp = l[:-1].split(":")
            gname = tmp[0]
            gid = tmp[2]

            user_list = []
            if len(tmp) >= 4 and len(tmp[3].strip()) > 0:
                user_list = tmp[3].split(",")

            if gid not in groups.values():
                print("Domain: %s group %s %s from group file doesn\'t exist  in userdb!" % \
                                     (dir,gid,gname), file=sys.stderr)
                continue
            if gname not in groups.keys():
                print("Domain: %s group name %s, %s from group file doesn\'t exist  in userdb!" % \
                                     (dir,gname,gid), file=sys.stderr)
                continue
            if gid != groups[gname]:
                print("Domain: %s group %s, %s from group file have different gid (%s) in userdb!" % \
                                     (dir,gname,gid,groups[gname]), file=sys.stderr)
                continue
            for uname in user_list:
                if uname not in users.keys():
                    print("Domain: %s User %s in group file in not in userdb!" % (dir,uname,), file=sys.stderr)
                    continue
                if dir in users[uname].compute_access:
                    users[uname].compute_access[dir].add_secondary_group(gid)
                    if gid not in users[uname].groups:
                        users[uname].add_group(list(groups.keys())[list(groups.values()).index(gid)], gid, False)
                else:
                    print("Domain: %s User %s in group file but not in passwd!" % (dir,uname,), file=sys.stderr)

            nis[dir].groups[gname]=gid

    lines = open(primary_groups).read()
    lines = re.findall(r"nis::def_domain[{\s}]+'([\w-]+)':\n\s+domaingid\s+=>\s+(\d+)", lines)
    for dir, gid in lines:
        if dir in nis:
            nis[dir].primary_gid.append(gid)

    return nis

def add_compute_resources(users, groups, gums):
    computeResources = {}

    # Add fermi_workers resource
    name = "fermi_workers"
    group = "fnalgrid"
    defHome = "/home"
    defShell = "/sbin/nologin"
    computeResources[name] = ComputeResource(name, None, "Batch", defHome, defShell)

    gumsUsers = []
    for v in gums:
        gumsUsers.append(gums[v].account_mappers)

    for uname in users:
        if users[uname].is_k5login or uname in gumsUsers:
            computeResources[name].users[uname] = users[uname]
            if groups[group] not in users[uname].groups:
                users[uname].add_group(group, groups[group], False)
            users[uname].compute_access[name] = ComputeAccess(name, groups[group], defHome + "/" + uname, defShell)

    return computeResources

def read_compute_batch(config, users, groups, gums):
    """
    read batch resources quotas and priorities
    Args:
        config:

    Returns:

    """

    batch_structure = {}

    # FermiGrid
    name = "fermigrid"
    group = "fnalgrid"
    defHome = "/home"
    defShell = "/sbin/nologin"

    gumsUsers = []
    for v in gums:
        gumsUsers.append(gums[v].account_mappers)

    batch_structure[name] = ComputeResource(name, None, "Batch", None, None)
    for uname in users:
        if users[uname].is_k5login or uname in gumsUsers:
            batch_structure[name].users[uname] = users[uname]
            if groups[group] not in users[uname].groups:
                users[uname].add_group(group, groups[group], False)
            users[uname].compute_access[name] = ComputeAccess(name, groups[group], defHome + "/" + uname, defShell)

    quotas = open(config.config._sections[name]["quotas"]).readlines()
    for quota in quotas:
        qname, value = quota.strip().split(" = ")
        experiment = re.findall(r"GROUP_QUOTA_(group|DYNAMIC_group)_(\w+)\.?(\w+)?", qname)[0][1]
        if qname.__contains__("DYNAMIC"):
            quotaType = "dynamic"
        else:
            quotaType = "static"
        batch_structure[name].batch.append(ComputeBatch(qname, value, quotaType, experiment))

    return batch_structure


def read_vulcan_compute_resources(config, nis, users, groups, cms_groups):
    """
    read vulcan compute resources from a list and add to nis list
    Args:
        config:
        nis:
        groups:
        cms_groups:

    Returns:

    """
    cfg = config.config._sections["vulcan"]
    conn_string = "host='%s' dbname='%s' user='%s' password='%s'" % \
                  (cfg["hostname"], cfg["database"], cfg["username"], cfg["password"])
    conn = pg.connect(conn_string)
    cursor = conn.cursor(cursor_factory=pg.extras.DictCursor)

    # compute resources
    # select a.userid, s.groupid, a.shell, a.home_dir from res_accts_t1 as a left join res_shares_t1 as s on a.shareid = s.shareid where resourceid = %s;
    # select a.userid, t.gpname, a.shell, a.home_dir from res_accts_t1 as a left join (select s.*, g.gpname from res_shares_t1 as s left join groups_t1 as g on s.groupid = g.groupid) as t on a.shareid = t.shareid where resourceid = 4;
    for comp in open(cfg["computeres"]).readlines():
        comp = comp.strip().split(",")
        dir = "cms"
        
        if comp[0] == "N":
            nis[dir].cresource = comp[1]
            nis[dir].chome = comp[4]
            nis[dir].cshel = comp[3]
            for uname in users:
                if dir in users[uname].compute_access.keys():
                    users[uname].compute_access[comp[1]] = users[uname].compute_access[dir]
                    users[uname].compute_access.__delitem__(dir)
        elif comp[0] != "F":
            nis[dir] = ComputeResource(comp[1], dir, comp[2], comp[4], comp[3])
            cursor.execute("select g.groupid, g.gpname from res_shares_t1 as s left join groups_t1 as g on s.groupid = g.groupid where s.resourceid = %s;" % comp[0])
            resourceGroups = cursor.fetchall()
            for group in resourceGroups:
                if group["gpname"] not in groups:
                    print("Domain: %s group %s doesn\'t exist in userdb!" % (dir,group["gpname"]), file=sys.stderr)
                    continue
                if group["gpname"] not in cms_groups.values():
                    print("Domain: %s group %s is not a valid CMS group!" % (dir,group["gpname"]), file=sys.stderr)
                    continue
                nis[dir].groups[group["gpname"]]=groups[group["gpname"]]

            cursor.execute("select a.userid, t.gpname, a.shell, a.home_dir from res_accts_t1 as a \
                            left join (select s.*, g.gpname from res_shares_t1 as s left join groups_t1 as g on s.groupid = g.groupid) as t \
                            on a.shareid = t.shareid where resourceid = %s;" % comp[0])
            rows = cursor.fetchall()
            for row in rows:
                uname = row["home_dir"].rsplit("/", 1)[-1]
                if row["gpname"] in cms_groups.values() and uname in users:
                    gid = list(cms_groups.keys())[list(cms_groups.values()).index(row["gpname"])]
                    if comp[1] not in users[uname].compute_access:
                        users[uname].compute_access[comp[1]] = ComputeAccess(comp[1], gid, row["home_dir"], row["shell"])
                    else:
                        if gid == groups[comp[5]]:
                            users[uname].compute_access[comp[1]].add_secondary_group(users[uname].compute_access[comp[1]].gid)
                            users[uname].compute_access[comp[1]].gid = gid
                        else:
                            users[uname].compute_access[comp[1]].add_secondary_group(gid)
                    nis[dir].users[uname] = users[uname]
                    if gid not in users[uname].groups:
                        users[uname].add_group(row["gpname"], gid, False)
            nis[dir].primary_gid.append(groups[comp[5]]) ### CHECK IT! ###

def read_vulcan_storage_resources(config, users, groups, cms_groups):
    """
    read vulcan storage resources from a list and return a storage_structure
    Args:
        config:
        users:
        groups:
        cms_groups:

    Returns:

    """
    multiplier = {
	    "B": 1,
	    "KB": 1000,
	    "KIB": 1024,
	    "MB": 1000000,
	    "MIB": 1048576,         #1024^2
	    "GB": 1000000000,
	    "GIB": 1073741824,      #1024^3
	    "TB": 1000000000000,
	    "TIB": 1099511627776,   #1024^4
	}

    cfg = config.config._sections["vulcan"]
    conn_string = "host='%s' dbname='%s' user='%s' password='%s'" % \
                  (cfg["hostname"], cfg["database"], cfg["username"], cfg["password"])
    conn = pg.connect(conn_string)
    cursor = conn.cursor(cursor_factory=pg.extras.DictCursor)

    # storage resources
    storageList = open(cfg["storageres"]).readlines()
    storage_structure = {}
    for storage in storageList:
        storage = storage.strip().split(",")
        if storage[0] != "0": # This resources are coming from Vulcan
            storage_structure[storage[1]] = StorageResource(storage[1], storage[2], storage[3], storage[4], storage[5])
            try:
                # user quotas
                cursor.execute("select a.* from res_accts_t1 as a left join res_shares_t1 as s on a.shareid = s.shareid where s.resourceid = %s" 
                              % storage[0])
                rows = cursor.fetchall()

                for row in rows:
                    uname = row["home_dir"].rsplit("/", 1)[-1]
                    if uname in users:
                        storage_structure[storage[1]].quotas.append(StorageQuota(row["userid"], "null", "cms", row["home_dir"], row["quota"], "B", "null"))
                    else:
                        print("User %s doesn't exist in userdb. Skipping quota it's quota in %s." % (uname, storage[1]))

                # group quotas
                cursor.execute("select s.*, g.gpname from res_shares_t1 as s \
                                left join groups_t1 as g on s.groupid = g.groupid where s.resourceid = %s;" % storage[0])
                rows = cursor.fetchall()

                for row in rows:
                    if row["gpname"] in groups:
                        storage_structure[storage[1]].quotas.append(StorageQuota("null", groups[row["gpname"]], "cms", "null", row["quota"], "B", "null"))
                    else:
                        print("Group %s doesn't exist in userdb. Skipping quota it's quota in %s." % (row["gpname"], storage[1]))
            except:
                print("failed to fetch %s data from Vulcan" % storage[1], file=sys.stderr)
        else: #This resources are coming from an external file instead of Vulcan
            storage_structure[storage[1]] = StorageResource(storage[1], storage[2], storage[3], storage[4], storage[5])
            try:
                rows = open(cfg[storage[1].lower()]).readlines()

                uquota = True
                for row in rows:
                    # matches lines like: "user_or_group         0   100G   120G  00 [------]"
                    if re.match(r"[a-z\d_]+(\s+(0|\d+(\.\d+)?[BKMGTE])){3}\s+\d{2}\s+(\[(-+|\d\sdays?|-none-)\]|\d{1,2}(:\d{1,2}){2})", row):
                        row = row.split()
                        if row[2] != '0':
                            quota = row[2][0:-1]
                        if row[2][-1] in "KMGTE":
                            unit = row[2][-1] + "B"
                        else:
                            unit = "B"
                    elif re.match(r"[a-z0-9]+\s+(\d+.\d{2}\s[PTGMK]?B\s+){4}(\d+\s+){2}\w+\/\w+", row):
                        row = row.split()
                        quota = row[7]
                        unit = row[8]
                    else:
                        if "Group" in row:
                            uquota = False
                        continue

                    if uquota: # user quotas
                        if row[0] in users:
                            storage_structure[storage[1]].quotas.append(
                                    StorageQuota(users[row[0]].uid, "null", "cms", "%s/%s" % (storage_structure[storage[1]].spath, row[0]), str(int(float(quota) * multiplier[unit])), "B", "null")
                                )
                        else:
                            print("User %s doesn't exist in userdb. Skipping quota it's quota in %s." % (row[0], storage[0]))
                    else: # group quotas
                        if row[0] in cms_groups.values():
                            storage_structure[storage[1]].quotas.append(
                                    StorageQuota("null", groups[row[0]], "cms", "null", str(int(float(quota) * multiplier[unit])), "B", "null")
                                )
                        else:
                            print("Group %s is not a valid CMS group. Skipping quota it's quota in %s." % (row[0], storage[0]))

            except:
                print("failed to fetch %s data from %s" % (storage[0], cfg[storage[0].lower()]), file=sys.stderr)

    return storage_structure

def read_nas_storage(config):
    """
    read nas storage resources from a list of files and return a nas_structure
    Args:
        config:

    Returns: 
        nas_structure

    """
    servers = config.config._sections["nas"]

    nas_structure = []
    for server in servers:
        for line in open(servers[server], "r").readlines():
            if re.match(r"#+\sExport\sname:\s(.+)", line):
                volume = re.findall(r"#+\sExport\sname:\s(.+)", line)[0]
            host_access = re.findall(r"(.+)\((.+)\)", line)
            if host_access:
                nas_structure.append(NasStorage(server, volume, host_access[0][1], host_access[0][0]))
    
    return nas_structure

def populate_db(config, users, gids, vomss, gums, roles, collaborations, nis, storages, batch_structure, nas_structure, compute_resources):
    """
    create mysql dump for ferry database
    Args:
        config:
        users:
        gids:
        vomss:
        roles:

    Returns:

    """
    #mysql_client_cfg = MySQLUtils.createClientConfig("main_db", config)
    #connect_str = MySQLUtils.getDbConnection("main_db", mysql_client_cfg, config)

    if config.has_option("main_db", "update"):
        update = True
        updateList = config._sections["main_db"]["update"].splitlines()
    else:
        update = False

    fd = open(config._sections["path"]["output"], "w")

    if not update:
        # rebuild database and schema
        fd.write("\\connect ferry_test\n")
        fd.write("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'ferry';\n")
        fd.write("DROP DATABASE ferry;\n")
        fd.write("CREATE DATABASE ferry OWNER ferry;\n")
        fd.write("\\connect ferry\n")
        fd.write("GRANT ALL ON SCHEMA public TO ferry;\n")
        fd.write("GRANT ALL ON SCHEMA public TO public;\n")
        for line in open(config._sections["main_db"]["schemadump"]):
            fd.write(line)
        fd.flush()

    # populate users table
    #command = ""
    for user in users.values():
        #if user.uname!='kherner':
        #    continue
        if not user.expiration_date:
            user.expiration_date = "NULL"
        elif user.expiration_date == "EXPIRED":
            user.expiration_date = "NULL"
        elif user.expiration_date == "No Expiration date":
            user.expiration_date = "\'2038-01-01\'"
        else:
            user.expiration_date = "\'%s\'" % user.expiration_date
        status = "False"
        if user.status:
            status = "True"
        if update:
            if "users" in updateList:
                update_string = " on conflict (uname) do update set full_name = \'%s\', status = %s, expiration_date = %s, last_updated = NOW()" \
                % (user.full_name.strip().replace("'", "''"), status, user.expiration_date)
            else:
                update_string = " on conflict do nothing"
        else:
            update_string = ""
        fd.write("insert into users (uid, uname, full_name, status, expiration_date, last_updated) values (%d,\'%s\',\'%s\',%s,%s, NOW())%s;\n"
              % (int(user.uid), user.uname, user.full_name.strip().replace("'", "''"), status, user.expiration_date, update_string))
        # for now will just create ferry.sql file
        # results,return_code=MySQLUtils.RunQuery(command,connect_str)
        # if return_code!=0:
        #    print('Error ', command, file=sys.stderr)

    # populate groups table with unix group

    group_counter = 1
    gid_map = {}
    for gname, index in gids.items():
        if update:
            update_string = " on conflict do nothing"
        else:
            update_string = ""
        fd.write("insert into groups (gid,name,type,last_updated) values (%d,\'%s\','UnixGroup',NOW())%s;\n"
              % (int(index), gname, update_string))
        gid_map[index] = group_counter
        group_counter += 1
        # results,return_code=MySQLUtils.RunQuery(command,connect_str)
        # if return_code!=0:
        #    print('Error ', command, file=sys.stderr)
    fd.flush()

    # populate user_group table
    for _, user in users.items():
        #if uname!='kherner':
        #    continue
        for gid in user.groups:
            groupid = gid_map[gid]
            if update:
                if "user_group" in updateList:
                    update_string = " on conflict (uid, groupid) do update set is_leader = %s" % (user.groups[gid].is_leader)
                else:
                    update_string = " on conflict do nothing"
            else:
                update_string = ""
            fd.write("insert into user_group values (%d,(select groupid from groups where gid = %s),%s)%s;\n" % (int(user.uid), gid, user.groups[gid].is_leader, update_string))

    # populate collaborative_unit
    for cu in collaborations:
        if cu.alt_name:
            alt_name = "\'%s\'" % (cu.alt_name)
        else:
            alt_name = "NULL"
        if update:
            if "affiliation_units" in updateList:
                update_string = " on conflict (name) do update set alternative_name = %s, last_updated = NOW()" \
                % (alt_name)
            else:
                update_string = " on conflict do nothing"
        else:
            update_string = ""
        fd.write("insert into affiliation_units (name, alternative_name, last_updated) " +\
            "values (\'%s\',%s,NOW())%s;\n" % (cu.name, alt_name, update_string))

        # populate voms_url
        if update:
            update_string = " on conflict do nothing"
        else:
            update_string = ""
        if isinstance(cu,VOMS):
            if type(cu.url) is list:
                urls = cu.url
            else:
                urls = [cu.url]
            for link in urls:
                fd.write("insert into voms_url (unitid, url, last_updated) values ((select unitid from affiliation_units where name = '%s'),\'%s\',NOW())%s;\n"
                % (cu.name, link, update_string))

        # populate collaboration unit groups
        if update:
            update_string = " on conflict do nothing"
        else:
            update_string = ""
        for gid in cu.groups.values():
            index = gid_map[gid]
            is_primary = 'false'
            if (cu.name in nis.keys() and gid in nis[cu.name].primary_gid) or (cu.alt_name in nis.keys() and gid in nis[cu.alt_name].primary_gid):
                is_primary = 'true'
            fd.write("insert into affiliation_unit_group values((select unitid from affiliation_units where name = '%s'),%d,%s,NOW())%s;\n" \
            % (cu.name,index,is_primary,update_string))
    fd.flush()

    #populate compute_resource
    res_counter = 0
    for cu in collaborations:
        if cu.name in nis.keys():
            nis_info = nis[cu.name]
        elif cu.alt_name in  nis.keys():
            nis_info = nis[cu.alt_name]
        else:
            print("Neither %s not %s found in NIS" % (cu.name,cu.alt_name), file=sys.stderr)
            continue
        res_counter += 1
        if update:
            if "compute_resources" in updateList:
                update_string = " on conflict (name) do update set default_shell = \'%s\', default_home_dir = \'%s\', last_updated = NOW()" \
                % (nis_info.cshell, nis_info.chome)
            else:
                update_string = " on conflict do nothing"
        else:
            update_string = ""
        fd.write("insert into compute_resources (name, default_shell, default_home_dir, type, unitid, last_updated)" +\
                 " values (\'%s\', \'%s\', \'%s\', \'%s\', (select unitid from affiliation_units where name = '%s'), NOW())%s;\n" % (nis_info.cresource, nis_info.cshell,
                                                                             nis_info.chome,nis_info.ctype,cu.name,update_string))

        for _, user in nis_info.users.items():
            comp = user.compute_access[nis_info.cresource]
            groupid = gid_map[comp.gid]
            if update:
                if "compute_access" in updateList:
                    update_string = " on conflict (compid, uid) do update set shell = \'%s\', home_dir = \'%s\', last_updated = NOW()" \
                    % (comp.shell, comp.home_dir)
                else:
                    update_string = " on conflict do nothing"
            else:
                update_string = ""
            fd.write("insert into compute_access (compid, uid, shell, home_dir, last_updated)" + \
                     " values ((select compid from compute_resources where name = '%s'), %s, \'%s\', \'%s\', NOW())%s;\n" % (nis_info.cresource, user.uid, comp.shell, comp.home_dir, update_string))
            if update:
                update_string = " on conflict do nothing"
            else:
                update_string = ""
            fd.write("insert into compute_access_group (compid, uid, groupid, is_primary)" + \
                     " values ((select compid from compute_resources where name = '%s'), %s, (select groupid from groups where gid = %s), true)%s;\n" % (nis_info.cresource, user.uid, comp.gid, update_string))
            for gid in user.compute_access[nis_info.cresource].secondary_groups:
                fd.write("insert into compute_access_group (compid, uid, groupid, is_primary)" + \
                         " values ((select compid from compute_resources where name = '%s'), %s, (select groupid from groups where gid = %s), false)%s;\n" % (nis_info.cresource, user.uid, gid, update_string))

    for res in compute_resources.values():
        if res.cunit:
            unit = res.cunit
        else:
            unit = "Null"
        res_counter += 1
        if update:
            if "compute_resources" in updateList:
                update_string = " on conflict (name) do update set default_shell = \'%s\', default_home_dir = \'%s\', last_updated = NOW()" \
                % (res.cshell, res.chome)
            else:
                update_string = " on conflict do nothing"
        else:
            update_string = ""
        fd.write("insert into compute_resources (name, default_shell, default_home_dir, type, unitid, last_updated)" +\
                 " values (\'%s\', \'%s\', \'%s\', \'%s\', (select unitid from affiliation_units where name = '%s'), NOW())%s;\n" \
                 % (res.cresource, res.cshell, res.chome,res.ctype,unit,update_string))
        for _, user in res.users.items():
            comp = user.compute_access[res.cresource]
            groupid = gid_map[comp.gid]
            if update:
                if "compute_access" in updateList:
                    update_string = " on conflict (compid, uid) do update set shell = \'%s\', home_dir = \'%s\', last_updated = NOW()" \
                    % (comp.shell, comp.home_dir)
                else:
                    update_string = " on conflict do nothing"
            else:
                update_string = ""
            fd.write("insert into compute_access (compid, uid, shell, home_dir, last_updated)" + \
                     " values ((select compid from compute_resources where name = '%s'), %s, \'%s\', \'%s\', NOW())%s;\n" \
                     % (res.cresource, user.uid, comp.shell, comp.home_dir, update_string))
            if update:
                update_string = " on conflict do nothing"
            else:
                update_string = ""
            fd.write("insert into compute_access_group (compid, uid, groupid, is_primary)" + \
                     " values ((select compid from compute_resources where name = '%s'), %s, (select groupid from groups where gid = %s), true)%s;\n" \
                     % (res.cresource, user.uid, comp.gid, update_string))
            for gid in user.compute_access[res.cresource].secondary_groups:
                fd.write("insert into compute_access_group (compid, uid, groupid, is_primary)" + \
                         " values ((select compid from compute_resources where name = '%s'), %s, (select groupid from groups where gid = %s), false)%s;\n" \
                         % (res.cresource, user.uid, gid, update_string))

    fd.flush()
    
    #populate storage_resource
    storage_counter = 0
    for storage in storages.values():
        storage_counter += 1
        if update:
            if "storage_resources" in updateList:
                update_string = " on conflict (name) do update set type = \'%s\', default_path = \'%s\', default_quota = \'%s\', default_unit = \'%s\', last_updated = NOW()" \
                % (storage.stype, storage.spath, storage.squota, storage.sunit)
            else:
                update_string = " on conflict do nothing"
        else:
            update_string = ""
        fd.write("insert into storage_resources (name, type, default_path, default_quota, default_unit, last_updated)" + \
                 " values (\'%s\', \'%s\', \'%s\', \'%s\', \'%s\', NOW())%s;\n" \
                 % (storage.sresource, storage.stype, storage.spath, storage.squota, storage.sunit, update_string))
        for quota in storage.quotas:
            for cu in collaborations:
                if cu.name != quota.qcunit:
                    continue
                if quota.qgid != 'null':
                    gid = quota.qgid
                    target = "(storageid, groupid) where valid_until is null"
                else:
                    gid = 'null'
                    target = "(storageid, uid) where valid_until is null"
                if update:
                    if "storage_quota" in updateList:
                        update_string = " on conflict %s do update set path = \'%s\', value = %s, unit = \'%s\', last_updated = NOW()" \
                        % (target, quota.qpath, quota.qvalue, quota.qunit)
                    else:
                        update_string = " on conflict do nothing"
                else:
                    update_string = ""
                query = "insert into storage_quota (storageid, uid, groupid, unitid, path, value, unit, valid_until, last_updated)" + \
                        " values (%s, %s, (select groupid from groups where gid = %s), (select unitid from affiliation_units where name = '%s'), \'%s\', %s, \'%s\', \'%s\', NOW())%s;\n" \
                        % (storage_counter, quota.quid, gid, cu.name, quota.qpath, quota.qvalue, quota.qunit, quota.quntil, update_string)
                fd.write(query.replace("'null'", "null"))
                break

    # populating experiment_fqan table
    # GUMS darksidepro {'group': '/fermilab/darkside', 'server': 'fermilab', 'uname': 'darksidepro', 'gid': '9985', 'role': 'Production', 'user_group': 'darksidepro', 'account_mappers': 'darksidepro'}
    # experiment_fqan(fqanid, fqan, mapped_user,mapped_group);
    fqan_counter = 0
    for _, gmap in gums.items():
        fqan_counter += 1
        gname = list(gids.keys())[list(gids.values()).index(gmap.gid)]
        #and gmap.uname in users.keys()
        if gmap.uname:
            un = "\'%s\'" % (gmap.uname)
            target = "(fqan, mapped_user, mapped_group)"
        else:
            un='NULL'
            target = "(fqan, mapped_group) WHERE (mapped_user IS NULL)"

        exp = gmap.group[1:].strip().split('/')
        if exp[0] == 'fermilab':
            if len(exp) == 1:
                exp_name = exp[0]
            else:
                exp_name = exp[1]
        else:
            exp_name = exp[0]

        exp = 'NULL'
        for cu in collaborations:
            if exp_name.lower() == cu.name.lower():
                exp = cu.name
                break

        if update:
            if "grid_fqan" in updateList:
                update_string = " on conflict %s do update set mapped_user = (select uid from users where uname = %s), mapped_group = (select groupid from groups where name = \'%s\' and type = 'UnixGroup'), last_updated = NOW()" \
                % (target, un, gname)
            else:
                update_string = " on conflict do nothing"
        else:
            update_string = ""
        fd.write("insert into grid_fqan (unitid,fqan,mapped_user,mapped_group) values((select unitid from affiliation_units where name = '%s'),\'%s/Role=%s/Capability=NULL\', (select uid from users where uname = %s), (select groupid from groups where name = \'%s\' and type = 'UnixGroup'))%s;\n"
        % (exp, gmap.group, str(gmap.role).replace('None', 'NULL'), un, gname, update_string))
        gmap.set_id(fqan_counter)
    fd.flush()

    if update:
        update_string = " on conflict do nothing"
    else:
        update_string = ""
    experiment_counter = 0
    for cu in collaborations:
        experiment_counter += 1
        if not isinstance(cu,VOMS):
            continue

        for _, user in users.items():
            #if uname!='kherner':
            #    continue
            if type(cu.url) is list:
                urls = cu.url
            else:
                urls = [cu.url]
            for url in urls:
                if user.vo_membership.__contains__((cu.name, url)):
                    for umap in user.vo_membership[(cu.name, url)]:
                        fqanid = 0
                        for gmap in gums.values():
                            if  umap.group == gmap.group and umap.role == gmap.role:
                                fqanid = gmap.fqanid
                                break
                        fqan = "\'%s/Role=%s/Capability=NULL\'" % (umap.group, str(umap.role).replace('None', 'NULL'))
                        fd.write("insert into grid_access values  (%d,(select fqanid from grid_fqan where fqan = %s),False,False,NOW())%s;\n" % \
                                (int(user.uid), fqan, update_string))

                    fd.flush()

    # populating user_certificates table
    if update:
        update_string = " on conflict do nothing"
    else:
        update_string = ""
    for user in users.values():
        for dn, details in user.certificates.items():
            fd.write("insert into user_certificates (dn,uid,last_updated) "
                     "values (\'%s\',%d,NOW())%s;\n" % (dn, int(user.uid), update_string))
            for experiment in details['experiments']:
                fd.write("insert into affiliation_unit_user_certificate (unitid,dnid,last_updated) "
                         "values ((select unitid from affiliation_units where name = \'%s\'),(select dnid from user_certificates where dn = \'%s\'),NOW())%s;\n" % (collaborations[experiment - 1].name, dn, update_string))

    # populating external_affiliation_attribute
    if update:
        update_string = " on conflict do nothing"
    else:
        update_string = ""
    for _, user in users.items():
        for external_affiliation in user.external_affiliations:
            fd.write("insert into external_affiliation_attribute (uid,attribute,value,last_updated) values (%d,\'%s\',"
                     "\'%s\',NOW())%s;\n" % (int(user.uid), external_affiliation[0], external_affiliation[1], update_string))

    # populating compute_batch
    for cr_name, cr_data in batch_structure.items():
        res_counter += 1
        if update:
            if "compute_resources" in updateList:
                update_string = " on conflict (name) do update set default_shell = \'%s\', default_home_dir = \'%s\', last_updated = NOW()" \
                % (str(cr_data.cshell).replace("None", "default"), str(cr_data.chome).replace("None", "default"))
            else:
                update_string = " on conflict do nothing"
        else:
            update_string = ""
        if cr_data.cunit:
            aunit = collaborations[cr_data.cunit - 1].name
        else:
            aunit = cr_data.cunit
        query = ("insert into compute_resources (name, default_shell, default_home_dir, type, unitid, last_updated) " + 
                 "values (\'%s\', \'%s\', \'%s\', \'%s\', (select unitid from affiliation_units where name = %s), NOW())%s;\n"
              % (cr_name, str(cr_data.cshell).replace("None", "default"),
              str(cr_data.chome).replace("None", "default"), cr_data.ctype, aunit, update_string))
        fd.write(query.replace("'None'", "NULL").replace("None", "NULL"))
        for batch in cr_data.batch:
            if update:
                if "compute_batch" in updateList:
                    update_string = " on conflict (compid, name) where valid_until is null do update set value = %s, type = \'%s\', last_updated = NOW()" \
                    % (batch.value, batch.type)
                else:
                    update_string = " on conflict do nothing"
            else:
                update_string = ""
            query = ("insert into compute_batch (compid, name, value, type, unitid, last_updated) " +
                     "values ((select compid from compute_resources where name = '%s'), \'%s\', %s, \'%s\', (select unitid from affiliation_units where name = '%s'), NOW())%s;\n"
                  % (cr_name, batch.name, batch.value, batch.type, batch.experiment, update_string))
            fd.write(query.replace("'None'", "Null").replace("None", "Null"))
        for _, user in cr_data.users.items():
            comp = user.compute_access[res.cresource]
            groupid = gid_map[comp.gid]
            if update:
                if "compute_access" in updateList:
                    update_string = " on conflict (compid, uid) do update set shell = \'%s\', home_dir = \'%s\', last_updated = NOW()" \
                    % (comp.shell, comp.home_dir)
                else:
                    update_string = " on conflict do nothing"
            else:
                update_string = ""
            fd.write("insert into compute_access (compid, uid, shell, home_dir, last_updated)" + \
                     " values ((select compid from compute_resources where name = \'%s\'), %s, \'%s\', \'%s\', NOW())%s;\n" % (comp.comp_name, user.uid, comp.shell, comp.home_dir, update_string))
            if update:
                update_string = " on conflict do nothing"
            else:
                update_string = ""
            fd.write("insert into compute_access_group (compid, uid, groupid, is_primary)" + \
                     " values ((select compid from compute_resources where name = \'%s\'), %s, (select groupid from groups where gid = %s), true)%s;\n" % (comp.comp_name, user.uid, comp.gid, update_string))
            for gid in user.compute_access[res.cresource].secondary_groups:
                fd.write("insert into compute_access_group (compid, uid, groupid, is_primary)" + \
                         " values ((select compid from compute_resources where name = \'%s\'), %s, (select groupid from groups where gid = %s), false)%s;\n" % (comp.comp_name, user.uid, gid, update_string))


    # populating nas_storage
    if update:
        update_string = " on conflict do nothing"
    else:
        update_string = ""
    for nas in nas_structure:
        fd.write("insert into nas_storage (server, volume, access_level, host) values ('%s', '%s', '%s', '%s')%s;\n"
              % (nas.server, nas.volume, nas.access_level, nas.host, update_string))
    fd.flush()

    # add post ingest script
    fd.write("\n")
    for line in open(config._sections["path"]["post_ingest"]):
        fd.write(line)
    fd.flush()

    fd.close()

if __name__ == "__main__":
    import os
    config = Configuration()
    if len(sys.argv) > 1:
        config.configure(sys.argv[1])
    else:
        config.configure(os.path.dirname(os.path.realpath(__file__)) + "/dev.config")
    # read all information about groups from gid.lis
    gids = read_gid(config.config.get("user_db", "gid_file"))

    # read all information about users from uid.lis file
    users = read_uid(config.config.get("user_db", "uid_file"))

    # read services_user_files.csv and add this information to users containers
    read_services_users(config.config.get("user_db", "services_user_file"), users)

    # read Vulcan group memberships and add this information to users containers
    cms_groups=read_vulcan_user_group(config, users)

    # read NIS information
    nis_structure = read_nis(config.config.get("nis", "primary_groups"), config.config.get("nis", "dir_path"),
                             config.config.get("nis", "exclude_domain"), config.config.get("nis", "name_mapping"), users, gids)

    # read NAS storages
    nas_structure = read_nas_storage(config)

    # read valid CMS resources from Vulcan
    read_vulcan_compute_resources(config, nis_structure, users, gids, cms_groups)
    storages = read_vulcan_storage_resources(config, users, gids, cms_groups)

    # process voms information; list of VOMS instances should be in configuration file
    voms_instances = config.config.get("voms_instances", "list")
    voms_list = voms_instances.split(",")
    voms_list.sort()
    vomss = {} # dictionary of VOs from VOMS instances
    roles = []
    gums = read_gums_config(config, users, gids) # List of GUMS group mapping

    # read Vulcan X509 certificates and voms and add this information to proper containers
    read_vulcan_certificates(config, users, vomss)

    # read FermiGrid quotas
    batch_structure = read_compute_batch(config, users, gids, gums)

    # add special compute resoruces
    compute_resources = add_compute_resources(users, gids, gums)

    # need to assign incremental ids to all the entities
    # groupid  - use gids index
    # unitid - collaboration unit
    # computeid - compute cluster id
    # fqanid -



    for vn in voms_list:
        url = config.config.get("voms_db_%s" % (vn,), "url")
        # reads vo related information from VOMS
        get_vos(config.config, vn, url, gids, vomss)

    # define collaboration unit
    # in case where there are fermilab subgroup and a separate VO (des, dune) the nis_structure will be set for a
    # separate vo

    collaborations = build_collaborations(vomss, nis_structure, gids)

    # add VOMS info for collaboration_unit if relevant
    for vo in vomss.values():
        found = False
        for cu in collaborations:
            if cu.name == vo.name or cu.alt_name == vo.name:
                found = True
                break
        if not found:
            print("NIS domain doesn't exist for %s" % (vo.name), file=sys.stderr)
            collaborations.append(vo)
            vo.set_id(len(collaborations))


    for vn in voms_list:
        url = config.config.get("voms_db_%s" % (vn,), "url")
        assign_vos(config.config, vn, url, roles, users, gums, collaborations)

    #for uname, user in users.items():
    #        if user.uname == "wicz":
    #            print user.uname
    #            for id, certs in user.certificates.items():
    #                for c in certs:
    #                    print id, collaborations[id-1].name,c.dn,c.ca
    #            for k,v in user.vo_membership.items():
    #                for l in v:
    #                    print k, l.__dict__
    populate_db(config.config, users, gids, vomss, gums, roles,collaborations, nis_structure, storages, batch_structure, nas_structure, compute_resources)
    print("Done!")
