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
            if not len(line[:-1]):
                continue
            tmp = line[:-1].split("\t\t")
            if not usrs.__contains__(tmp[4].strip().lower()):
                usrs[tmp[4].strip().lower()] = User(tmp[0].strip(), tmp[3].strip().lower().capitalize() + " " +
                                                     tmp[2].strip().lower().capitalize(), tmp[4].strip().lower())
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
        if not len(line[:-1]):
            continue
        try:
            tmp = line[:-1].strip("\t").split("\t")
            groupids[tmp[1].strip().lower()] = tmp[0].strip()

        except:
            print("Faoiled reading group.lis (%s) file. Failed: " % (fname, line), file=sys.stderr)
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
                if tmp[2] != "No Expiration date":
                    if tmp[2] == "EXPIRED":
                        users[tmp[0]].set_status(0)
                users[tmp[0]].set_expiration_date(tmp[2].strip())
                users[tmp[0]].is_k5login = True
        except:
            print("csv Failed ", line, file=sys.stderr)


def get_vos(config, vn, vurl, gids):
    """
    Read data from VOMS databases
    Args:
        config:
        vn:
        vname:
        vurl:
        gids:

    Returns: {voname:VOMS,}

    """
    mysql_client_cfg = MySQLUtils.createClientConfig("voms_db_%s" % (vn,), config)
    connect_str = MySQLUtils.getDbConnection("voms_db_%s" % (vn,), mysql_client_cfg, config)
    command = "select dn from groups"
    groups, return_code = MySQLUtils.RunQuery(command, connect_str, False)
    volist = {}
    if return_code:
        print("Failed to select dn from table group from voms_db_%s %s" % (vn, groups), file=sys.stderr)
        return volist
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
            if not volist.__contains__(name):
                volist[name] = VOMS(vurl, vn, name)
            if gids.__contains__(name):
                volist[name].add_voms_unix_group(name,gids[name])
        except:
            print("group is not defined ", name, file=sys.stderr)
    return volist


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
    command = "select u.dn,u.userid,d.subject_string,c.subject_string from certificate d, ca c, usr u where u.userid = "\
              "d.usr_id and c.cid=d.ca_id and (c.subject_string not like \'%HSM%\' and c.subject_string not like " \
              "\'%Digi%\' and c.subject_string  not like \'%DOE%\') and u.userid in (" \
              "select distinct  u.userid from usr u, certificate d where u.userid = d.usr_id);"
    table, return_code = MySQLUtils.RunQuery(command, connect_str)
    if return_code:
        print("Failed to extract information from VOMS %s" % (vn), file=sys.stderr)
        return
    else:
        for line in table:
            uname = re.findall("UID:([A-z]*)", line)
            if len(uname) > 0:
                if uname[0] not in members_list:
                    members_list[uname[0]] = []
                members_list[uname[0]].append(line.split("\t", 1)[1])

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
        for m in members:
            member = m.split("\t")
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
                for m in members:
                    member = m.split("\t")
                    if member[0] == mid:
                        subject = member[1].strip()
                        issuer = member[2].strip()
                        for cu in collaborations:
                            if cu.name == group or cu.alt_name == group:
                                CA = ca.matchCA(CAs, subject)
                                if CA:
                                    index = collaborations.index(cu) +1
                                    user.add_cert(Certificate(index,subject, issuer))
                                else:
                                    print("Certificate %s is not issued by a valid CA" % subject)

                if len(tmp) > 1:
                    role = tmp[1].strip()
                else:
                    role = None

                for key, umap in gums_map.items():

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
    gums_fn = config.config.get("gums_config", "gums_config")
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
    vomss.append({'cms': VOMS(url, vo, vo)})

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
                users[row["uname"]].add_cert(Certificate(len(vomss), row["auth_string"], CA["subjectdn"]))
                cernUser = re.findall("/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=(\w*)/CN=\d+/CN=[A-z\s]+", row["auth_string"])
                if len(cernUser) > 0:
                    users[row["uname"]].add_external_affiliation("cern", cernUser[0])

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
    for vos in vomss:
        for vo in vos.values():
            # We should ignore VO (des, dune) that are subgroups in fermilab VOMS
            #  they don't have NIS domain
            #  We also should ignore top level fermilab vo - it doesn't have NIS domain

            if (vo.name in ["des","dune"] and vo.url.find("fermilab") >= 0) or vo.name == "fermilab":
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

def read_nis(dir_path, exclude_list, altnames, users, groups):
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
        nis[dir] = ComputeResource( dir, dir)

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
            nis[dir].users[uname] = users[uname]
            nis[dir].primary_gid.append(gid)
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
            if len(tmp) < 4 or len(tmp[3].strip()) == 0:
                continue

            user_list = tmp[3].split(",")
            if gid not in groups.values():
                print("Domain: %s group %s from group filedoesn\'t exist  in userdb!" % \
                                     (dir,gid,), file=sys.stderr)
                continue
            if gname not in groups.keys():
                print("Domain: %s group name %s, %s from group file doesn\'t exist  in userdb!" % \
                                     (dir,gname,gid), file=sys.stderr)
                continue
            if gid != groups[gname]:
                print("Domain: %s group %s from group file %s have different gid in userdb!" % \
                                     (dir,gname,gid,groups[gname]), file=sys.stderr)
                continue
            for uname in user_list:
                if uname not in users.keys():
                    print("Domain: %s User %s in group file in not in userdb!" % (dir,uname,), file=sys.stderr)
                    continue
                if dir in users[uname].compute_access:
                    users[uname].compute_access[dir].add_secondary_group(gid)
                    nis[dir].groups[gname]=gid
                else:
                    print("Domain: %s User %s in group file but not in passwd!" % (dir,uname,), file=sys.stderr)

    return nis

def read_compute_batch(config):
    """
    read batch resources quotas and priorities
    Args:
        config:

    Returns:

    """

    batch_structure = {}

    # FermiGrid
    batch_structure["fermigrid"] = ComputeResource("fermigrid", None, "Batch", None, None)
    quotas = open(config.config._sections["fermigrid"]["quotas"]).readlines()

    for quota in quotas:
        name, value = quota.strip().split(" = ")
        batch_structure["fermigrid"].batch.append(ComputeBatch(name, value, "quota"))

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
        
        if comp[0] != "0":
            nis[dir] = ComputeResource(comp[1], dir, comp[2], comp[4], comp[3])
            cursor.execute("select g.groupid, g.gpname from res_shares_t1 as s left join groups_t1 as g on s.groupid = g.groupid where s.resourceid = %s;" % comp[0])
            resourceGroups = cursor.fetchall()
            for group in resourceGroups:
                if group["gpname"] not in groups:
                    print("Domain: %s group %s doesn\'t exist in userdb!" % (dir,group["gpname"]), file=sys.stderr)
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
                    users[uname].compute_access[comp[1]] = ComputeAccess(comp[1], gid, row["home_dir"], row["shell"])
                    nis[dir].users[uname] = users[uname]
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
        if storage[0] != "0": # This resources are coming from an external file instead of Vulcan
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
        else:
            storage_structure[storage[1]] = StorageResource(storage[1], storage[2], storage[3], storage[4], storage[5])
            try:
                rows = open(cfg[storage[1].lower()]).readlines()

                uquota = True
                for row in rows:
                    # matches lines like: "user_or_group         0   100G   120G  00 [------]"
                    if re.match("[a-z\d_]+(\s+(0|\d+(\.\d+)?[BKMGTE])){3}\s+\d{2}\s+(\[(-+|\d\sdays?|-none-)\]|\d{1,2}(:\d{1,2}){2})", row):
                        row = row.split()
                        if row[2] != '0':
                            quota = row[2][0:-1]
                        if row[2][-1] in "KMGTE":
                            unit = row[2][-1] + "B"
                        else:
                            unit = "B"

                        if uquota: # user quotas
                            if row[0] in users:
                                storage_structure[storage[1]].quotas.append(
                                        StorageQuota(users[row[0]].uid, "null", "cms", "%s/%s" % (storage_structure[storage[1]].spath, row[0]), quota, unit, "null")
                                    )
                            else:
                                print("User %s doesn't exist in userdb. Skipping quota it's quota in %s." % (row[0], storage[0]))
                        else: # group quotas
                            if row[0] in cms_groups.values():
                                storage_structure[storage[1]].quotas.append(
                                        StorageQuota("null", groups[row[0]], "cms", "null", quota, unit, "null")
                                    )
                            else:
                                print("Group %s is not a valid CMS group. Skipping quota it's quota in %s." % (row[0], storage[0]))
                    else:
                        if row.startswith("Group quota"):
                            uquota = False
            except:
                print("failed to fetch %s data from %s" % (storage[0], cfg[storage[0].lower()]), file=sys.stderr)

    return storage_structure

def populate_db(config, users, gids, vomss, gums, roles, collaborations, nis, storages, batch_structure):
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
    mysql_client_cfg = MySQLUtils.createClientConfig("main_db", config)
    connect_str = MySQLUtils.getDbConnection("main_db", mysql_client_cfg, config)

    # rebuild database and schema
    fd = open(config._sections["path"]["output"], "w")
    fd.write("\connect ferry_test\n")
    fd.write("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'ferry';\n")
    fd.write("DROP DATABASE ferry;\n")
    fd.write("CREATE DATABASE ferry OWNER ferry;\n")
    fd.write("\connect ferry\n")
    fd.write("GRANT ALL ON SCHEMA public TO ferry;\n")
    fd.write("GRANT ALL ON SCHEMA public TO public;\n")
    for line in open(config._sections["main_db"]["schemadump"]):
        fd.write(line)
    fd.flush()

    # populate users table
    command = ""
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
        fd.write("insert into users (uid, uname, full_name, status, expiration_date, last_updated) values (%d,\'%s\',\'%s\',%s,%s, NOW());\n"
                 % (int(user.uid), user.uname, user.full_name.strip(), status,
                    user.expiration_date))
        # for now will just create ferry.sql file
        # results,return_code=MySQLUtils.RunQuery(command,connect_str)
        # if return_code!=0:
        #    print('Error ', command, file=sys.stderr)

    # populate groups table with unix group

    group_counter = 1
    gid_map = {}
    for gname, index in gids.items():
        fd.write("insert into groups (gid,name,type,last_updated) values (%d,\'%s\','UnixGroup',NOW());\n"
                 % (int(index), gname))
        gid_map[index] = group_counter
        group_counter += 1
        # results,return_code=MySQLUtils.RunQuery(command,connect_str)
        # if return_code!=0:
        #    print('Error ', command, file=sys.stderr)
    fd.flush()

    # populate collaborative_unit
    for cu in collaborations:
        if isinstance(cu,VOMS):
            link = "\'%s\'" % (cu.url)
        else:
            link = "NULL"
        if cu.alt_name:
            alt_name = "\'%s\'" % (cu.alt_name)
        else:
            alt_name = "NULL"
        fd.write("insert into affiliation_units (name, voms_url, alternative_name, last_updated) " +\
            "values (\'%s\',%s,%s,NOW());\n" % (cu.name, link,alt_name))
        # populate collaboration unit groups

        for gid in cu.groups.values():
            index = gid_map[gid]
            is_primary = 0
            if cu.name in nis.keys():
                if index in nis[cu.name].primary_gid:
                    is_primary = 1
            else:
                    is_primary = 0
            fd.write("insert into affiliation_unit_group values(%d,%d,%s,NOW());\n" % (cu.unitid,index,is_primary))
    fd.flush()

    #populate compute_resource
    nis_counter = 0
    for cu in collaborations:
        if cu.name in nis.keys():
            nis_info = nis[cu.name]
        elif cu.alt_name in  nis.keys():
            nis_info = nis[cu.alt_name]
        else:
            print("Neither %s not %s found in NIS" % (cu.name,cu.alt_name), file=sys.stderr)
            continue
        nis_counter += 1
        fd.write("insert into compute_resources (compid,name, default_shell,default_home_dir,type, unitid,last_updated)" +\
                 " values (\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',%s,NOW());\n" % (nis_counter, nis_info.cresource,
                                                                         nis_info.cshell,nis_info.chome,nis_info.ctype,cu.unitid))

        for uname, user in nis_info.users.items():
            comp = user.compute_access[nis_info.cresource]
            groupid = gid_map[comp.gid]
            fd.write("insert into compute_access (compid, uid, groupid,shell,home_dir,last_updated)" + \
                     " values (%s,%s,%s,\'%s\',\'%s\', NOW());\n" % (nis_counter,user.uid,groupid,comp.shell,comp.home_dir))
    fd.flush()
    
    #populate storage_resource
    storage_counter = 0
    for storage in storages.values():
        storage_counter += 1
        fd.write("insert into storage_resources (name, type, default_path, default_quota, default_unit, last_updated)" + \
                 " values (\'%s\', \'%s\', \'%s\', \'%s\', \'%s\', NOW());\n" % (storage.sresource, storage.stype, storage.spath, storage.squota, storage.sunit))
        for quota in storage.quotas:
            for cu in collaborations:
                if cu.name != quota.qcunit:
                    continue
                if quota.qgid != 'null':
                    groupid = gid_map[quota.qgid]
                else:
                    groupid = 'null'
                query = "insert into storage_quota (storageid, uid, groupid, unitid, path, value, unit, valid_until, last_updated)" + \
                        " values (%s, %s, %s, %s, \'%s\', %s, \'%s\', \'%s\', NOW());\n" % (storage_counter, quota.quid, groupid, cu.unitid, quota.qpath, 
                                                                                             quota.qvalue, quota.qunit, quota.quntil)
                fd.write(query.replace("'null'", "null"))
                break

    # populating experiment_fqan table
    # GUMS darksidepro {'group': '/fermilab/darkside', 'server': 'fermilab', 'uname': 'darksidepro', 'gid': '9985', 'role': 'Production', 'user_group': 'darksidepro', 'account_mappers': 'darksidepro'}
    # experiment_fqan(fqanid, fqan, mapped_user,mapped_group);
    fqan_counter = 0
    for key, gmap in gums.items():
        fqan_counter += 1
        gname = list(gids.keys())[list(gids.values()).index(gmap.gid)]
        #and gmap.uname in users.keys()
        if gmap.uname:
            un = "\'%s\'" % (gmap.uname)
        else:
            un='NULL'

        fd.write("insert into grid_fqan (fqan,mapped_user,mapped_group) values(\'%s/Role=%s\',%s,\'%s\');\n" % (gmap.group,gmap.role,un,gname))
        gmap.set_id(fqan_counter)
    fd.flush()

    experiment_counter = 0
    for cu in collaborations:
        experiment_counter += 1
        if not isinstance(cu,VOMS):
            continue

        for uname, user in users.items():
            #if uname!='kherner':
            #    continue
            if user.vo_membership.__contains__((cu.name, cu.url)):
                for umap in user.vo_membership[(cu.name, cu.url)]:
                    fqanid = 0
                    for gmap in gums.values():
                        if  umap.group == gmap.group and umap.role == gmap.role:
                            fqanid = gmap.fqanid
                            break
                    fd.write("insert into grid_access values  (%d,%d,%d,False,False,NOW());\n" % \
                             (int(user.uid),cu.unitid, fqanid ))

                if cu.unitid not in user.certificates.keys():
                    continue
                for certs in user.certificates[cu.unitid]:
                    fd.write("insert into user_certificate (uid,dn,issuer_ca,unitid,last_updated) values (%d,\'%s\',"
                            "\'%s\',%d,NOW());\n" % (int(user.uid), certs.dn,certs.ca,experiment_counter))

                fd.flush()
    for uname, user in users.items():
        #if uname!='kherner':
        #    continue
        for gid in user.groups:
            groupid = gid_map[gid]
            fd.write("insert into user_group values (%d,%d,%s);\n" % (int(user.uid), groupid, user.groups[gid].is_leader))

    # populating external_affiliation_attribute
    for uname, user in users.items():
        for external_affiliation in user.external_affiliations:
            fd.write("insert into external_affiliation_attribute (uid,attribute,value,last_updated) values (%d,\'%s\',"
                     "\'%s\',NOW());\n" % (int(user.uid), external_affiliation[0], external_affiliation[1]))

    # populating compute_batch
    cr_counter = nis_counter
    for cr_name, cr_data in batch_structure.items():
        cr_counter += 1
        query = ("insert into compute_resources (compid, name, default_shell, default_home_dir, type, unitid, last_updated) " + 
                 "values (\'%s\', \'%s\', \'%s\', \'%s\', \'%s\', %s, NOW());\n"
              % (cr_counter, cr_name, cr_data.cshell, cr_data.chome, cr_data.ctype, cr_data.cunit))
        fd.write(query.replace("'None'", "Null").replace("None", "Null"))
        for batch in cr_data.batch:
            query = ("insert into compute_batch (compid, name, value, type, groupid, last_updated) " +
                     "values (%s, \'%s\', %s, \'%s\', %s, NOW());\n"
                  % (cr_counter, batch.name, batch.value, batch.type, batch.groupid))
            fd.write(query.replace("'None'", "Null").replace("None", "Null"))

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
    nis_structure = read_nis(config.config.get("nis", "dir_path"),config.config.get("nis", "exclude_domain"),
                             config.config.get("nis", "name_mapping"),users, gids)

    # read FermiGrid quotas
    batch_structure = read_compute_batch(config)

    # read valid CMS resources from Vulcan
    read_vulcan_compute_resources(config, nis_structure, users, gids, cms_groups)
    storages = read_vulcan_storage_resources(config, users, gids, cms_groups)

    # process voms information; list of VOMS instances should be in configuration file
    voms_instances = config.config.get("voms_instances", "list")
    voms_list = voms_instances.split(",")
    voms_list.sort()
    vomss = [] # list of VOs from VOMS instances
    roles = []
    gums = read_gums_config(config, users, gids) # List of GUMS group mapping

    # read Vulcan X509 certificates and voms and add this information to proper containers
    read_vulcan_certificates(config, users, vomss)

    # need to assign incremental ids to all the entities
    # groupid  - use gids index
    # unitid - collaboration unit
    # computeid - compute cluster id
    # fqanid -



    for vn in voms_list:
        url = config.config.get("voms_db_%s" % (vn,), "url")
        # reads vo related information from VOMS
        vos = get_vos(config.config, vn, url, gids)
        vomss.append(vos)

    # define collaboration unit
    # in case where there are fermilab subgroup and a separate VO (des, dune) the nis_structure will be set for a
    # separate vo

    collaborations = build_collaborations(vomss, nis_structure, gids)

    # add VOMS info for collaboration_unit if relevant
    for vos in vomss:
        for vo in vos.values():
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
    populate_db(config.config, users, gids, vomss, gums, roles,collaborations, nis_structure, storages, batch_structure)
    print("Done!")
