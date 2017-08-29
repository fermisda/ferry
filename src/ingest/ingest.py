#!/usr/bin/env python

""" This script is uploading data from various source to FERRY database
"""
import sys
from Configuration import Configuration
from MySQLUtils import MySQLUtils
from User import User, Certificate, ComputeAccess
from Resource import CollaborationUnit, VOMS, VOUserGroup, ComputeResource
import xml.etree.ElementTree as ET
import psycopg2 as pg
import psycopg2.extras
import fetchcas as ca


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
            if not usrs.has_key(tmp[4].strip().lower()):
                usrs[tmp[4].strip().lower()] = User(tmp[0].strip(), tmp[2].strip().lower().capitalize(),
                                                     tmp[3].strip().lower().capitalize(), tmp[4].strip().lower())
                usrs[tmp[4].strip().lower()].add_group((tmp[1].strip().lower()))
        except:
            print >> sys.stderr, "Failed ", line
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
            print >> sys.stderr, "Faoiled reading group.lis (%s) file. Failed: " % (fname, line)
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
            if users.has_key(tmp[0]):
                if tmp[2] != "No Expiration date":
                    if tmp[2] == "EXPIRED":
                        users[tmp[0]].set_status(0)
                users[tmp[0]].set_expiration_date(tmp[2].strip())
                users[tmp[0]].is_k5login = True
        except:
            print >> sys.stderr, "csv Failed ", line


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
    groups, return_code = MySQLUtils.RunQuery(command, connect_str,False)
    volist = {}
    if return_code:
        print >> sys.stderr, "Failed to select dn from table group from voms_db_%s %s" % (vn, groups)
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
            if not volist.has_key(name):
                volist[name] = VOMS(vurl, vn, name)
            if gids.has_key(name):
                volist[name].add_voms_unix_group(name,gids[name])
        except:
            print >> sys.stderr, "group is not defined ", name
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
    mysql_client_cfg = MySQLUtils.createClientConfig("voms_db_%s" % (vn,), config)
    connect_str = MySQLUtils.getDbConnection("voms_db_%s" % (vn,), mysql_client_cfg, config)
    # for all users in user-services.csv

    total = 0

    for uname, user in usrs.items():
        #if uname!='kherner':
        #    continue
        # do we want to have expired users in VOMS?
        total += 1
        if not user.is_k5login:
            continue

        command = "select u.userid,d.subject_string,c.subject_string from certificate d, ca c, usr u where u.userid = "\
                  "d.usr_id and c.cid=d.ca_id and (c.subject_string not like \'%HSM%\' and c.subject_string not like " \
                  "\'%Digi%\' and c.subject_string  not like \'%DOE%\') and u.userid in (" \
                  "select distinct  u.userid from usr u, certificate d where u.userid = d.usr_id and (u.dn like " \
                  "\'%UID:" + uname + "\' or d.subject_string like \'%UID:" + uname + "\'));"
        members, return_code = MySQLUtils.RunQuery(command, connect_str, False)

        if return_code:
            print >> sys.stderr, "Failed to extract information from VOMS %s for user %s" % (vn, user.uname)
            continue
        # user is not a member of VO
        if len(members[0].strip()) == 0:
            continue

        mids = []

        # stores all voms uids
        for m in members:
            member = m.split("\t")
            if member[0] not in mids:
                mids.append(member[0].strip())

        for mid in mids:
            # gets all the groups that this user is a member
            command = "select distinct g.dn from m m, groups  g where g.gid=m.gid and " \
                      "m.userid =" + mid+";"
            affiliation, return_code = MySQLUtils.RunQuery(command, connect_str)
            if return_code:
                print >> sys.stderr, "Failed to extract information from VOMS %s m and g tables for user %s" % (vn,
                                                                                                          user.uname)
                continue
            # gets all roles
            command = "select g.dn,r.role from m m, roles r, groups  g where  r.rid=m.rid and g.gid=m.gid and " \
                      "m.userid =" + mid +" and r.role!=\'VO-Admin\';"
            group_role, return_code = MySQLUtils.RunQuery(command, connect_str,False)
            if return_code:
                print >> sys.stderr, "Failed to extract information from VOMS %s g and g tables for user %s" % (vn,
                                                                                                          user.uname)
            affiliation = affiliation+group_role

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
                                index = collaborations.index(cu) +1
                                user.add_cert(Certificate(index,subject, issuer))

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
                            print "disaster", new_umap.__dict__
                            sys.exit(1)
                        if umap.gid not in user.gids:
                            user.add_group(umap.gid)
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
                if not vo_groups.has_key(vo_group):
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
                            print >> sys.stderr, "Group gid %s for %s id not in gid.lis" % (gid,key)
                        else:
                            gmap.gid = gid
                        uname = element.attrib.get('accountName')

                        if uname and uname not in users.keys():
                            print >> sys.stderr, "User uname %s for %s id not in uid.lis" % (uname,key)
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

    validGroups = []
    for line in open(config["validgroups"], "r").readlines():
        validGroups.append(line.split()[0])

    for row in rows:
        if row["auth_string"] in users:
            if str(row["groupid"]) in validGroups:
                users[row["auth_string"]].add_group(row["groupid"], False)

    # ensure all Vulcan users are in us_cms (5063) group in Ferry
    cursor.execute("select u.userid, a.auth_string from users_t1 as u left join auth_tokens_t1 as a on u.userid = a.userid where a.auth_method = 'UNIX'")
    rows = cursor.fetchall()

    for row in rows:
        if row["auth_string"] in users:
            users[row["auth_string"]].add_group('5063', False)
            users[row["auth_string"]].add_to_vo('cms', 'https://voms2.cern.ch:8443/voms/cms')


    # fetch group leadership from vulcan
    cursor.execute("select l.*, a.auth_string from leader_group_t1 as l left join auth_tokens_t1 as a on l.userid = a.userid where a.auth_method = 'UNIX'")
    rows = cursor.fetchall()

    for row in rows:
        if row["auth_string"] in users:
            if str(row["groupid"]) in (item[0] for item in users[row["auth_string"]].gids):
                users[row["auth_string"]].set_leader(row["groupid"])
                #users[row["auth_string"]].add_group(row["groupid"], True)
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

    CADir = config.config._sections["CAs"]["cadir"]
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
                users[row["uname"]].add_certs(row["auth_string"], CA["subjectdn"], vo, url)

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
            print >> sys.stderr,"NIS domain doesn't exist for %s" % (vo.name)
            c = CollaborationUnit(domain)
            c.groups = nis.groups
            collaborations.append(c)
            c.set_id(len(collaborations))

    return collaborations

def read_nis(dir_path, exclude_list, altnames, users, groups, cms_groups):
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
            # print >> sys.stderr, "Skipping %s" % (dir,)
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
                print >> sys.stderr, "Domain: %s User %s in not in userdb!" % (dir,uname,)
                continue
            if uid != users[uname].uid:
                for u in users.values():
                    if u.uid == uid:
                        print >> sys.stderr, "Domain: %s user %s, %s has different uid (%s) in userdb! This userdb " \
                                             "uid (%s) is mapped to %s." % (dir, uname,uid, users[uname].uid, uid, u.uname)
                        print >> sys.stderr, "Assume that uid is correct, using %s" % (users[uname].uid,)

            if gid not in groups.values():
                print >> sys.stderr, "Domain: %s group %s doesn\'t exist in userdb!" % (dir,gid,)
                continue
            users[uname].compute_access[dir] = ComputeAccess(dir, gid, home_dir, shell)
            nis[dir].users[uname] = users[uname]
            nis[dir].primary_gid.append(gid)
            nis[dir].groups[groups.keys()[groups.values().index(gid)]] = gid

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
                print >> sys.stderr, "Domain: %s group %s from group filedoesn\'t exist  in userdb!" % (dir,gid,)
                continue
            if gname not in groups.keys():
                print >> sys.stderr, "Domain: %s group name %s, %s from group file doesn\'t exist  in userdb!" % \
                                     (dir,gname,gid)
                continue
            if gid != groups[gname]:
                print >> sys.stderr, "Domain: %s group %s from group file %s have different gid in userdb!" % \
                                     (dir,gname,gid,groups[gname])
                continue
            for uname in user_list:
                if uname not in users.keys():
                    print >> sys.stderr, "Domain: %s User %s in group file in not in userdb!" % (dir,uname,)
                    continue
                if dir in users[uname].compute_access:
                    users[uname].compute_access[dir].add_secondary_group(gid)
                    nis[dir].groups[gname]=gid
                else:
                    print >> sys.stderr, "Domain: %s User %s in group file but not in passwd!" % (dir,uname,)



    # add cms structure, this should modified not to have anything harcoded!!!!!
    dir = "cms"
    nis[dir] = ComputeResource( dir, dir,"Interactive","/uscms/home","/bin/tcsh")
    for gid in cms_groups:
        if gid not in groups.values():
            print >> sys.stderr, "Domain: %s group %s doesn\'t exist in userdb!" % (dir,gid,)
            continue
        gn = groups.keys()[groups.values().index(gid)]
        nis[dir].groups[gn]=gid
    gid = groups['us_cms']

    for uname, user in users.items():
        home = "/uscms/home/%s" % (uname)
        for tup in user.gids:
            if tup[0] != "5063":  # us_cms
                continue
            user.compute_access["cms"] = ComputeAccess("cms", gid, home, "/bin/tcsh" )
            nis[dir].users[uname] = user
            break
    nis[dir].primary_gid.append(groups['us_cms'])
    return nis

def populate_db(config, users, gids, vomss, gums, roles, collaborations, nis):
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
    fd = open("ferry.sql", "w")
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
        fd.write("insert into users values (%d,\'%s\',\'%s\',\'%s\',\'%s\',%s,%s, NOW());\n"
                 % (int(user.uid), user.uname, user.first_name, "", user.last_name, status,
                    user.expiration_date))
        # for now will just create ferry.sql file
        # results,return_code=MySQLUtils.RunQuery(command,connect_str)
        # if return_code!=0:
        #    print >> sys.stderr,'Error ', command

    # populate groups table with unix group

    group_counter = 1
    gid_map = {}
    for gname, index in gids.items():
        fd.write("insert into groups (gid,group_name,group_type,groupid) values (%d,\'%s\','UnixGroup',%d);\n" % (int(
            index), gname,group_counter))
        gid_map[index] = group_counter
        group_counter += 1
        # results,return_code=MySQLUtils.RunQuery(command,connect_str)
        # if return_code!=0:
        #    print >> sys.stderr,'Error ', command
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
        fd.write("insert into collaboration_unit (unit_name, voms_url, alternative_name, last_updated) " +\
            "values (\'%s\',%s,%s,NOW());\n" % (cu.name, link,alt_name))
        # populate collaboration unit groups

        for gid in cu.groups.values():
            index = gid_map[gid]
            is_primary = 0
            if cu.name in nis.keys():
                if index in nis[cu.name].primary_gid:
                    is_primary = 1
            else:
                    is_primary = 1
            fd.write("insert into collaboration_unit_group values(%d,%d,%s,NOW());\n" % (cu.unitid,index,is_primary))
    fd.flush()

    #populate compute_resource
    nis_counter = 0
    for cu in collaborations:
        if cu.name in nis.keys():
            nis_info = nis[cu.name]
        elif cu.alt_name in  nis.keys():
            nis_info = nis[cu.alt_name]
        else:
            print >> sys.stderr, "Neither %s not %s found in NIS" % (cu.name,cu.alt_name)
            continue
        nis_counter += 1
        fd.write("insert into compute_resource (compid,name, default_shell,default_home_dir,comp_type, unitid,last_updated)" +\
                 " values (\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',%s,NOW());\n" % (nis_counter, nis_info.cresource,
                                                                         nis_info.cshell,nis_info.chome,nis_info.ctype,cu.unitid))

        for uname, user in nis_info.users.items():
            comp = user.compute_access[nis_info.cresource]
            groupid = gid_map[comp.gid]
            fd.write("insert into compute_access (compid, uid, groupid,shell,home_dir,last_updated)" + \
                     " values (%s,%s,%s,\'%s\',\'%s\', NOW());\n" % (nis_counter,user.uid,groupid,comp.shell,comp.home_dir))
    fd.flush()
    # populate collaboration unit groups

    for gid in cu.groups.values():
        index = gid_map[gid]
        is_primary = 0
        if cu.name in nis.keys():
            if index in nis[cu.name].primary_gid:
                is_primary = 1
        else:
                is_primary = 0
        fd.write("insert into collaboration_unit_group values(%d,%d,%s,NOW());\n" % (cu.unitid,index,is_primary))
        fd.flush()


    # populating experiment_fqan table
    # GUMS darksidepro {'group': '/fermilab/darkside', 'server': 'fermilab', 'uname': 'darksidepro', 'gid': '9985', 'role': 'Production', 'user_group': 'darksidepro', 'account_mappers': 'darksidepro'}
    # experiment_fqan(fqanid, fqan, mapped_user,mapped_group);
    fqan_counter = 0
    for key, gmap in gums.items():
        fqan_counter += 1
        gname = gids.keys()[gids.values().index(gmap.gid)]
        #and gmap.uname in users.keys()
        if gmap.uname:
            un = "\'%s\'" % (gmap.uname)
        else:
            un='NULL'

        fd.write("insert into grid_fqan (fqan,mapped_user,mapped_group) values(\'%s/Role=%s\',%s,\'%s\');\n" % (gmap.group,gmap.role,un,gname))
        gmap.set_id(fqan_counter)
    fd.flush()

    experiment_counter =0
    for cu in collaborations:
        experiment_counter += 1
        if not isinstance(cu,VOMS):
            continue

        for uname, user in users.items():
            #if uname!='kherner':
            #    continue
            if user.vo_membership.has_key((cu.name,cu.url)):
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
                       fd.write("insert into user_certificate (uid,dn,issuer_ca,unitid,last_update) values (%d,\'%s\',"
                                "\'%s\',%d,NOW());\n" % (int(user.uid), certs.dn,certs.ca,experiment_counter))

                fd.flush()
    for uname, user in users.items():
        #if uname!='kherner':
        #    continue
        for gid, is_primary in user.gids:
            groupid = gid_map[gid]
            fd.write("insert into user_group values (%d,%d,%s);\n" % (int(user.uid), groupid, is_primary))
    fd.flush()
    fd.close()


if __name__ == "__main__":
    import os
    config = Configuration()
    config.configure(sys.argv[1])
    # read all information about users from uid.lis file
    users = read_uid(config.config.get("user_db", "uid_file"))

    # read all information about groups from gid.lis
    gids = read_gid(config.config.get("user_db", "gid_file"))

    # read services_user_files.csv and add this information to users containers
    read_services_users(config.config.get("user_db", "services_user_file"), users)

    # read Vulcan group memberships and add this information to users containers
    cms_groups=read_vulcan_user_group(config, users)

    # read NIS information
    nis_structure = read_nis(config.config.get("nis", "dir_path"),config.config.get("nis", "exclude_domain"),
                                 config.config.get("nis", "name_mapping"),users, gids,cms_groups)


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
                print >> sys.stderr,"NIS domain doesn't exist for %s" % (vo.name)
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
    populate_db(config.config, users, gids, vomss, gums, roles,collaborations, nis_structure )
