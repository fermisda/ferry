#!/usr/bin/env python

""" This script is uploading data from various source to FERRY database
"""
import sys
from Configuration import Configuration
from MySQLUtils import MySQLUtils
import xml.etree.ElementTree as ET
import psycopg2 as pg
import psycopg2.extras
import fetchcas as ca


class VOMS:
    """
    VOMS presents information about VOMS instance groups and roles
    voname is a name of VOMS
    experiment could be either a VO name or subgroup as in case of fermilab VO
    It is planning to address any generic case it just deal with what we have now
    """
    def __init__(self, vurl, vo_name, experiment):
        self.name = experiment
        self.url = vurl
        if experiment != vo_name:
            self.url = "%s/%s" % (url, experiment)
        self.gid = 0
        self.roles = []
        self.expid = 0

    def add_unix_gid(self, gid):
        self.gid = gid

    def add_roles(self, rnames):
        self.roles = rnames

    def set_id(self, index):
        self.expid = index

class VOUserGroup:
    """
    GUMS mapping from configuration file
    """

    def __init__(self, voms_user_group, voms_server, vo_group, role):
        self.user_group = voms_user_group
        self.server = voms_server
        self.group = vo_group
        self.role = role
        self.uname = None
        self.account_mappers = None
        self.gid = None
        self.fqanid = 0

    def set_id(self, index):
        self.fqanid = index


class Certificates:
    """
    Certificate class stores cert issuer, subject and experiment (name and url)
    """
    def __init__(self, subject, issuer, vomsname, vurl):
        self.vomsid = (vomsname, vurl)
        self.subjects = [subject,]
        self.issuers = [issuer,]

    def add_cert(self,subject, issuer):
        if subject not in self.subjects:
            self.subjects.append(subject)
            self.issuers.append(issuer)


class User:
    """
    User class represents all information about user gathered from various sources
    """

    def __init__(self, uid, last_name, first_name, uname):
        self.uid = uid
        if last_name.find("'") > 0:
            tmp = last_name.split("'")
            last_name = "%s'%s" % (tmp[0], tmp[1].capitalize())
        self.last_name = last_name.replace("'", "''")
        self.first_name = first_name.replace("'", "''")
        self.uname = uname
        self.gids = []
        self.expiration_date = None
        self.certs = []
        self.status = True
        self.vo_membership = {}
        self.is_k5login = False

    def add_to_vo(self, vname, vurl):
        if not self.vo_membership.has_key((vname, vurl)):
            self.vo_membership[(vname, vurl)] = []

    def add_to_vo_role(self, vname, vurl, gums_mapping):
        self.vo_membership[(vname, vurl)].append(gums_mapping)

    def add_group(self, gid, leader = False):
        if (str(gid), leader) not in self.gids:
            self.gids.append((str(gid), leader))
        if (str(gid), not leader) in self.gids:
            self.gids.remove((str(gid), not leader))

    def set_expiration_date(self, dt):
        self.expiration_date = dt

    def set_status(self, status):
        self.status = status

    def add_certs(self, subject, issuer, vname, vurl):
        subject = subject.replace("'", "''")
        for c in self.certs:
            if c.vomsid == (vname, vurl):
                c.add_cert(subject, issuer)
                return
        cert = Certificates(subject, issuer, vname, vurl)
        self.certs.append(cert)


def read_uid(fname):
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
    fd = open(fname)
    groupids = {}
    for line in fd.readlines():
        if not len(line[:-1]):
            continue
        try:
            tmp = line[:-1].strip("\t").split("\t")
            groupids[tmp[1].strip().lower()] = tmp[0].strip()

        except:
            print >> sys.stderr, " group Failed ", line
    return groupids


def populate_db(config, users, gids, vomss, gums, roles):
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
        fd.write("insert into users values (%d,\'%s\',\'%s\',\'%s\',\'%s\',%s,%s, NOW());\n"
                 % (int(user.uid), user.uname, user.first_name, "", user.last_name, user.status,
                    user.expiration_date))
        # for now will just create ferry.sql file
        # results,return_code=MySQLUtils.RunQuery(command,connect_str)
        # if return_code!=0:
        #    print >> sys.stderr,'Error ', command

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
    # populating experiment_fqan table
    # GUMS darksidepro {'group': '/fermilab/darkside', 'server': 'fermilab', 'uname': 'darksidepro', 'gid': '9985', 'role': 'Production', 'user_group': 'darksidepro', 'account_mappers': 'darksidepro'}
    # experiment_fqan(fqanid, fqan, mapped_user,mapped_group);
    fqan_counter = 0
    for key, gmap in gums.items():
        fqan_counter += 1
        gname = gids.keys()[gids.values().index(gmap.gid)]
        un=gmap.uname
        if gmap.uname:
            un = "\'%s\'" % (gmap.uname)
        else:
            un='NULL'
        fd.write("insert into grid_fqan (fqanid,fqan,mapped_user,mapped_group) values(%d,\'%s/Role=%s\',%s,\'%s\');\n" %
                 (fqan_counter,gmap.group,gmap.role,un,gname))
        gmap.set_id(fqan_counter)
    fd.flush()

    experiment_counter = 0

    for vos in vomss:
        for vname, vo in vos.items():
            experiment_counter += 1
            fd.write("insert into collaboration_unit (unitid,unit_name,voms_url,alternative_name,last_updated) values ("
                     "%d,\'%s\',"
                     "\'%s\',\'\',NOW());\n" % (experiment_counter,vname, vo.url))
            vo.set_id(experiment_counter)

            for uname, user in users.items():

                #if uname!='kherner':
                #    continue

                if user.vo_membership.has_key((vname,vo.url)):
                    for umap in user.vo_membership[(vname, vo.url)]:
                        fqanid = 0
                        for gmap in gums.values():

                            if  umap.group == gmap.group and umap.role == gmap.role:
                                fqanid = gmap.fqanid
                                break
                        fd.write("insert into grid_access values  (%d,%d,%d,False,False,NOW());\n" % \
                                 (int(user.uid),experiment_counter, fqanid, ))

                    for certs in user.certs:
                        if certs.vomsid == (vname, vo.url):
                            for k in range (0,len(certs.subjects)):
                                fd.write("insert into user_certificate values (%d,\'%s\',%d,\'%s\',NOW());\n"
                                     % (int(user.uid), certs.subjects[k], experiment_counter, certs.issuers[k]))

                    fd.flush()
    for uname, user in users.items():
        #if uname!='kherner':
        #    continue
        for gid, is_primary in user.gids:
            groupid = gid_map[gid]
            fd.write("insert into user_group values (%d,%d,%s);\n" % (int(user.uid), groupid, is_primary))
    fd.flush()
    fd.close()


def read_services_users(fname, users):
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

    Returns:

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
                volist[name].add_unix_gid(gids[name])
        except:
            print >> sys.stderr, "group is not defined ", name
    return volist


def assign_vos(config, vn, vurl, rls, usrs, gums_map):
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
                  "\'%" + uname + "%\' or d.subject_string like \'%" + uname + "%\'));"
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
            group_role, return_code = MySQLUtils.RunQuery(command, connect_str)
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
                        user.add_certs(subject, issuer, group, url)
                if len(tmp) > 1:
                    role = tmp[1].strip()
                else:
                    role = None
                # print "%s /%s %s %s" % (user.uname, subgroup, role, url)
                for key, umap in gums_map.items():
                    # if "/%s" % (subgroup,) == umap.group:
                    #    print "gums_dict", umap.__dict__
                    if "/%s" % (subgroup,) == umap.group and role == umap.role:
                        # print "found ",umap.group, umap.role
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


def read_gums_config(config):
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
                        gmap.gid = element.attrib.get('groupName')
                        gmap.uname = element.attrib.get('accountName')
                        break
    return gums_mapping

def read_vulcan_user_group(config, users):
    config = config.config._sections["vulcan"]
    conn_string = "host='%s' dbname='%s' user='%s' password='%s'" % \
                  (config["hostname"], config["database"], config["username"], config["password"])
    print conn_string
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
                users[row["auth_string"]].add_group(row["groupid"], True)

def read_vulcan_certificates(config, users, vomss):
    cfg = config.config._sections["vulcan"]
    conn_string = "host='%s' dbname='%s' user='%s' password='%s'" % \
                  (cfg["hostname"], cfg["database"], cfg["username"], cfg["password"])
    print conn_string
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
    read_vulcan_user_group(config, users)

    # process voms information; list of VOMS instances should be in configuration file
    voms_instances = config.config.get("voms_instances", "list")
    voms_list = voms_instances.split(",")
    voms_list.sort()
    vomss = [] # list of VOs from VOMS instances
    roles = []
    gums = read_gums_config(config) # List of GUMS group mapping
    # read Vulcan X509 certificates and voms and add this information to proper containers
    read_vulcan_certificates(config, users, vomss)
    for vn in voms_list:
        url = config.config.get("voms_db_%s" % (vn,), "url")
        # reads vo related information from VOMS
        vos = get_vos(config.config, vn, url, gids)
        vomss.append(vos)
        assign_vos(config.config, vn, url, roles, users, gums)

    #for uname, user in users.items():
    #        if user.uname == "kherner":
    #            print user.uname
    #            for c in user.certs:
    #                print c.__dict__
    #            for k,v in user.vo_membership.items():
    #                for l in v:
    #                    print k, l.__dict__
    populate_db(config.config, users, gids, vomss, gums, roles)
