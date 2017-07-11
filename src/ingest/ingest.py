#!/usr/bin/env python

""" This script is uploading data from various source to FERRY database
"""
import sys
from Configuration import Configuration
from MySQLUtils import MySQLUtils
import xml.etree.ElementTree as ET


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

    def add_unix_gid(self, gid):
        self.gid = gid

    def add_roles(self, rnames):
        self.roles = rnames


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
        self.last_name = last_name.replace("'", "\\'")
        self.first_name = first_name.replace("'", "\\'")
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

    def add_group(self, gid):
        self.gids.append(gid)

    def set_expiration_date(self, dt):
        self.expiration_date = dt

    def set_status(self, status):
        self.status = status

    def add_certs(self, subject, issuer, vname, vurl):
        subject = subject.replace("'", "\\'")
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
        #try:
            if not len(line[:-1]):
                continue
            tmp = line[:-1].split("\t\t")
            if not usrs.has_key(tmp[4].strip().lower()):
                usrs[tmp[4].strip().lower()] = User(tmp[0].strip(), tmp[2].strip().lower().capitalize(),
                                                     tmp[3].strip().lower().capitalize(), tmp[4].strip().lower())
                usrs[tmp[4].strip().lower()].add_group((tmp[1].strip().lower()))
        #except:
        #    print >> sys.stderr, "Failed ", line
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


def populate_db(config, users, gids, vomss, roles):
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
    command = ""
    for user in users.values():
        if not user.expiration_date:
            user.expiration_date = "NULL"
        elif user.expiration_date == "EXPIRED":
            user.expiration_date = "NULL"
        elif user.expiration_date == "No Expiration date":
            user.expiration_date = "\'2038-01-01\'"
        else:
            user.expiration_date = "\'%s\'" % user.expiration_date
        fd.write("insert into users values (%d,\'%s\',\'%s\',\'%s\',\'%s\',\'%s.fnal.gov\',%s,%s, NOW());\n"
                 % (int(user.uid), user.uname, user.first_name, "", user.last_name, user.uname, user.status,
                    user.expiration_date))
        # for now will just create ferry.sql file
        # results,return_code=MySQLUtils.RunQuery(command,connect_str)
        # if return_code!=0:
        #    print >> sys.stderr,'Error ', command

    for gname, index in gids.items():
        fd.write("insert into groups values (%d,\'%s\','UnixGroup');\n" % (int(index), gname))
        # results,return_code=MySQLUtils.RunQuery(command,connect_str)
        # if return_code!=0:
        #    print >> sys.stderr,'Error ', command
    fd.flush()
    i = 1
    for role in roles:
        if not role:
            continue
        fd.write("insert into experiment_roles (roleid, role_name) values (%d,\'%s\');\n" % (i, role))
        i += 1
    fd.flush()
    i = 0
    for vos in vomss:
        for vname, vo in vos.items():
            i += 1
            fd.write("insert into experiments (expid,experiment_name,voms_url,last_updated) values (%d,\'%s\',"
                     "\'%s\'," "NOW());\n" % (i, vname, "https://"+vo.url))
            for uname, user in users.items():
                if user.vo_membership.has_key((vname,"https://"+vo.url)):

                    for umap in user.vo_membership[(vname, "https://"+vo.url)]:
                        if umap.role:
                            rid = roles.index(umap.role)
                            fd.write("insert into experiment_membership values  (%d,%d,%d,False,False,\'%s\',NOW(),"
                                     "%d);\n" % (int(user.uid), i, rid, umap.uname, int(umap.gid)))
                        else:
                            fd.write("insert into experiment_membership values  (%d,%d,NULL,False,False,\'%s\',NOW(),%d);\n"
                                     % (int(user.uid), i, umap.uname, int(umap.gid)))
                            for certs in user.certs:
                                if certs.vomsid == (vname, "https://"+vo.url):
                                    for k in range (0,len(certs.subjects)):
                                        fd.write("insert into user_certificate values (%d,\'%s\',\'%s\', NOW(),%d);\n"
                                             % (int(user.uid), certs.subjects[k], certs.issuers[k], i))
                        fd.flush()
    for uname, user in users.items():
        is_primary = False
        for gid in user.gids:
            fd.write("insert into user_group values (%d,%d,%s);\n" % (int(user.uid), int(gid), is_primary))
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


def get_vos(config, vn, vname,vurl, gids):
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
    groups, return_code = MySQLUtils.RunQuery(command, connect_str)
    volist = {}
    for g in groups:
        exp = g[1:].strip().split('/')
        if exp[0] == 'fermilab':
            if len(exp) == 1:
                name = exp[0]
            elif len(exp) > 2:
                name = exp[1] + exp[2]  # this is to handle marsmu2e
            else:
                name = exp[1]
        else:
            name = exp[0]

        #try:
        if not volist.has_key(name):
            volist[name] = VOMS(vurl, vname, name)
        if gids.has_key(name):
            volist[name].add_unix_gid(gids[name])
        #except:
        #    print >> sys.stderr, "group is not defined ", name
    return volist


def assign_vos(config, vn, vurl, rls, usrs, gums_map):
    """
    From VOMS database tries to get information about each user, certificate and group and role affiliation
    Args:
        config:
        vn:
        vurl:
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
        # do we want to have expired users in VOMS?
        total += 1
        if not user.is_k5login:
            continue

        command = "select u.userid,d.subject_string,c.subject_string from certificate d, ca c, usr u where u.userid = "\
                  "d.usr_id and c.cid=d.ca_id and (c.subject_string not like \'%HSM%\' and c.subject_string not like " \
                  "\'%Digi%\' and c.subject_string  not like \'%DOE%\') and u.dn like \'/DC=org/DC=cilogon/C=US/O=" \
                  "Fermi National Accelerator Laboratory/OU=People/CN=%/CN=UID:"+user.uname+"\';"
        members, return_code = MySQLUtils.RunQuery(command, connect_str, False)
        # user is not a member of VO
        if len(members[0].strip()) == 0:
            continue

        mids = []

        # add all additional certificates

        for m in members:
            member = m.split("\t")
            if member[0] not in mids:
                mids.append(member[0].strip())

        for mid in mids:
            command = "select distinct g.dn from m m, groups  g where g.gid=m.gid and " \
                      "m.userid =" + mid+";"
            affiliation, return_code = MySQLUtils.RunQuery(command, connect_str)

            command = "select g.dn,r.role from m m, roles r, groups  g where  r.rid=m.rid and g.gid=m.gid and " \
                      "m.userid =" + mid +" and r.role!=\'VO-Admin\';"
            group_role, return_code = MySQLUtils.RunQuery(command, connect_str)

            affiliation = affiliation+group_role

            for aff in affiliation:
                tmp = aff.split("\t")
                subgroup = tmp[0].strip()[1:]
                groups = subgroup.split('/')
                if len(groups) > 1 and groups[0] == 'fermilab':
                    group = "".join(groups[1:])
                    vurl = "%s/%s" % (vomsurl, group)
                else:
                    group = groups[0]
                    vurl = vomsurl
                user.add_to_vo(group, vurl)
                for m in members:
                    member = m.split("\t")
                    if member[0] == mid:
                        subject = member[1].strip()
                        issuer = member[2].strip()
                        user.add_certs(subject, issuer, group, vurl)
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
                        if umap.gid not in user.gids:
                            user.gids.append(umap.gid)
                        user.add_to_vo_role(group, vurl, new_umap)

                        break
                if role not in rls:
                    rls.append(role)


def read_gums_config(config, vname):
    """

    Args:
        config:
        vname:

    Returns:

    """
    gums_fn = config.config.get("voms_db_%s" % (vname,), "gums_config")
    tree = ET.parse(gums_fn)
    gums_mapping = {}
    # key vomsUserGroup
    root = tree.getroot()
    for child in root:
        if child.tag == "userGroups":
            for element in child.getchildren():
                voms_user_group = element.attrib.get('name')
                voms_server = element.attrib.get('vomsServer')
                vo_group = element.attrib.get('voGroup')
                if not vo_group:
                    continue
                role = element.attrib.get('role')
                gums_mapping[voms_user_group] = VOUserGroup(voms_user_group, voms_server, vo_group, role)

    for key, gmap in gums_mapping.items():
        for child in root:
            if child.tag == "groupToAccountMappings":
                for element in child.getchildren():
                    if element.attrib.get('userGroups') == key:
                        gmap.account_mappers = element.attrib.get('accountMappers')
                break
    for key, gmap in gums_mapping.items():
        for child in root:
            if child.tag == "accountMappers":
                for element in child.getchildren():
                    if element.attrib.get('name') == gmap.account_mappers:
                        gmap.gid = element.attrib.get('groupName')
                        gmap.uname = element.attrib.get('accountName')
                        break
    return gums_mapping


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

    # process voms information
    voms_instances = config.config.get("voms_instances", "list")
    voms_list = voms_instances.split(",")
    voms_list.sort()
    vomss = []
    roles = []
    for vn in voms_list:
        vomsurl = config.config.get("voms_db_%s" % (vn,), "url")

        url = vomsurl[8:]
        host = url[:url.find(":")]
        voname = url[url.rfind("/")+1:]
        # reads vo related information from VOMS
        vos = get_vos(config.config, vn, voname, url, gids)
        vomss.append(vos)
        gums = read_gums_config(config, vn)
        assign_vos(config.config, vn, vomsurl, roles, users, gums)
    for uname, user in users.items():
            if user.uname == "kherner":
                print user.uname
                for c in user.certs:
                    print c.__dict__
                for k,v in user.vo_membership.items():
                    for l in v:
                        print k, l.__dict__
    populate_db(config.config, users, gids, vomss, roles)
