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
    def __init__(self,url,voname, experiment):
        self.name = experiment
        self.url = url
        if experiment != voname:
            self.url = "%s/%s" % (url,experiment)
        self.gid = 0
        self.roles = []

    def add_unix_gid(self, gid):
        self.gid = gid

    def add_roles (self, rnames):
        self.roles = rnames

class VOUserGroup:
    """
    GUMS mapping from configuration file
    """

    def __init__(self,vomsUserGroup,vomsServer,voGroup,role):
        self.user_group = vomsUserGroup
        self.server = vomsServer
        self.group = voGroup
        self.role = role
        self.uname = None
        self.account_mappers = None
        self.gid = None


class Certificate:
    """
    Certificate class stores cert issuer, subject and experiment (name and url)
    """
    def __init__(self,subject, issuer, vomsname,vomsurl):
        self.vomsid = (vomsname, vomsurl)
        self.subject = subject
        self.issuer = issuer

class User:
    """
    User class represents all information about user gathered from various sources
    """

    def __init__(self, uid, last_name, first_name, uname):
        self.uid = uid
        if last_name.find("'") > 0:
            tmp=last_name.split("'")
            last_name = "%s'%s" % (tmp[0],tmp[1].capitalize())
        self.last_name = last_name.replace("'","\\'")
        self.first_name = first_name.replace("'","\\'")
        self.uname = uname
        self.gids = []
        self.expiration_date = None
        self.certs=[]
        self.status = 1
        self.vo_membership = {}
        self.is_k5login = False

    def add_to_vo(self,voname,vomsurl):
        if not self.vo_membership.has_key((voname,vomsurl)):
            self.vo_membership[(voname,vomsurl)] = []

    def add_to_vo_role(self,voname,vomsurl,gums_mapping):
        self.vo_membership[(voname,vomsurl)].append(gums_mapping)

    def add_group(self, gid):
        self.gids.append(gid)

    def set_expiration_date(self,dt):
        self.expiration_date = dt

    def set_status(self,status):
        self.status = status

    def add_certs(self,subject,issuer,vomsname,vomsurl):
        subject = subject.replace("'","\\'")
        cert = Certificate(subject,issuer,vomsname,vomsurl)
        for c in self.certs:
            if c.vomsid == (vomsname,vomsurl):
                return
        self.certs.append(cert)

def read_uid(fname):
    fd = open(fname)
    users = {}
    for line in fd.readlines():
        try:
            if  not len(line[:-1]):
                continue
            tmp=line[:-1].split("\t\t")
            if not users.has_key(tmp[4].strip().lower()):
                users[tmp[4].strip().lower()]=User(tmp[0].strip(), tmp[2].strip().lower().capitalize(),
                                                   tmp[3].strip().lower().capitalize(),tmp[4].strip().lower())
                users[tmp[4].strip().lower()].add_group((tmp[1].strip().lower()))
        except:
            print >> sys.stderr, "Failed ", line
    return users

def read_gid(fname):
    fd = open(fname)
    gids = {}
    for line in fd.readlines():
        if  not len(line[:-1]):
            continue
        try:
            tmp=line[:-1].strip("\t").split("\t")
            gids[tmp[1].strip().lower()] = tmp[0].strip()

        except:
            print >> sys.stderr, " group Failed ", line
    return gids


def populate_db(config,users,gids,vomss,roles):
    """
    create mysql dump for ferry database
    :param config:
    :param users:
    :param gids:
    :param vos:
    :param roles:
    :return:
    """
    mysql_client_cfg=MySQLUtils.createClientConfig("main_db",config)
    connect_str=MySQLUtils.getDbConnection("main_db",mysql_client_cfg,config)
    fd=open("ferry.sql","w")
    command = ""
    for user in users.values():
        if not user.expiration_date:
            user.expiration_date = "NULL"
        elif user.expiration_date == "EXPIRED":
            user.expiration_date = "NULL"
        elif user.expiration_date == "No Expiration date":
            user.expiration_date = "\'2038-01-01\'"
        else:
            user.expiration_date = ("\'%s\'") % user.expiration_date
        fd.write("insert into users values (%d,\'%s\',\'%s\',\'%s\',\'%s\',\'%s.fnal.gov\',%d,%s, NOW());\n"
                 % (int(user.uid), user.uname,user.first_name, "",user.last_name, user.uname,user.status,
                    user.expiration_date))
        # for now will just create ferry.sql file
        # results,return_code=MySQLUtils.RunQuery(command,connect_str)
        # if return_code!=0:
        #    print >> sys.stderr,'Error ', command

    for gname,id in gids.items():
        fd.write("insert into groups values (%d,\'%s\','UnixGroup');\n" % (int(id),gname))
        # results,return_code=MySQLUtils.RunQuery(command,connect_str)
        # if return_code!=0:
        #    print >> sys.stderr,'Error ', command
    fd.flush()
    i=1
    for role in roles:
        if not role:
            continue
        fd.write("insert into experiment_roles (roleid, role_name) values (%d,\'%s\');\n" % (i,role))
        i +=1
    fd.flush()
    i = 0
    for vos in vomss:
        for voname,vo in vos.items():
            i += 1
            fd.write("insert into experiments (expid,experiment_name,gid,voms_url,last_updated) values (%d,\'%s\',%d,"
                     "\'%s\',"
                     "NOW());\n" % (i,voname, int(vo.gid), vo.url))
            for uname, user in users.items():
                if user.vo_membership.has_key((voname,vo.url)):
                    for map in  user.vo_membership[(voname,vo.url)]:
                        if map.role:
                            rid=roles.index(map.role)
                            fd.write("insert into experiment_membership values  (%d,%d,%d,0,0,\'%s\',NOW(),%d);\n" % (int(
                                user.uid),i,rid,map.uname,int(map.gid)))
                        else:
                            fd.write("insert into experiment_membership values  (%d,%d,NULL,0,0,\'%s\',NOW(),%d);\n" % (int(
                                user.uid),i,map.uname,int(map.gid)))
                        for cert in user.certs:
                            if cert.vomsid == (voname,vo.url):
                                fd.write("insert into user_certificate values (%d,\'%s\',\'%s\', NOW(),%d);\n" % (int(
                                user.uid), cert.subject,cert.issuer,i))
                        fd.flush()
    for uname, user in users.items():
        is_primary = 1
        for gid in user.gids:
            fd.write("insert into user_group values (%d,%d,%d);\n" % (int(user.uid),int(gid),is_primary))
            is_primary = 0
    fd.flush()
    fd.close()

def read_services_users(fname,users):
    fd = open(fname)
    for line in fd.readlines():

        if line.startswith("#"):
            continue
        try:
            tmp=line[:-1].split(",")
            if users.has_key(tmp[0]):
                if tmp[2] != "No Expiration date":
                    if  tmp[2] =="EXPIRED":
                        users[tmp[0]].set_status(0)
                users[tmp[0]].set_expiration_date(tmp[2].strip())
                users[tmp[0]].is_k5login = True
        except:
            print >> sys.stderr, "csv Failed ", line

def get_vos(config, vn, host,voname,gids):
    """
    Read data from VOMS databases
    :param config:
    :param host:
    :param voname:
    :param gids:
    :return:
    """
    mysql_client_cfg = MySQLUtils.createClientConfig("voms_db_%s" % (vn,), config)
    connect_str = MySQLUtils.getDbConnection("voms_db_%s" % (vn,), mysql_client_cfg, config)
    command = "select dn from groups"
    groups,return_code = MySQLUtils.RunQuery(command, connect_str)
    vos={}
    for g in groups:
        exp = g[1:].strip().split('/')
        if exp[0] == 'fermilab':
            if len(exp) == 1:
                name = exp[0]
            elif len(exp) > 2:
                name = exp[1]+exp[2] # this is to handle marsmu2e
            else:
                name = exp[1]
        else:
            name = exp[0]

        try:
            if not vos.has_key(name):
                vos[name] =  VOMS(vomsurl, voname, name)
            if gids.has_key(name):
                vos[name].add_unix_gid(gids[name])
        except:
            print  >> sys.stderr,"group is not defined ", name
    return vos

def assign_vos(config, vn, vomsurl, host, voname, vos, roles, users, gids, gums):
    """
    From VOMS database tries to get information about each user, certificate and group and role affiliation
    :param config:
    :param host:
    :param voname:
    :param vos:
    :param roles:
    :param users:
    :param gids:
    :param gums:
    :return:
    """
    mysql_client_cfg=MySQLUtils.createClientConfig("voms_db_%s" % (vn,),config)
    connect_str=MySQLUtils.getDbConnection("voms_db_%s" % (vn,),mysql_client_cfg,config)
    # for all users in user-services.csv
    counter=0
    total =0

    for uname,user in users.items():
        # do we want to have expired users in VOMS?
        total += 1
        if not user.is_k5login:
            continue

        command = "select u.userid,d.subject_string,c.subject_string from certificate d, ca c, usr u where u.userid = " \
              "d.usr_id and c.cid=d.ca_id and (c.subject_string not like \'%HSM%\' and c.subject_string not like " \
              "\'%Digi%\' and c.subject_string  not like \'%DOE%\') and u.dn like \'/DC=org/DC=cilogon/C=US/O=Fermi " \
                  "National Accelerator Laboratory/OU=People/CN=%/CN=UID:"+user.uname+"\';"
        members,return_code = MySQLUtils.RunQuery(command,connect_str,False)
        # user is not a member of VO
        if len(members[0].strip()) == 0:
            continue

        mids = []

        # add all additional certificates

        for m in members:
            member=m.split("\t")
            if member[0] not in mids:
                mids.append(member[0])


        for mid in mids:
            command = "select distinct g.dn from m m, groups  g where g.gid=m.gid and " \
                      "m.userid =" + mid+";"
            affiliation,return_code = MySQLUtils.RunQuery(command,connect_str)

            command = "select g.dn,r.role from m m, roles r, groups  g where  r.rid=m.rid and g.gid=m.gid and " \
                      "m.userid =" + member[0].strip()+" and r.role!=\'VO-Admin\';"
            group_role,return_code = MySQLUtils.RunQuery(command,connect_str)

            affiliation = affiliation+group_role

            for aff in affiliation:
                tmp = aff.split("\t")
                subgroup = tmp[0].strip()[1:]
                groups = subgroup.split('/')
                if len(groups) > 1 and groups[0] == 'fermilab':
                    group = "".join(groups[1:])
                    url = "%s/%s" % (vomsurl,group)
                else:
                    group = groups[0]
                    url=vomsurl
                user.add_to_vo(group,url)
                for m in members:
                    member=m.split("\t")
                    if member[0] == mid:
                        subject = member[1].strip()
                        issuer =  member[2].strip()
                        user.add_certs(subject,issuer,group,url)
                if len(tmp) > 1:
                    role=tmp[1].strip()
                else:
                    role = None

                for key,map in gums.items():

                    if "/%s" % (subgroup,) == map.group and role == map.role:
                        if not map.uname:
                            account_name = user.uname
                        else:
                            account_name = map.uname

                        new_map=VOUserGroup(map.user_group,map.server,map.group,map.role)
                        new_map.uname=account_name
                        new_map.gid=map.gid
                        if map.gid not in user.gids:
                            user.gids.append(map.gid)
                        user.add_to_vo_role(group,url, new_map)

                        break
                if role not in roles:
                    roles.append(role)


def read_gums_config(config, vn):
    gums_fn = config.config.get("voms_db_%s" % (vn,), "gums_config")
    tree = ET.parse(gums_fn)
    gums={}
    # key vomsUserGroup
    root = tree.getroot()
    for child in root:
        if child.tag == "userGroups":
            for element in child.getchildren():
                vomsUserGroup = element.attrib.get('name')
                vomsServer = element.attrib.get('vomsServer')
                voGroup = element.attrib.get('voGroup')
                if not voGroup:
                    continue
                role = element.attrib.get('role')
                userGroup = VOUserGroup(vomsUserGroup,vomsServer,voGroup,role)
                gums[vomsUserGroup]=userGroup

    for key,gmap in gums.items():
        for child in root:
            if child.tag == "groupToAccountMappings":
                for element in child.getchildren():
                    if element.attrib.get('userGroups') == key:
                        gmap.account_mappers = element.attrib.get('accountMappers')
                break
    for key,gmap in gums.items():
        for child in root:
            if child.tag == "accountMappers":
                for element in child.getchildren():
                    if element.attrib.get('name') == gmap.account_mappers:
                        gmap.gid = element.attrib.get('groupName')
                        gmap.uname = element.attrib.get('accountName')
                        break
    return gums



if __name__ == "__main__":
    config = Configuration()
    config.configure(sys.argv[1])
    # read all information about users from uid.lis file
    users = read_uid(config.config.get("user_db", "uid_file"))

    # read all information about groups from gid.lis
    gids = read_gid(config.config.get("user_db", "gid_file"))

    # read services_user_files.csv and add this information to users containers
    read_services_users(config.config.get("user_db", "services_user_file"),users)

    # process voms information
    voms_instances =config.config.get("voms_instances", "list")
    voms_list = voms_instances.split(",")
    voms_list.sort()
    vomss = []
    roles = []
    for vn in voms_list:
        vomsurl = config.config.get("voms_db_%s" % (vn,), "url")

        url = vomsurl[8:]
        host = url[:url.find(":")]
        voname = url[url.rfind("/")+1:]
        # reads vo related informatmation from VOMS
        vos = get_vos(config.config, vn, host, voname, gids)
        vomss.append(vos)
        gums = read_gums_config(config, vn)
        assign_vos(config.config, vn, vomsurl,host, voname, vos, roles, users, gids, gums)
    populate_db(config.config, users, gids, vomss, roles)