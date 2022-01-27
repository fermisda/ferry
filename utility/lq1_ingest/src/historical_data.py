#######################################################################################################################
# Creates dictionary of valid users with                                                                              #
# primary and secondary groups.                                                                                       #
# Reads a historical passwd and group files                                                                           #
# Reads a historical project file that contains                                                                       #
# info project name and PI name                                                                                       #
# Communicates with Ferry to get all valid                                                                            #
# users and groups. Verifies that historical                                                                          #
# information is correct, flags invalid info                                                                          #
# and not active users, create a valid users                                                                          #
# dictionary                                                                                                          #
# Inserts in Ferry for each valid project                                                                             #
#  1. createGroup with project name and a type specified in config                                                    #
#  2. addUserToGroup add a PI of the project to this group with leader set to True                                    #
#  3. addUserToGroup add a member of the project to this group with leader set to False                               #
#  4. setUserAccessToComputeResource add a user to resource specified in config file, with bash and primary group     #
#  5. setUserAccessToComputeResource add a user to resource with all secondary groups                                 #
#######################################################################################################################
import sys
import argparse
import yaml

import ferry_execute


class Project:
    def __init__(self, pname, pi_user):
        self.project_name = pname
        self.pi_name = pi_user.uname
        self.members = [pi_user]


class User:
    def __init__(self, u_uname, u_full_name, u_uid, g_gid, u_home, u_shell):
        self.uname = u_uname
        self.uid = u_uid
        self.gid = g_gid
        self.gname = None
        self.full_name = u_full_name
        self.secondary_groups = []
        self.home = u_home
        self.shell = u_shell


class Group:
    def __init__(self, g_name, g_gid, u_list):
        self.gname = g_name
        self.gid = g_gid
        self.users = u_list


def read_passwd_file(fname):
    users = {}
    try:
        lines = open(fname).readlines()
    except IOError as e:
        print ('Error: could not open passwd file %s' % (fname,), e)
        sys.exit(1)
    except Exception as e:
        print ('Error: problems with reading passwd file %s' % (fname,), e)
        sys.exit(1)

    for line in lines:
        tmp = line[:-1].split(":")
        users[tmp[0]] = User(tmp[0], tmp[4], tmp[2], tmp[3], tmp[5], tmp[6].strip())
    return users


def read_group_file(fname, users):
    groups = {}
    try:
        lines = open(fname).readlines()
    except IOError as e:
        print ('Error: could not open group file %s' % (fname,), e)
        sys.exit(1)
    except Exception as e:
        print ('Error: problems with reading group file %s' % (fname,), e)
        sys.exit(1)

    for line in lines:
        # csi_batavia:x:8715:boyd,fkhan,tiradani
        valid_u_list = []
        tmp = line[:-1].split(":")
        u_list = tmp[3].strip().split(",")
        if len(u_list[0]):
            for u in u_list:
                if u not in users.keys():
                    print ("Error: the username %s is in the group file but not in the passwd file" % (u))
                    continue
                valid_u_list.append(u)
        groups[tmp[0]] = Group(tmp[0], tmp[2], valid_u_list)

    for uname, u in users.items():
        found = False
        for name, g in groups.items():
            if u.gid == g.gid:
                u.gname = name
                found = True
            elif uname in g.users:
                u.secondary_groups.append(name)
        if not found:
            print ("Error: the username %s have a group id %s that doesn't exist in the group file" % (u.uname, u.gid))
            users.pop(uname)
    return groups


def read_project_file(fname, users, groups):
    project_list = {}
    try:
        lines = open(fname).readlines()
    except IOError as e:
        print ('Error: could not open project file %s' % (fname,), e)
        sys.exit(1)
    except Exception as e:
        print ('Error: problems with reading project file %s' % (fname,), e)
        sys.exit(1)
    for line in lines:
        if line.startswith("#"):
            continue
        pname, pi = line[:-1].split(" ")

        if pname not in groups.keys():
            print ("Warning: the project %s doesn't have a corresponding unix group in group file!" % (pname))
            continue
        if pi not in users.keys():
            print ("Warning: the PI %s is not in passwd file" % (pi))
            continue
        project_list[pname] = Project(pname, users[pi])
    return project_list


def add_user_to_cluster(url, cert,cluster, u, is_test):
    cmd = "%s/setUserAccessToComputeResource?groupname=%s&homedir=%s&primary=True&resourcename" \
                  "=%s&username=%s&shell=%s" % (url, u.gname, u.home, cluster, u.uname, u.shell)
    execute(cert, cmd, is_test)
    for gname in u.secondary_groups:
        cmd = "%s/setUserAccessToComputeResource?groupname=%s&primary=False&resourcename" \
                      "=%s&username=%s" % (url, gname, cluster, u.uname)
        execute(cert, cmd, is_test)


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", type=str, default="../config/config.yaml", help="Config file")
    return parser.parse_args()


def read_config(fn):
    with open(fn, 'r') as f:
        config = yaml.load(f)
    return config

def execute(cert, cmd, is_test = False):
    if not is_test:
        print ("Info: Running ferry command")
    print (cmd)
    if not is_test:
        return ferry_execute.execute_ferry_api(cmd,cert)

def main():
    args = None
    config = None
    url = None
    cert = ()
    home_path = None
    shell_default = None
    project_name = None
    cluster = None
    group_type = None
    passwd_file = None
    group_file = None
    is_test = True

    try:
        args = parse_arguments()
    except Exception as e:
        print ('Error: could not parse arguments', e)
    try:
        config = read_config(args.config)
        url = config["ferry"]["ferry_url"]
        cert = (config["ferry"]["cert_pem"], config["ferry"]["cert_key"])
        home_path = config["project"]["homedir_default"]
        shell_default = config["project"]["shell_default"]
        cluster = config["project"]["cluster"]
        group_type = config["project"]["group_type"]
        passwd_file = config["data"]["passwd"]
        group_file = config["data"]["group"]
        project_file = config["data"]["projects"]
	is_test = config["ferry"]["test"]
    except IOError as e:
        print ('Error: could not open config file %s' % (args.config,), e)
        sys.exit(1)
    except KeyError as e:
        print ("Error: not found in configuration:", e)
        sys.exit(1)
    except Exception as e:
        print ('Error: problems with reading config file %s' % (args.config,), e)
        sys.exit(1)

    users = read_passwd_file(passwd_file)
    groups = read_group_file(group_file, users)
    projects = read_project_file(project_file, users, groups)


    cmd = "%s/getAllUsers" % (url)
    ferry_users = execute(cert, cmd)
    # [{u'status': False, u'username': u'131.225.107.4', u'fullname': u'System Administrators', u'expirationdate':
    # None, u'uid': 44673},..]
    not_found_users = []

    # checks if users in passwd file are valid users in ferry: checks uid for each uname,
    # checks if uname is in ferry, set shell nologin for all inactive users
    for uname, u in users.items():
        found = False
        for f_u in ferry_users:
            if f_u["username"] == uname:
                found = True
                if not f_u["status"]:
                    print ("Warning: the username %s has inactive status in Ferry. Replacing shell with nologin" % (
                        uname))
                    users[uname].shell = "/sbin/nologin"
                if int(f_u["uid"]) != int(u.uid):
                    print("Error: the username %s uid %s in passwd file doesn't match the uid %s in Ferry" % (uname,
                                                                                                            u.uid,
                                                                                                       f_u["uid"]))
                    users.pop(uname)
                    continue

        if not found:
            print ("Error: could not find the uname %s in Ferry" % uname)
            users.pop(uname)

    cmd = "%s/getAllGroups?grouptype=UnixGroup" % (url)
    ferry_groups = execute(cert, cmd)
    # {"gid":5777,"groupname":"bp202","grouptype":"UnixGroup"},

    # checks if group in groups file corresponds to ferry info:
    # verifies of a gname is in Fery ,and gid is correct for a gname,
    for gname, g in groups.items():
        found = False
        for f_g in ferry_groups:
            if gname == f_g["groupname"]:
                found = True
                if int(g.gid) != int(f_g["gid"]):
                    print ("Error: The group ids of groupname %s are different in group file (%s) and Ferry (%s)" % (
                        gname,
                                                                                                              g.gid,
                                                                                                              f_g[
                                                                                                                  "gid"]))
                    groups.pop(gname)
                    break
        if not found:
            print ("Error: the groupname %s is not in Ferry" % (gname))
            groups.pop(gname)

    if is_test:
        print ("Passwd file:")
        for uname, u in users.items():
            if u.gname in groups.keys():
                # hamir:x:58490:9540:Yigal Shamir:/home/shamir:/usr/bin/tcsh
                print ("%s:x:%s:%s:%s:%s:%s" % (uname, u.uid, u.gid, u.full_name, u.home, u.shell))

        print("***********************************")
        print ("Group file:")
        for gname, g in groups.items():
            # hiq2ff:x:8859:atlytle,cdavies,jkoponen,lepage
            valid_users = []
            for uname in g.users:
                if uname in users.keys():
                    valid_users.append(uname)
            print ("%s:x:%s:%s" % (gname, g.gid, ",".join(valid_users)))
        print("***********************************")

    # checks that pi (uname) in project file is valid, adds all members - people that have primary or secondary group
    #  as a project name (gname) to a project members list
    for pname, p in projects.items():
        for uname, u in users.items():
            if uname == p.pi_name:
                continue
            if u.gname == pname or pname in u.secondary_groups:
                projects[pname].members.append(u)
    #create a cluster
    cmd = "%s/createComputeResource?resourcename=%s&resourcetype=Batch&homedir=%s&shell=%s" % (url,cluster,
                                                                                        home_path,shell_default)
    execute(cert, cmd, is_test)

    # creates a project
    # adds all members to their project group Type LQ1Cluster
    # adds all members to the cluster
    project_members = []
    for pname, p in projects.items():
        cmd = "%s/createGroup?groupname=%s&grouptype=%s" % (url, pname, group_type)
        execute(cert, cmd, is_test)
        for u in p.members:
            leader = "False"
            if u.uname == p.pi_name:
                leader = "True"
            cmd = "%s/addUserToGroup?username=%s&groupname=%s&grouptype=%s&leader=%s" % (url, u.uname, pname,
                                                                                         group_type, leader)
            execute(cert, cmd, is_test)
            add_user_to_cluster(url, cert,cluster, u, is_test)
            if u.uname not in project_members:
                project_members.append(u.uname)
        execute(cert, cmd, is_test)


    for uname, u in users.items():
        if uname in project_members:
            continue
        print ("Warning unattached member %s added to %s cluster " % (u.uname,cluster))
        #Now we need to insert in the cluster people who are not in any project but historically had an account on lq1
        add_user_to_cluster(url, cert,cluster, u, is_test)

    if is_test:
        for pname, p in projects.items():
            print ("************Project: %s **********" % (pname))
            print ("PI: %s" % (p.pi_name))
            members = []
            for u in p.members:
                members.append("%s (%s)" % (u.full_name,u.uname))
            print ("Members: %s" % ", ".join(members))
if __name__ == "__main__":
    main()
