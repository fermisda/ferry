#!/usr/bin/env python

import os
import sys
import configparser
import urllib.request
import re
import datetime
import psycopg2
import psycopg2.extras

SOURCES = ['uid.lis', 'gid.lis', 'services-users.csv']

def update_users(cursor, uid_lis, services_users_csv):
    """
    Reads data from uid.lis and service_user.csv and updates Ferry users table accordingly.
    """
    ferry_users = {}  # {uname: (full_name, expiration_date)}
    userdb_users = {} # {uname: [uid, full_name, expiration_date]}
    actions = ''

    # Parses dates to Ferry format
    def DateSwitcher(date):
        if not date or date == 'EXPIRED':
            return 'Null'
        if date == 'No Expiration date':
            return "'2038-01-01'"
        if isinstance(date, datetime.date):
            return "'%s'" % date.strftime('%Y-%m-%d')
        return "'%s'" % date

    # Parses Ferry users table into a dictionary
    cursor.execute('select * from users')
    table = cursor.fetchall()
    for row in table:
        ferry_users[row['uname']] = (row['full_name'], DateSwitcher(row['expiration_date']))

    # Parses UserDB users into a dictionary
    lines = open(uid_lis).readlines()
    for line in lines:
        line = re.findall('(\d+)\t\t(\d+)\t\t(.+)\t\t(.+)\t\t(.+)', line)
        if len(line) == 1:
            uid, gid, last_name, first_name, uname = line[0]
            uname = uname.lower().strip()
            full_name = ' '.join([first_name.strip().capitalize(), last_name.strip().capitalize()]).strip()
            userdb_users[uname] = [uid, full_name, 'Null']
    lines = open(services_users_csv).readlines()
    for line in lines:
        line = re.findall('(\w+)\,(\".+\"),(No\sExpiration\sdate|\d{4}-\d{2}-\d{2})', line)
        if len(line) == 1:
            uname, full_name, exp_date = line[0]
            full_name = full_name.strip('"')
            exp_date = DateSwitcher(exp_date)
            if uname in userdb_users:
                userdb_users[uname][1] = full_name
                userdb_users[uname][2] = exp_date

    # Compares data in UserDB with Ferry database and updates it as necessary
    for uname in userdb_users:
            if uname in ferry_users:
                if userdb_users[uname][1] != ferry_users[uname][0]:
                    actions += 'UPDATE users SET full_name = \'%s\', last_updated = NOW() where uname = \'%s\';\n' \
                            % (userdb_users[uname][1].replace("'", "''"), uname)
                if userdb_users[uname][2] != ferry_users[uname][1]:
                    if userdb_users[uname][2] != 'Null':
                        status = 'True'
                    else:
                        status = 'False'
                    actions += 'UPDATE users SET expiration_date = %s, status = %s, last_updated = NOW() where uname = \'%s\';\n' \
                            % (userdb_users[uname][2], status, uname)
            else:
                if userdb_users[uname][2] != 'Null':
                    status = 'True'
                else:
                    status = 'False'
                actions += "INSERT INTO users (uid, uname, full_name, status, expiration_date, last_updated) VALUES (%s, \'%s\', \'%s\', %s, %s, NOW());\n" \
                        % (userdb_users[uname][0], uname, userdb_users[uname][1].replace("'", "''"), status, userdb_users[uname][2])

    return actions

def update_groups(cursor, gid_lis):
    """
    Reads data from gid.lis and updates Ferry groups table accordingly.
    """
    groups = [] # [gpname1, gpname2, ...]
    actions = ''

    # Parses Ferry groups table into a list of group names
    cursor.execute('select * from groups')
    table = cursor.fetchall()
    for row in table:
        groups.append(row['name'])

    # Compares gid.lis with Ferry user_group table and updates it accordingly
    lines = open(gid_lis).readlines()
    for line in lines:
        line = re.findall('(\d+)\t(.+)\t\t.+', line)
        if len(line) == 1:
            gid, name = line[0]
            name = name.strip().lower()
            if name not in groups:
                actions += "INSERT INTO groups (gid, name, type, last_updated) values (%s, \'%s\', 'UnixGroup', NOW());\n" \
                        % (gid, name)
    
    return actions

def update_user_group(cursor, uid_lis):
    """
    Reads data from uid.lis and updates Ferry user_group table accordingly.
    """
    groups = {} # {gid: (groupid, [list of users])}
    uids = []   # [uid1, uid2, ...]
    actions = ''

    # Parses Ferry users table into a list of uids
    cursor.execute('select * from users')
    table = cursor.fetchall()
    for row in table:
        uids.append(str(row['uid']))

    # Parses Ferry user_group table into a dictionary
    cursor.execute('select uid, gid, groups.groupid from user_group left join groups on user_group.groupid = groups.groupid')
    table = cursor.fetchall()
    for row in table:
        if str(row['gid']) not in groups:
            groups[str(row['gid'])] = (str(row['groupid']), [])
        groups[str(row['gid'])][1].append(str(row['uid']))

    # Compares uid.lis with Ferry user_group table and updates it accordingly
    lines = open(uid_lis).readlines()
    for line in lines:
        line = re.findall('(\d+)\t\t(\d+)\t\t.+\t\t.+\t\t.+', line)
        if len(line) == 1:
           uid, gid = line[0]
           if uid in uids and gid in groups:
               if uid not in groups[gid][1]:
                   actions += "INSERT INTO user_group (uid, groupid, is_leader, last_updated) VALUES (%s, %s, False, NOW());\n" \
                           % (uid, groups[gid][0])

    return actions

if __name__ == '__main__':
    CONFIG = configparser.ConfigParser()
    if len(sys.argv) > 1:
        CONFIGPATH = sys.argv[1]
    else:
        CONFIGPATH = os.path.dirname(os.path.realpath(__file__)) + "/update.config"
    CONFIG.read_file(open(CONFIGPATH))

    # Download source files
    for source in SOURCES:
        url = CONFIG.get('sources', source)
        filePath = CONFIG.get('path', 'source_dir') + source

        text = urllib.request.urlopen(url).read().decode()
        if os.path.isfile(filePath):
            os.remove(filePath)
        f = open(filePath, 'w')
        f.write(text)
        f.close()

    # Access Ferry Database
    CONN_STRING = "host='%s' dbname='%s' user='%s' password='%s'" % \
                  (CONFIG.get('ferry', 'hostname'), CONFIG.get('ferry', 'schema'),
                   CONFIG.get('ferry', 'username'), CONFIG.get('ferry', 'password'))
    CONN = psycopg2.connect(CONN_STRING)
    CURSOR = CONN.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # Apply changes to Ferry Database
    actions = update_users(CURSOR, CONFIG.get('path', 'source_dir') + SOURCES[0], CONFIG.get('path', 'source_dir') + SOURCES[2])
    if actions != '':
        #print(actions)
        CURSOR.execute(actions)
    
    actions = update_groups(CURSOR, CONFIG.get('path', 'source_dir') + SOURCES[1])
    if actions != '':
        #print(actions)
        CURSOR.execute(actions)
    
    actions = update_user_group(CURSOR, CONFIG.get('path', 'source_dir') + SOURCES[0])
    if actions != '':
        #print(actions)
        CURSOR.execute(actions)

    CONN.commit()
