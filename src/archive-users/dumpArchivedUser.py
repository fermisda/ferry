import argparse
import json
import pprint
import psycopg2


def parse_command_line():
    doc = "Dumps out the data for an archived user."
    parser = argparse.ArgumentParser(description=doc)
    parser.add_argument('host', help="Host database is on.")
    parser.add_argument('port', type=int, help="Database Port number.")
    parser.add_argument('dbname', help="Database to connect to.")
    parser.add_argument('user', help="User to connect as")
    parser.add_argument('uid', help="uid to dump")
    parser.add_argument('-p', '--password', help="Password for the database user -- if not in your .pgpass .")
    args = parser.parse_args()
    return args

def main():

    args = parse_command_line()
    password = ""
    if args.password:
        password = "password=%s" % args.password
    conn = psycopg2.connect("dbname=%s host=%s port=%s user=%s %s" % (args.dbname, args.host, args.port, args.user, password))
    cursor = conn.cursor()
    sql = "select  uid, uname, user_data, date_deleted from user_archives where uid=%s" % args.uid
    cursor.execute(sql)
    row = cursor.fetchall()
    if len(row) > 1:
        print("Multiple rows found, need to update this script.")
    elif len(row) < 1:
        print("No rows found.")
    else:
        uid, uname, user_data, date_deleted = row[0]
        print("\n\nUID: %s\nUNAME: %s\nDATE_DELETED:%s\n\n" % (uid, uname, date_deleted))
        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(user_data)

if __name__ == "__main__":
    main()
