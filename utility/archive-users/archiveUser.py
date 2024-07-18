
import sys
import argparse
import logging
import json
from datetime import date

import psycopg2


class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, date):
            return str(obj)
        return json.JSONEncoder.default(self, obj)


def parse_command_line():
    doc = "Archives a user by their UID along with all associated records.   No changes will be commited without the commit flag set. Always do a test run before committing."
    parser = argparse.ArgumentParser(description=doc)
    parser.add_argument('host', help="Host database is on.")
    parser.add_argument('port', type=int, help="Database Port number.")
    parser.add_argument('dbname', help="Database to connect to.")
    parser.add_argument('user', help="User to connect as")
    parser.add_argument('uid', type=int, help='UID of the user TO BE ARCHIVED.')
    parser.add_argument('-c', '--commit', action="store_true", help='Save changes to the database. Required to save data!')
    parser.add_argument('-p', '--password', help="Password for the database user account. For testing only, for production use .pgpass .")
    parser.add_argument('-s', '--screenoff', action="store_false", help='Output log data to screen - default is write to screen')
    parser.add_argument('-v', '--verbose', type=int, choices=[0, 1, 2], default=1, help="Log level: 0 off, 1 info (default), 2 debug")
    parser.add_argument('-f', '--log_file', help="File to log to - defalut none.")
    args = parser.parse_args()
    return args

def set_logging(verbose, log_file):
    logArgs = {
        "format": "[%(asctime)s][%(levelname)s] %(message)s",
        "datefmt": "%Y/%m/%d %H:%M:%S"
    }
    if verbose == 0:
        logger = logging.getLogger()
        logger.disabled = True
    elif verbose == 1:
        logArgs["level"] = logging.INFO
    elif verbose == 2:
        logArgs["level"] = logging.DEBUG
    if log_file is not None:
        logArgs["filename"] = log_file
    else:
        logArgs["stream"] = sys.stdout

    logging.basicConfig(**logArgs)

def verify_uid(cursor, uid):
    uname = None
    sql = """select uid, uname, status, expiration_date, last_updated, full_name, is_groupaccount
             from users
             where uid = %s""" % uid
    cursor.execute(sql)
    row = cursor.fetchall()
    cnt = len(row)
    if cnt < 1:
        pass
    elif cnt > 1:
        logging.error("WHAT??? multiple records were retured for UID: %s", uid)
        raise SystemExit(1)
    else:
        uuid, uname, status, expiration_date, last_updated, full_name, is_groupaccount = row[0]
        logging.info("Located --> UID: %s UNAME: %s STATUS: %s EXPIRATION_DATE: %s LAST_UPDATED: %s FULL_NAME: %s IS_GROUPACCOUNT: %s",
                     uuid, uname, status, expiration_date, last_updated, full_name, is_groupaccount)
    return uname

def fetch_records(cursor, uid, table, fields):
    sql = "select %s from %s where uid = %s" % (fields, table, uid)
    if table == "affiliation_unit_user_certificate":
        sql = """select %s from %s
                   where dnid in (select dnid from user_certificates where uid = %s)
              """ % (fields, table, uid)
    logging.debug("fetch_records --> sql: %s", sql)
    cursor.execute(sql)
    rows = cursor.fetchall()
    logging.info("fetch_records: %s: %s rows found", table, len(rows))
    logging.debug("fetch_records -> rows found: %s", str(rows))
    flist = fields.split(",")
    data = []
    for row in rows:
        cnt = 0
        row_data = {}
        for field in flist:
            row_data[field] = row[cnt]
            cnt = cnt + 1
        data.append(row_data)
    logging.debug("fetch_records --> row_data: %s", data)
    return data

def delete_records(cursor, uid, table):
    sql = "delete from %s where uid = %s" % (table, uid)
    if table == "affiliation_unit_user_certificate":
        sql = """delete from %s
                   where dnid in (select dnid from user_certificates where uid = %s)
              """ % (table, uid)
    logging.debug("delete_records --> sql: %s", sql)
    cursor.execute(sql)

def archive(cursor, uid, uname):
    user_data = {}
    # Add in reverse order of DB relationships so all the deletes work.
    schema = [
        "storage_quota",
        "affiliation_unit_user_certificate", "user_certificates",
        "external_affiliation_attribute", "grid_access",
        "compute_access_group", "compute_access",
        "user_group", "users",
    ]

    for table in schema:
        logging.info("archive -->  Processing table: %s:", table)
        cursor.execute("select * from %s LIMIT 0" % table)
        fields_list = [desc[0] for desc in cursor.description]
        fields = ",".join(fields_list)
        logging.debug("archive --> table: %s fields: %s", table, str(fields))
        user_data[table] = fetch_records(cursor, uid, table, fields)
        delete_records(cursor, uid, table)

    #logging.debug("archive --> user_data: %s", str(user_data))
    # Note the DateEncoder defined at that top of this file.
    jdata = json.dumps(user_data, cls=DateEncoder)
    logging.debug("archive --> user_data converted to json: %s", str(jdata))

    sql = "insert into user_archives (uid, uname, user_data) values (%s, '%s', '%s')" % (uid, uname, jdata)
    cursor.execute(sql)
    logging.info("archive --> User uid: %s uname: %s archived.", uid, uname)

def main():

    args = parse_command_line()
    set_logging(args.verbose, args.log_file)

    password = ""
    if args.password:
        password = "password=%s" % args.password
    logging.debug("Connecting to FERRY Database: %s Host: %s Port:%s", args.dbname, args.host, args.port)

    conn = psycopg2.connect("dbname=%s host=%s port=%s user=%s %s" % (args.dbname, args.host, args.port, args.user, password))
    cursor = conn.cursor()
    logging.debug("connected to database")

    uname = verify_uid(cursor, args.uid)
    if uname is None:
        logging.error("UID %s does not exist in database", args.uid)
    else:
        archive(cursor, args.uid, uname)

    if uname is not None:
        if args.commit is True:
            conn.commit()
            logging.info("All Changes Comitted to the Database! ")
            logging.info("\n\n")
        else:
            conn.rollback()
            logging.info("Changes rolled back.  Requires --commit to keep changes.")
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
