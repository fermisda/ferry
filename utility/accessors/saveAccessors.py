import os
import sys
import argparse
import pathlib
import time
import yaml
import psycopg2
from datetime import datetime

def parse_command_line():
    doc  = """Outputs  SQL inserts needed to load the accessors table. This is intended to be run before over writing the DB from production.
    Then just delete accessors and run the inserts."""
    parser = argparse.ArgumentParser(description=doc)
    parser.add_argument('config', help='path/name of the .yaml config file, without the extension')
    parser.add_argument('outfile', help="path/name of the ouput .sql, without the extension")
    args = parser.parse_args()
    return args

def main():

    args = parse_command_line()

    file = open("%s.yaml" % args.config)
    config = yaml.load(file, Loader=yaml.FullLoader)
    file.close()

    database = config['database']
    password = ""
    if database.get('password'):
        password = "password=%s" % database.get('password')
    conn = psycopg2.connect("dbname=%s host=%s port=%s user=%s %s" % (database['name'], database['host'], database['port'], database['user'], password))
    cursor = conn.cursor()

    sqlfile = "%s.sql" % args.outfile
    try:
        fname = pathlib.Path(sqlfile)
        stat = fname.stat()
        last_saved = datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
    except:
        last_saved = "1970-01-01 00:00:00"

    sql = "select count(*) from accessors where last_updated > to_timestamp('%s', 'YYYY-MM-DD HH24:MI:SS')" % last_saved
    cursor.execute(sql)
    (count,) = cursor.fetchone()
    if count == 0:
        sys.exit()
    sql = "select name, active, write, type, last_updated, last_used, comments from accessors"
    cursor.execute(sql)
    outfile = None
    for name, active, write, accType, last_updated, last_used, comments in cursor.fetchall():
        if outfile is None:
            if last_saved != "1970-01-01 00:00:00":
                now = datetime.now()
                newFileName = "%s-%s.sql" % (args.outfile, now.strftime("%Y%m%d%H%M%S"))
                os.rename(sqlfile, newFileName)
            outfile = open(sqlfile, 'w')

        outfile.write("insert into accessors (name, active, write, accType, last_updated, last_used, comments) values ('%s', %s %s, '%s', '%s', '%s', '%s');\n" %
                (name, active, write, accType, last_updated, last_used, comments))

if __name__ == "__main__":
    main()
