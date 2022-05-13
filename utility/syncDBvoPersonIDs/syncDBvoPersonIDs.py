import argparse
import configparser
import logging
import psycopg2

PROD_DB = 'ferry_prd'

def getFromDbUserData(host, port, dbname, user):
    dsn = "dbname='%s' user='%s' host='%s' port=%s" % (dbname, user, host, port)
    try:
        conn = psycopg2.connect(dsn)
    except:
        print("I am unable to connect to the database ", dbname)
        exit(1)

    sql = "select uname, voPersonID from users where status=true and is_groupaccount=false and is_sharedaccount=false"
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows

def updateToDbUserData(fromUserData, host, port, dbname, user):
    dsn = "dbname='%s' user='%s' host='%s' port=%s" % (dbname, user, host, port)
    try:
        conn = psycopg2.connect(dsn)
    except:
        print("I am unable to connect to the database ", dbname)
        exit(1)

    cursor = conn.cursor()
    updateCnt = 0
    okayCnt = 0
    notInDB = 0
    for from_uname, from_voPersonID in fromUserData:
        cursor.execute("select uname, voPersonID from users where uname='%s'" % from_uname)
        logging.debug("uname: <%s>" % from_uname)
        row = cursor.fetchone()
        if row is None:
            logging.info("%s not in %s" % (from_uname, dbname))
            notInDB += 1
            continue
        (uname, voPersonID) = row
        if (voPersonID != from_voPersonID) and (uname is not None):
            sql = "update users set voPersonID='%s' where uname='%s'" % (from_voPersonID, from_uname)
            cursor.execute(sql)
            logging.info(sql)
            updateCnt += 1
        else:
            okayCnt += 1

    cursor.close()
    conn.commit()
    conn.close()
    return updateCnt, okayCnt, notInDB

if __name__ == "__main__":
    desc = """ Ensures each voPersonID in the users table is the same in both databases.  If not, it updates users.voPersonID in
the 'to' databases."""

    logging.basicConfig(encoding='utf-8', level=logging.INFO)
    parser = argparse.ArgumentParser(description = desc)
    parser.add_argument("-c", "--config", metavar = "PATH", action = "store", help = "path to configuration file")
    opts = parser.parse_args()

    try:
        config = configparser.ConfigParser()
        config.read_file(open(opts.config))
    except:
        logging.error("could not find configuration file")
        exit(1)

    if "from_db" not in config:
        logging.error("from_db not in config file")
        exit(1)
    if "to_db" not in config:
        logging.error("to_db not in config file")
        exit(1)

    print("Will copy FROM: %s    TO: %s" % (config.get("from_db", "dbname"), config.get("to_db", "dbname")))
    txt = input("Enter YES to continue:  ")
    if txt != "YES":
        exit(0)
    logging.info("Here we go....")

    db = "from_db"
    fromUserData = getFromDbUserData(config.get(db, "host"), config.get(db, "port"), config.get(db, "dbname"), config.get(db, "user"))
    db = "to_db"

    if config.get(db, "dbname") == PROD_DB:
        txt = input("Wait! You are overwriting PRODUCTION! -- %s --  ARE YOU SURE?  Enter YES to continue:  ")
        if txt != "YES":
            exit(0)

    updateCnt, okayCnt, notInDB = updateToDbUserData(fromUserData, config.get(db, "host"), config.get(db, "port"),
                                                     config.get(db, "dbname"), config.get(db, "user"))

    logging.info("%s records were processed" % len(fromUserData))
    logging.info("%s records updated" % updateCnt)
    logging.info("%s records were good" % okayCnt)
    logging.info("%s users were not in %s" % (notInDB, config.get(db, "dbname")))
    logging.info("Now, be sure call the API syncLdapWithFerry.")
