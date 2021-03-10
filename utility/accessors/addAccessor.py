import sys
import argparse
import yaml
import psycopg2

def parse_command_line():
    doc  = """Outputs  SQL inserts needed to load the accessors table. This is intended to be run before over writing the DB from production.
    Then just delete accessors and run the inserts."""
    parser = argparse.ArgumentParser(description=doc)
    parser.add_argument('config', help='path/name of the .yaml config file, without the extension')
    args = parser.parse_args()
    return args

def menu(database):
    print("    Accessors Manager")
    choice = input("""
    a: Add New IP
    b: Add New DN
    q: Quit

    Choice: """)

    ask = None
    if choice == "A" or choice =="a":
        role = "ip_role"
    elif choice == "B" or choice =="b":
        role = "dn_role"
    elif choice=="Q" or choice=="q":
        sys.exit()
    else:
        print("You must only select either A or B")
        print("Please try again")
        menu()

    name = None
    if role=="ip_role":
        print("    Add New Accesssor by IP")
        print("    FNAL Example: 131.225.X.X")
        name = input("    Enter IP Address: ")
    else:
        print("\n    Add New Accessor by DN")
        print("    FNAL Example: /DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Full Name/CN=UID:uname")
        name = input("   Enter DN: ")

    w = input("\n    Allow use of write functions? (yes/no): ")
    writable = w.lower() in ['true', '1', 't', 'y', 'yes', 'yeah', 'yup', 'certainly', 'uh-huh']

    def getComment(role):
        commment = None
        if role == "ip_role":
            comment = input("    (Required) Host name or username if mac: ")
        else:
            comment = input("    (Optional) Comment: ")
        if role == "ip_role" and (comment is None or comment == ""):
            comment = getComment(role)
        return comment

    comment = getComment(role)

    print("\n    Verify the following: ")
    print("    Type:     ", role)
    print("    Name:     ", name)
    print("    Writable: ", writable)
    print("    Comment:  ", comment)
    v = input("Is this correct? (yes/no): ")
    valid = v.lower() in ['true', '1', 't', 'y', 'yes', 'yeah', 'yup', 'certainly', 'uh-huh']
    if valid == False:
        print("\n    Exiting.  You'll have to start over....")
        sys.exit()

    print("\n")
    password = ""
    if database.get('password'):
        password = "password=%s" % database.get('password')
    conn = psycopg2.connect("dbname=%s host=%s port=%s user=%s %s" % (database['name'], database['host'], database['port'], database['user'], password))
    cursor = conn.cursor()
    sql = "insert into accessors (name, write, type, comments) values ('%s', %s, '%s', '%s')" % (name, writable, role, comment)
    print("    SQL: ",sql)
    cursor.execute(sql)
    conn.commit()
    conn.close()
    print("\n\n    Saved to Database!")


def main():

    args = parse_command_line()

    file = open("%s.yaml" % args.config)
    config = yaml.load(file, Loader=yaml.FullLoader)
    file.close()
    database = config['database']
    menu(database)

if __name__ == "__main__":
    main()
