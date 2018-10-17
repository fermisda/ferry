import pycurl
import io
import json
import yaml
import sys


##################################################################
# Creates a file (group) needed for dcache transfer gratia probe #
# File format:
#
##################################################################

class FileGenerator:
    def __init__(self, conf):
        self.config = conf
        self.http = None

    def set_ferry_access(self):
        self.http = pycurl.Curl()
        self.http.setopt(pycurl.CAPATH, self.config["ca_path"])  # cert_path is location of CA certificates
        self.http.setopt(pycurl.SSLCERT,self.config["cert"] )  # cert is location to your proxy
        self.http.setopt(pycurl.SSLKEY,self.config["key"] )  # cert is location to your proxy

    def execute_ferry_api(self,action, arguments = []):
        cmd = "%s/%s?" % (self.config["ferry_url"], action)
        for a in arguments:
            cmd += "%s&" % (a,)
        buffer = io.BytesIO()
        self.http.setopt(pycurl.WRITEFUNCTION, buffer.write)
        self.http.setopt(pycurl.URL, str(cmd[:-1]))
        try:
            self.http.perform()
        except pycurl.error as err:
            print ("PyCurl error: {0}".format(err))
            return None
        except pycurl.error as err:
            print ("PyCurl error: {0}".format(err))
            return None
        except:
            raise FerryAPIError("Unexpected error: %s"% (sys.exc_info()[0]))
        data = buffer.getvalue().decode('UTF-8')
        # insanity of searching if it was an error
        if str(data).find("ferry_error") != -1 :
            return
        return json.loads(data)

def main():
    config = yaml.load(open(sys.argv[1]))
    file_generator = FileGenerator(config)
    file_generator.set_ferry_access()
    groups = file_generator.execute_ferry_api("getAllGroups", ["type=UnixGroup"])
    if groups is None or len(groups) < 1:
	print ("Something is wrong, not overwriting group file")
	sys.exit(1)
    fn = open("group",'w')
    for g in groups:
        group = g['name']
        gid = g['gid']
	if group.find(" ") >= 0:
		continue
        arguments = ["groupname=%s" % (group,),"type=UnixGroup"]
        users = file_generator.execute_ferry_api("getGroupMembers", arguments)
        user_list=""
        if users:
            for user in users:
                user_list += user['username'] + ','
            fn.write('%s:x:%s:%s\n' % (group, gid, user_list[:-1]))
            fn.flush()
    fn.close()
    sys.exit(0)
if __name__ == "__main__":
    main()
