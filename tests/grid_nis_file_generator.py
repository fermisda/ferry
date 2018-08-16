import pycurl
import io
import json
import yaml
import sys

class Resource:
    def __init__(self, rn, au):
        self.resourcename = rn
        self.affiliationunit = au
class FileGenerator:
    def __init__(self, conf):
        self.config = conf
        self.http = None

    def set_ferry_access(self):
        self.http = pycurl.Curl()
        self.http.setopt(pycurl.CAPATH, self.config["ca_path"])  # cert_path is location of CA certificates
        self.http.setopt(pycurl.SSLCERT,self.config["cert"] )  # cert is location to your proxy

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
            #raise FerryAPIError("%s: %s" % (cmd, data))
            #print ("ferry_error %s: %s" % (cmd[:-1], data))
            return
        return json.loads(data)

    def get_resources(self):
        action = "getAllComputeResources"
        data = self.execute_ferry_api(action)
        resources = {}
        for info in data:
            resources[info["name"]] = info["affiliation_unit"]
        return resources

    def create_file(self,action, rn, un = None):
        method = action[3:-4].lower()
        # this is very unpleasant but cms fqan is not registered to cms 
        if rn == "cmst1":
            un = None

        if un:
            arguments = ["resourcename=%s" % (rn,), "unitname=%s" % (un,)]
            fn = "%s.%s.%s" % (method,rn,un)
        else:
            arguments = ["resourcename=%s" % (rn,)]
            fn = "%s.%s" % (method,rn)
        data = self.execute_ferry_api(action, arguments)
        # if not data or "ferry_error" in data:
        #    print (data, action, arguments)

        fn = open(fn,'w')
        try:
            getattr(self,method)(fn, data, rn, un)
        except:
           # print ("ferry_error %s: %s" % (method, data)) 
            pass
        fn.close()

    def passwd(self,fn, data, rn, un):
        if not un:
            un = "null"
        if rn == "cmst1":
            un = "cms"
        users=data[un]["resources"][rn]
        for u in users:
            # {u'username': u'cdf', u'shell': u'/sbin/nologin', u'uid': u'1347', u'gid': u'3200', u'gecos': u'Eugene
            # Schmidt', u'homedir': u'/home/cdf'
            fn.write("%s:KERBEROS:%s:%s:%s:%s:%s\n" % (u["username"],u["uid"],u["gid"],u["gecos"],u["homedir"],
                                                       u["shell"]))
            fn.flush()

    def vorolemap(self,fn, data, rn, un):
        for d in data:
            #[{"fqan":"/cdf/Role=None","mapped_uname":"cdf","unitname":"cdf"},
            fn.write('"%s" %s\n' % (d["fqan"], d["mapped_uname"]))
            fn.flush()

    def group(self,fn, data, rn, un):
        for d in data:
            if d["unames"] is not None:
                fn.write("%s:x:%s:%s\n" % (d["groupname"],d["gid"],','.join(d["unames"])))
            else:
                fn.write("%s:x:%s:\n" % (d["groupname"],d["gid"]))
            fn.flush()

    def gridmap(self,fn, data, rn, un):
        for d in data:
            fn.write('"%s" %s\n' % (d["userdn"], d["mapped_uname"]))
            fn.flush()

def main():
    config = yaml.load(open(sys.argv[1]))
    file_generator = FileGenerator(config)
    file_generator.set_ferry_access()
    resources = file_generator.get_resources()
    for rn, un in resources.items():
        for info in config["actions"]:
            for action, rlist in info.items():
                if not rlist or rn in rlist:
                    file_generator.create_file(action,rn,un)

if __name__ == "__main__":
    main()
