import pycurl
import io
import json
import yaml
import sys

class FerryAPIError(Exception):
    def __init__(self, value):
        self.message = value

def set_ferry_access(proxy, capath):
    http = pycurl.Curl()
    http.setopt(pycurl.CAPATH, capath)  # cert_path is location of CA certificates
    http.setopt(pycurl.SSLCERT, proxy)  # cert is location to your proxy
    return http


def execute_ferry_api(url, http, action, arguments):
    cmd = "%s/%s?" % (url, action)
    for a in arguments:
        cmd += "%s&" % (a,)
    buffer = io.BytesIO()
    http.setopt(pycurl.WRITEFUNCTION, buffer.write)
    http.setopt(pycurl.URL, cmd[:-1])
    print (cmd[:-1])
    try:
        http.perform()
    except pycurl.error as err:
        print ("PyCurl error: {0}".format(err))
        return None
    except pycurl.error as err:
        print ("PyCurl error: {0}".format(err))
        return None
    except:
        raise FerryAPIError("Unexpected error: %s"% (sys.exc_info()[0]))
    data= buffer.getvalue().decode('UTF-8')
    # insanity of searching if it was an error
    if "ferry_error" in data:
        #raise FerryAPIError("%s: %s" % (cmd, data))
        print ("ferry_error %s - %s" % (cmd[:-1], data))
        return
    return json.loads(data)


def read_config(file_name):
    return yaml.load(open(file_name))

def createComputeResource(url, http, action, key, info):
    arguments = ["resourcename=%s" % (key,),]
    for attr, value in info.items():
            arguments.append("%s=%s" % (attr, value))
    execute_ferry_api(url, http, action, arguments)

def setComputeResourceInfo(url, http, action, key, info):
    arguments = ["resourcename=%s" % (key,),]
    for attr, value in info.items():
            arguments.append("%s=%s" % (attr, value))
    execute_ferry_api(url, http, action, arguments)

def createFQAN(url, http, action, key, info):
    do_something(url, http, action, key, info)

def setUserAccessToComputeResource(url, http, action, key, info):
    for m in info:
        arguments = ["resourcename=%s" % (key,),]
        for attr, value in m.items():
            arguments.append("%s=%s" % (attr, value))
        execute_ferry_api(url, http, action, arguments)

def addCertificateDNToUser(url, http, action, key, info):
    do_something(url, http, action, key, info)

def setUserExperimentFQAN(url, http, action, key, info):
    do_something(url, http, action, key, info)

def createExperiment(url, http, action, key, info):
    do_something(url, http, action, key, info)

def addUsertoExperiment(url, http, action, key, info):
    do_something(url, http, action, key, info)

def addUserToGroup(url, http, action, key, info):
    do_something(url, http, action, key, info)

def setPrimaryStatusGroup(url, http, action, key, info):
    do_something(url, http, action, key, info)

def do_something(url, http, action, key, info):
    for m in info:
        arguments = []
        for attr, value in m.items():
            arguments.append("%s=%s" % (attr, value))
        execute_ferry_api(url, http, action, arguments)
def main():
    config = read_config(sys.argv[1])
    http = set_ferry_access(config["cert"], config["ca_path"])
    url = config["ferry_url"]
    for resources in config["resources"]:
        for key, actions in resources.items():
            for dict_action in actions:
                for action, info in dict_action.items():
                   method_to_call = globals()[action]
                   method_to_call(url, http, action, key, info)

main()
