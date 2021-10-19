import json
import argparse
import configparser
import logging
import urllib.request
import urllib.error
import ssl

def syncLdapToFerry(hostname, ferryContext):
    url = hostname + "/" + "syncLdapWithFerry"
    reply = openURL(url) # , context = ferryContext)
    result = json.loads(reply)
    return (result["ferry_status"], result["ferry_error"])

def postToSlack(url, messages):
    if not isinstance(messages, list):
        messages = [messages]
    for message in messages:
        data = {"payload": target["payload"]}
        data["payload"] = data["payload"].replace("$ACT", "syncLdapWithFerry failed")
        data["payload"] = data["payload"].replace("$MSG", message)
        data = bytes(urllib.parse.urlencode(data).encode())
        openURL(url, data=data)


def openURL(url, data = None, context = None):
    ferryError = {'ferry_status': 'failure'}
    try:
        return (urllib.request.urlopen(url, data=data, context=context).read().decode())
    except urllib.error.URLError as e:
        ferryError['ferry_error'] = "Failed to access remote server: %s   error: %s" % (url, e.reason)
        logging.error(ferryError['ferry_error'])
    except Exception as e:
        ferryError['ferry_error'] = "Failed to access remove server: %s - general exception: %s" % (url, e)
        logging.error(ferryError['ferry_error'])
    return json.dumps(ferryError)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = "Script to syncronize LDAP to FERRY.")
    parser.add_argument("-c", "--config", metavar = "PATH", action = "store", help = "path to configuration file")
    opts = parser.parse_args()

    try:
        config = configparser.ConfigParser()
        config.read_file(open(opts.config))
    except:
        logging.error("could not find configuration file")
        exit(1)

    source = dict(config.items("ferry"))
    hostname = source["hostname"]

    if "slack" not in config:
        logging.error("slack not in config file")
        exit(1)
    target = dict(config.items("slack"))
    if "url" not in target:
        logging.error("url not in slack")
        exit(1)
    slackUrl = target["url"]

    ferryContext = None
    #ferryContext = ssl.SSLContext(protocol=ssl.PROTOCOL_TLSv1_2)
    #ferryContext.verify_mode = ssl.CERT_REQUIRED
    #ferryContext.verify_mode = ssl.CERT_NONE
    #ferryContext.load_cert_chain(config.get("ferry", "cert"), config.get("ferry", "key"))
    #ferryContext.load_verify_locations(capath=config.get("ferry", "ca"))



    status, error  = syncLdapToFerry(hostname, ferryContext)
    if status != "success":
        postToSlack(slackUrl, error)
