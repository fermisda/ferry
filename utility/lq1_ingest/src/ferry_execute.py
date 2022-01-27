import requests


def execute_ferry_api(ferry_url, ferry_cert):
    r = requests.post(ferry_url,cert=ferry_cert,verify=False).json()
    if r["ferry_status"] == "success":
        return  r["ferry_output"]
    else:
        print "error", r["ferry_error"]
        pass
    return None

def main():
    url = "https://ferry.fnal.gov:8443/getAllUsers"
    cert = ("../data/x509up_u6956","../data/x509up_u6956")

    print execute_ferry_api(url,cert)


if __name__ == "__main__":
    main()