# ---------------------------------------------------------------------|
#                        PROXYHANDLER module                           |
# ---------------------------------------------------------------------|
# Introduces proxy server support for online actions                   |
# Marian Tihanyi | 2021 | IDRP MIRRI                                   |
# -------------------------------------------------------------------- |
# Packages needed :  numpy, xml.etree.ElementTree, os                  |
# -------------------------------------------------------------------- |

import urllib.request
import urllib.error

# Function checks entered proxy availability - taken from:
# https://stackoverflow.com/questions/765305/proxy-check-in-python
def is_bad_proxy(query, type):
    try:
        if type == "http":
            proxy_handler = urllib.request.ProxyHandler({'http': query})
        else:
            proxy_handler = urllib.request.ProxyHandler({'https': query})

        opener = urllib.request.build_opener(proxy_handler)
        opener.addheaders = [('User-agent', 'Mozilla/5.0')]
        urllib.request.install_opener(opener)

        if type == 'http':
            req = urllib.request.Request('http://www.example.com')  # change the URL to test here
        else:
            req = urllib.request.Request('https://www.crz.gov.sk')  # change the URL to test here

        sock = urllib.request.urlopen(req)

    except urllib.error.HTTPError as e:
        print('Error code: ', e.code)
        return e.code

    except Exception as detail:
        print("ERROR: ", detail)
        return True

    return False


# Function to add and check proxy with user input
def query_proxy():
    pxname = ""
    pxname1 = ""
    pxname2 = ""

    bad_proxy = [False, False]

    use_proxy = input("Do you need to use proxy server for internet access? (y/n): ")

    if use_proxy == 'y' or use_proxy == 'Y':
        pxname = input("Enter http proxy information in the form 'http://IP_address:port' or 'd' for default (localhost:3128): ")

        if pxname == 'd' or pxname == 'D':
            pxname1 = 'http://127.0.0.1:3128'

        elif 'http://' not in pxname:
            print("Invalid entry. Using default http proxy.")
            pxname1 = 'http://127.0.0.1:3128'

        else:
            pxname1 = pxname

        pxname = input("Enter https proxy information in the form 'https://IP_address:port' or 'd' for default (localhost:3128): ")

        if pxname == 'd' or pxname == 'D':
            pxname2 = 'https://127.0.0.1:3128'

        elif 'https://' not in pxname:
            print("Invalid entry. Using default https proxy.")
            pxname2 = 'https://127.0.0.1:3128'

        else:
            pxname2 = pxname

    if use_proxy == 'y' or use_proxy == 'Y':
        print(f'Using http proxy: {pxname1}')
        print(f'Using https proxy: {pxname2}')

        if is_bad_proxy(pxname1, "http"):
            print("Entered http proxy server is either non-existent or offline.")
            bad_proxy[0] = True
        else:
            print("Entered http proxy server is online.")

        if is_bad_proxy(pxname2, "https"):
            print("Entered https proxy server is either non-existent or offline.")
            bad_proxy[1] = True
        else:
            print("Entered https proxy server is online.")

    if bad_proxy[0] is True and bad_proxy[1] is True:
        return "", ""
    else:
        if bad_proxy[0] is False and bad_proxy[1] is True:
            if use_proxy == 'y' or use + proxy == 'Y':
                return pxname1, ""
            else:
                return "", ""

        elif bad_proxy [0] is True and bad_proxy[1] is False:
            if use_proxy == 'y' or use + proxy == 'Y':
                return "", pxname2
            else:
                return "", ""
        else:
            if use_proxy == 'y' or use_proxy == 'Y':
                return pxname1, pxname2
            else:
                return "",""
