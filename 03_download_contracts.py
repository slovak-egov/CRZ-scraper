# Matej Badin    | UHP  | 2019                                          |
# Marian Tihanyi | IDRP | 2021: complete rework, see changelog below.   |
# ----------------------------------------------------------------------|
# Changelog 10/2021 - the following was implemented / remedied (in case |
# of issue or a problem):                                               |
#                                                                       |
# - Complete rework of download mechanism - instead of loading all the  |
#   names at once and downloading the files afterwards, everything      |
#   happens in an integrated fashion:                                   |
#   (loading - checking validity - downloading).                        |
#                                                                       |
# - The code no more puts all the PDF files in the same folder.         |
#   Rather than that, each contract ID has its very own folder, i.e.:   |
#   working_dir/<ID>/contract files. This way, original file names can  |
#   be preserved.                                                       |
#   All that makes manual navigation much easier later.                 |
#                                                                       |
# - Links to attachments contained in XML files have one or more of the |
#   following flaws:                                                    |
#                                                                       |
#   1. They might be nonfunctional at all, i.e. actual links changed.   |
#      The non-functional state might occur with all the links or only  |
#      some of them.                                                    |
#   2. The file name, which should contain contract ID followed by any  |
#      string, usually contains completely different ID, and/or         |
#      the naming convention, which actually might had been             |
#      a good idea once, is messed up in general, so one cannot rely    |
#      on it altogether (expecting certain naming format is therefore   |
#      deprecated and replaced with extracting actual file name from    |
#      either XML (if it is OK) and/or the live version of the subpage).|
#   2. They might correspond with the live version, but the attachment  |
#      does not exist (page HTML code contains links, but no files are  |
#      added.                                                           |
#   3. Even if functional, the total number of files / live links might |
#      be different from live version, which might result in downloading|
#      smaller number of files than available.                          |
#   4. Letter case issues - even if the link reads correctly, files     |
#      themselves sometimes have different letter case in extension,    |
#      e.g. PDF or Pdf instead of pdf. This is not reflected in the XML,|
#      while the website itself does not handle that (the code will not |
#      automatically repair a link broken because of letter case).      |
#      Live version of the website must contain working link, otherwise |
#      even manual download would not work (which happens, but          |
#      such a situation cannot be solved by scraping code).             |
#                                                                       |
# - Issues with files:                                                  |
#   1. Entirely missing PDF extension                                   |
#   2. Having zero size - i.e. corrupt / empty / nonexistent file       |
#                                                                       |
# - Handling duplicate contracts with same IDs                          |
# - Added Proxy support                                                 |
# - Handling the issue of random connection dropping (probably some     |
#   simple DDOS prevention algorithm or else - adding retry envelope    |
#   function).                                                          |
# --------------------------------------------------------------------  |
# Packages needed :  os, urllib.request, pandas, ast, requests,         |
#                    proxyhandler (own module, which enables internet   |
#                    connection via proxy), lxml, time and wraps from   |
#                    functools (for the "retry" decorator).             |
# --------------------------------------------------------------------  |
# Parsing downloaded data from CRZ GOV obtained by download_dump script |

import os
import urllib.request
import pandas as pd
import ast
import proxyhandler
import requests
import lxml.html as lh
import time
from functools import wraps


# This is for printing coloured text to the console:
class Colour:
   PURPLE = '\033[95m'
   CYAN = '\033[96m'
   DARKCYAN = '\033[36m'
   BLUE = '\033[94m'
   GREEN = '\033[92m'
   YELLOW = '\033[93m'
   RED = '\033[91m'
   BOLD = '\033[1m'
   UNDERLINE = '\033[4m'
   END = '\033[0m'


proxy_present = False

if not os.path.exists(os.getcwd() + '/contracts'):
    os.system('mkdir ' + os.getcwd() + '/contracts')

working_dir = os.getcwd() + '/contracts/'

DB_clean = pd.read_csv('CRZ_DB_clean.csv', delimiter='|', dtype=str)
number_of_contracts = DB_clean.shape[0]

download_db = []
proxy = []


# Decorator "retry" downloaded from: http://www.saltycrane.com/blog/2009/11/trying-out-retry-decorator-python/
# Original from: http://wiki.python.org/moin/PythonDecoratorLibrary#Retry
#
# Reason:
# Connection to crz.gov.sk seemed to have been randomly interrupted or dropped, respectively. That fired Timeout error,
# which belongs to ConnectionError class in requests. Despite attempts to catch the error (some of them successful),
# it would eventually crash, so I decided to google up some working solutions. This is one of them.
def retry(ExceptionToCheck, tries=4, delay=3, backoff=2, logger=None):
    # Retry calling the decorated function using an exponential backoff.
    #
    # http://www.saltycrane.com/blog/2009/11/trying-out-retry-decorator-python/
    # original from: http://wiki.python.org/moin/PythonDecoratorLibrary#Retry
    #
    # :param ExceptionToCheck: the exception to check. may be a tuple of
    #     exceptions to check
    # :type ExceptionToCheck: Exception or tuple
    # :param tries: number of times to try (not retry) before giving up
    # :type tries: int
    # :param delay: initial delay between retries in seconds
    # :type delay: int
    # :param backoff: backoff multiplier e.g. value of 2 will double the delay
    #     each retry
    # :type backoff: int
    # :param logger: logger to use. If None, print
    # :type logger: logging.Logger instance
    def deco_retry(f):

        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except ExceptionToCheck as e:
                    msg = "%s, retrying in %d seconds..." % (str(e), mdelay)
                    if logger:
                        logger.warning(msg)
                    else:
                        print('\n' + msg)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return f(*args, **kwargs)

        return f_retry  # true decorator

    return deco_retry


# Function url_is_alive:
# Taken from here: https://gist.github.com/dehowell/884204
# Checks that a given URL is reachable.
# :param url: A URL
# :rtype: bool
def url_is_alive(url):
    request = urllib.request.Request(url)
    request.get_method = lambda: 'HEAD'

    try:
        urllib.request.urlopen(request)
        return True
    except urllib.request.HTTPError:
        return False


# Function contract_download
#
# Downloads files from given links for a single contract identified using ID.
#
# Parameters:
# -----------
#   links_list As List =    List of valid links for download. Link validity
#                           must be checked and ensured outside the function.
#
#   cid As String =         ID of te contract, for which the attachments are to be downloaded.
#
#   target_path_prefix As String = The downloaded files will be stored in a folder, which name
#                                  is the ID of the contract. Everything before that folder name
#                                  must be passed to the function as this parameter,
#                                  e.g. /home/user/contracts/... (followed by ID).
#                                  The prefix is common for all contracts.
#
#   att_state As Integer =  Base index kept outside the function to keep track of total number
#                           of downloaded files. Each call of the function increments this number
#                           by the number of files belonging to a single contract.
#                           This is managed with the help of the "attachments_processed" local
#                           variable, which counts number of files for each instance
#                           of the function and is returned after processing.
#
#   retries As Integer =    Maximum number of retries when error returned after trying to open
#                           an url. when nothing is entered, value of 20 is used.
#                           Error is raised after 4 initial attempts, which means that
#                           the total number of attempts is therefore retries*4, i.e. 80
#                           for the default case.
# Returned data:
# --------------
#   attachments_processed = This is total number of processed attachments in a single run. It is
#                           added to a cummulative value outside the function and then
#                           the function is called again. This keeps track of total
#                           number of attachments processed in global.
#                           The return value is -1 in case of an error.
#
#   error =                 Error string in case an error occurs. If there is no error, this is
#                           empty string.
# Error states:
# -------------
#   Errors are always captured and the actual error message is returned through the "error"
#   variable.
#   There are two typed of Errors handled separately:
#
#   ConnectionError or TimeoutError =   If this type of error occurs, a retry mechanism
#                                       is administered. Maximum of 20 retries is set using
#                                       a default parameter value (must be multiplied by 4, which
#                                       is applied using the retry decorator).
#
#   Other error =                       If any other type of error than above occurs,
#                                       the processing stops and the following data is returned:
#                                       attachments_processed = -1
#                                       error = human readable error message.
#
def contract_download(links_list, cid, target_path_prefix, att_state, retries=20):
    attachments_processed = 0
    error = ""
    r = 0

    while True:
        try:
            if not os.path.exists(target_path_prefix + cid):
                os.system("mkdir " + target_path_prefix + cid)

            for j, dl_link in enumerate(links_list):
                operstr = dl_link.split("/")

                # File ending with dot, but no extension, is invalid and it would not be normally saved.
                # If this is the case, we need to remove the dot:
                if operstr[len(operstr) - 1][-1] == ".":
                    operstr[len(operstr) - 1] = operstr[len(operstr) - 1][:-1]

                print(f'\t\tFile: {j + 1} (total: {att_state + j + 1} / {number_of_attachments}), {dl_link}, target:'
                      f'./{target_path_prefix.split("/")[-2]}/{target_path_prefix.split("/")[-1]}{cid}/{operstr[len(operstr) - 1]}', end="")
                time.sleep(0.01)

                # Thanks to random connection dropouts, the function is treated with a decorator.
                @retry(requests.ConnectionError, tries=4, delay=3, backoff=2)
                def url_gethead():
                    return requests.head(dl_link, allow_redirects=True)

                # Instead of urllib, we use requests.get to download files. Thanks to random connection dropouts, the function is treated with a decorator.
                @retry(requests.ConnectionError, tries=4, delay=3, backoff=2)
                def urlopen_with_retry():
                    return requests.get(dl_link, allow_redirects=True)

                urlhead = url_gethead()
                size_valid = False

                # We also check size of the attachment here (see DB load piece below), in case the link in xml was invalid and the code decided
                # to try live version, which went OK (but has never been tested for valid file size):
                if 'Content-Length' in urlhead.headers:
                    if urlhead.headers['Content-Length'].isnumeric():
                        if int(urlhead.headers['Content-Length']) > 0:
                            req = urlopen_with_retry()
                            size_valid = True
                            open(target_path_prefix + cid + "/" + operstr[len(operstr) - 1], 'wb').write(req.content)

                if size_valid is True:
                    if j < len(links_list) - 1:
                        print(" [SUCCESS].")
                    else:
                        print(" [SUCCESS].", end="")

                else:
                    print("\n\tThe file to be downloaded has zero size, therefore is invalid. Skipping...")

                r = 0
                attachments_processed += 1

            return attachments_processed, error

        # Try to catch TimeoutError or ConnectionError:
        except ConnectionError or TimeoutError:
            if r < retries - 1:
                print(f"\n\tConnection or timeout error, retry no. {r + 1} of {retries}...")
                r += 1

            else:
                return -1, "Timeout Error"

        # All other errors:
        except Exception as e:
            return -1, repr(e)


# Function get_live_links()
#
# Extracts links to attachment files at given URL either from XML source or from
# live version of the website depending on whether the links in the XML
# correspond with the live ones.
#
# Parameters:
# -----------
#   parent_urls =   A list of URLs to check. These are URLs of the regular website,
#                   which contains underlined hypertext links to attachment files.
#                   There are two possible formats, of which usually only one works
#                   depending on when the contract was published (a legacy thing):
#                   1. https://crz.gov.sk/<ID>
#                   2. https://crz.gov.sk/zmluva/<ID>
#                   These are not direct links to any attachments !
#
#   runlevel =      This state variable differentiates between two call conditions:
#                   0 = Call in order to load working links from live version after
#                       finding out the links in XML are non-functional.
#                   1 = Links in XML are functional and the function is called to
#                       compare them to the live version (the main philosophy is to
#                       check total number of attachment links = files and if there
#                       are additional links compared to XML, to add them to the
#                       returned links list).
#
#   proxy_on =      A flag that internet connection should be routed via proxy server.
#
#   proxy_object =  Reference to the object containing proxy information. Used only
#                   if proxy_on is True.
#
#   current_links = A tuple of links to attachments loaded from the XML to be compared
#                   with live version, if runlevel == 1.
#                   Can be left blank if runlevel == 0.
#
# Returned data:
# --------------
#   no_new_links = Corresponds to "no_link" variable in free code - if there were no
#                  working links to attachments extracted from live version when
#                  calling with runlevel = 0 or there were no new unique links found
#                  in addition to those passed from XML when calling with runlevel = 1,
#                  this is set to True, otherwise False.
#                  This is also set to True if all of the passed parent_urls return
#                  error.
#
#   c_links =      List of extracted working links to attachments.
#                  This also includes those from XML if calling with runlevel = 1.
#                  If runlevel == 0 and no_new_links is True, this is always empty.
#                  If runlevel == 1 and no_new_links is True, this might or might not
#                  be empty depending on whether there were working links to attachments
#                  passed from XML using current_links. If there were no links passed,
#                  this is empty (and no_new_links always True in such case).
#
# Error states:
# -------------
#       Errors are always captured and corresponding message is printed to the console.
#       There are 3 possible errors:
#       - Requested web location contained in either item in parent_links does not exist
#         (returns error state).
#       - Requested web location contained in either item in parent_links exists, but
#         there are no files attached (hence no links are found in the page source).
#       - Only if runlevel == 1 and the links found exactly match those passed from XML
#         through the current_links parameter, there are no new links to add. This is not
#         an error per se, but rather a condition, which is announced, while the return
#         values remind an error state.
def get_live_links(parent_urls, runlevel, proxy_on, proxy_object, current_links=()):
    no_new_links = True
    c_links = []
    total_valid_links = 0

    if runlevel == 1:
        print("\tComparing saved links with live version for changes... ", end="")

    for uc, url in enumerate(parent_urls):
        if runlevel == 0:
            print(f"\tTrying: {url}...", end="")

        if proxy_on is True:
            # checking url with "retry" decorator, because the connection is dropped randomly.
            @retry(requests.ConnectionError, tries=4, delay=3, backoff=2)
            def urlopen_with_retry2():
                return requests.get(url, allow_redirects=True, proxies=proxy_object)

            page_data = urlopen_with_retry2()
        else:
            # checking url with "retry" decorator, because the connection is dropped randomly.
            @retry(requests.ConnectionError, tries=4, delay=3, backoff=2)
            def urlopen_with_retry3():
                return requests.get(url, allow_redirects=True)

            page_data = urlopen_with_retry3()

        # Site is loaded successfully:
        if page_data.ok:
            doc = lh.fromstring(page_data.content)

            try:
                # This html class contains links to attachments. If there are no attachments,
                # usually there is no such html class, which will raise en error.
                attachs = doc.find_class('area area2')[0][1]

                if runlevel == 0:
                    print(" web location OK, extracting attachment download links.")

                total_valid_links += 1

                if len(attachs) > 0:
                    # This is for runlevel == 0, i.e. the links passed from XML are non-functional.
                    if runlevel == 0:
                        for attach in attachs:
                            print("\t\tDownload link: https://www.crz.gov.sk" + attach[1].attrib['href'] + "...", end="")

                            # Check, if url is alive and say something about it to the user:
                            if url_is_alive('https://www.crz.gov.sk' + attach[1].attrib['href']):
                                c_links.append('https://www.crz.gov.sk' + attach[1].attrib['href'])
                                print(" is OK.")
                            else:
                                print(" is invalid.")

                        if len(c_links) > 0:
                            no_new_links = False
                            print("\tValid download links found, proceeding with download.")
                        else:
                            print("\tNo valid download links found, download cancelled.")

                        # This command speeds up the process according to the fact that
                        # only one of the passed parent_links works. Thus, if the code made it
                        # here, we already have what we wanted and do not need to try another
                        # link from the list.
                        break

                    # This is for runlevel == 1, i.e. comparison of links passed from XML with
                    # live version (but those from XML work):
                    else:
                        announcement_done = False

                        # We only proceed if there are already some links passed, otherwise it would be
                        # the runlevel = 0 situation:
                        if current_links != ():
                            # For comparison purposes, we should unify letter case and make it case-insensitive,
                            # in case (mainly) the extension has its case messed up.
                            current_links_lowercase = [lnk.casefold() for lnk in current_links]

                            # Already working links from XML are added to the returned list:
                            c_links = c_links + current_links
                            check_len = len(current_links)

                            # Here we compare what we found online with that, which is passed from XML:
                            for attach in attachs:
                                # If this is True, we have found a unique new link:
                                if 'https://www.crz.gov.sk' + attach[1].attrib['href'].casefold() not in current_links_lowercase:
                                    # Announcement, which is displayed only once for all the new unique links:
                                    if announcement_done is False:
                                        print("additional links to files found, appending them to the links list:")
                                        announcement_done = True

                                    # Unique link is appended and logged to the console:
                                    c_links.append('https://www.crz.gov.sk' + attach[1].attrib['href'])
                                    print(f"\t\tAppending: https://www.crz.gov.sk{attach[1].attrib['href']}")

                            # If the list of links to be returned is bigger than the one originally passed,
                            # there must have been new unique links added.
                            if len(c_links) > check_len:
                                no_new_links = False

                            # ..on the contrary, if the length matches, nothing noteworthy happened:
                            else:
                                print("no additional files found, proceeding with original links.")

                        # If we passed nothing to compare, we get nothing in return:
                        else:
                            c_links = []
                else:
                    if runlevel == 1:
                        # This "error" state is different for runlevels 0 and 1. If there are
                        # no links to attachments in live version when runlevel == 0, the situation
                        # is rather bad, because there are literally no links to attachments to use
                        # and no files can be downloaded at all.
                        # However, with runlevel == 1, this usually means that there are just no
                        # links to attachments in addition to the ones already passed from XML,
                        # so we can and will still use those ones.
                        print("no additional files found, proceeding with original links.")

            except Exception as e:
                print(repr(e))
                if runlevel == 0:
                    # See comment above.
                    print(" web location is OK, but does not contain any valid download links (files).")

                else:
                    # See comment above.
                    print("Live version does not contain any valid download links (files).")
                pass

        else:
            if runlevel == 0:
                # See comment above
                print(" web location is invalid.")
            else:
                if total_valid_links == 0:
                    # See comment above
                    print("no additional files found, proceeding with original links.")

    return no_new_links, c_links


# Checking for proxy (even if there is no proxy, the function handles that):
proxyname1, proxyname2 = proxyhandler.query_proxy()

if proxyname1 != "" or proxyname2 != "":
    if proxyname1 != "" and proxyname2 != "":
        proxy = urllib.request.ProxyHandler({'http': proxyname1, 'https': proxyname2})
    elif proxyname1 != "":
        proxy = urllib.request.ProxyHandler({'http': proxyname1})
    elif proxyname2 != "":
        proxy = urllib.request.ProxyHandler({'https': proxyname2})

    # construct a new opener using proxy settings
    opener = urllib.request.build_opener(proxy)

    # install the opener on the module-level
    urllib.request.install_opener(opener)

    proxy_present = True

number_of_attachments = 0
size = 0.0

print("Loading contracts database, please wait...")

for i in range(0, number_of_contracts):
    contract_size = 0.0

    # download_db is a list of lists with the following syntax of each element (sub-list):
    # 0: contract_ID
    # 1 to n: download links including main contract documents and supplements
    # n + 1 (last): Total download size of particular contract
    download_db.append([])

    attachments = ast.literal_eval(DB_clean.iloc[i, 20])

    download_db[i].append(str(DB_clean.iloc[i, 2]))  # contract_ID

    for j in range(1, len(attachments)):
        if str(attachments[j]).find("https://") >= 0:
            if str(attachments[j - 1]).isnumeric():
                # There are also invalid attachments having zero size and we don't want them - this only works for
                # links in the database (loaded form xml files), not for live version.
                if int(attachments[j - 1]) > 0:
                    download_db[i].append(attachments[j])  # download link

                    if j > 0:
                        contract_size += float(attachments[j - 1]) / 1000000  # file size

                    number_of_attachments += 1

    # We also need links to files at pages containing only contract supplement documents:
    supplements = []

    if not pd.isnull(DB_clean.iloc[i, 21]):
        supplements = ast.literal_eval(DB_clean.iloc[i, 21].replace(' nan,', ' "nan",'))

        for supplement in supplements:
            supplement_ID = str(supplement[1])

            supplement_attachment_number = 0
            supplement_attachments = ast.literal_eval(supplement[9])

            for attachment in supplement_attachments:
                download_db[i].append(attachment[0])
                number_of_attachments += 1

                if str(attachment[2]).isnumeric():
                    contract_size += float(attachment[2]) / 1000000

    download_db[i].append(contract_size)
    size += contract_size
    print(f"\r{round(i / number_of_contracts * 100, 1)}%", end="")

print('')

# This input may be omitted in the future, it was mainly held because of download error issues, which were
# extremely rare after the last version. However, it is still here for the time.
start = input("Please enter starting number of contract ('1' for the first one): ")

# Contract number input verification:
try:
    s = int(start) - 1

    if s < 0 or s >= number_of_contracts:
        print(f"Allowed interval is between 1 and {number_of_contracts}. Defaulting to 1.")
        s = 0
except:
    print(f"Allowed interval is between 1 and {number_of_contracts}. Defaulting to 1.")
    s = 0
    pass

if s > 0:
    size = 0.0
    number_of_attachments = 0

    for i in range(s, number_of_contracts):
        size += float(download_db[i][len(download_db[i]) - 1])

        for attachment in download_db[i]:
            if str(attachment).find("https://") >= 0:
                number_of_attachments += 1

print(f'Starting with contract no. {s + 1}.')
print(f'Totally {number_of_contracts - s} contracts with {number_of_attachments} files, having total size of {round(size)} MB, will be downloaded.')

# The main process starts here.
dl_size = 0.0
att = 0
i = s
missing_files = 0
skipped = 0
links_count = 0
no_link = False
duplicates = 0
IDs = []
urls = []

while i < number_of_contracts:
    check_links = []

    print(f"*** Contract number {i + 1} out of {number_of_contracts}, ID: {download_db[i][0]} ***")

    # Check for duplicates - if the same ID is in IDs DB, the download of that particular contract
    # is stopped.
    if download_db[i][0] not in IDs:
        # Check all download links for currently processed ID:
        no_link = False

        # First we check, whether the list extracted from XML contains some links, not just ID:
        if len(download_db[i]) > 2:
            for j in range(0, len(download_db[i])):
                # Link validity check:
                if str(download_db[i][j]).find("https://") >= 0:
                    # Are found links alive?
                    if url_is_alive(download_db[i][j]) is False:
                        number_of_attachments -= 1
                        no_link = True
                        break
                    else:
                        # If the found link is alive, we add it to the resulting active links list.
                        check_links.append(download_db[i][j])

            # The links in XML might be OK, but we also need to check live version
            # of the website, whether there were additional files added in the meantime:
            if no_link is False:
                # These are parent_urls for link extraction function:
                urls = ['https://crz.gov.sk/' + str(download_db[i][0]),
                        'https://crz.gov.sk/zmluva/' + str(download_db[i][0])]

                c_len = len(check_links)

                # Checking if there are additional links to attachments:
                result, check_links = get_live_links(urls, 1, proxy_present, proxy, check_links)

                # result holds information, whether more links were added.
                # result = False -> new links were added (no_new_links = False)
                # result = True  -> no new links were added (now_new_links = True)
                if result is False:
                    # If something was added, we calculate difference number for new links only).
                    diff = len(check_links) - c_len
                    print(f"\tAdded {diff} new links from live version.")
                    number_of_attachments += diff
        else:
            # No links in XML:
            no_link = True

        # If the links in the source XML file were not alive (or even existing), we have to load
        # live version of the website and take the links from its source:
        if no_link is True:
            # In case any links were added above before the code found an invalid link, we reset them here:
            urls = ['https://crz.gov.sk/' + str(download_db[i][0]),
                    'https://crz.gov.sk/zmluva/' + str(download_db[i][0])]

            print("\t[*** ERROR ***] Invalid links in XML file. Trying live version...")

            no_link, check_links = get_live_links(urls, 0, proxy_present, proxy)

            # If valid links were found, we need to increment total number of files to be downloaded:
            if no_link is False:
                number_of_attachments += len(check_links)

        # Normal processing: Here we either added some links, found any links in live version or are just using working links from XML:
        if no_link is False:
            # If any of the error correcting mechanisms above were successful, we can proceed with download, which still
            # might fail:
            status, err = contract_download(check_links, download_db[i][0], working_dir, att)

            # The download was successful:
            if status > -1:
                att += status
                dl_size = dl_size + download_db[i][len(download_db[i]) - 1]

                # Eye candy output:
                print('')
                print('\n\t\t' + Colour.BOLD + Colour.CYAN + '[' + str(round((dl_size / size) * 100, 1)) + '%]' + Colour.END, end="")
                print(f" {round(dl_size, 0)}/{round(size - dl_size, 0)} MB, Saved: "
                      f"{att}, Skipped: {skipped}, Missing: {missing_files}, Duplicates: {duplicates}; of total: {number_of_attachments}")
                print('')

            # If something else went wrong during download despite all the effort, we let the user know:
            else:
                print(f' Error: {err} encountered while downloading files for contract ID {download_db[i][0]}, skipping.')
                skipped += 1

        else:
            # There are also contracts / supplements at CRZ, which lack any attachments (the subpage exists, but there are no files)
            # and we need to handle that somehow.
            print("\tNo files associated with the processed contract ID - adding to the list of contracts with missing files.")
            missing_files += 1

        # Add processed ID to the IDs database, so that it is not processed again (duplicate prevention):
        IDs.append(download_db[i][0])
        i += 1

    # The processed contract ID was found within the list of already processed IDs, therefore it is a duplicate record.
    # We want to do something in that case (like keep track of duplicate records)...
    else:
        # First, we find the original record, which is identical to the one we are processing:
        found = 0

        if len(IDs) > 0:
            for x, contract_id in enumerate(IDs):
                if contract_id == download_db[i][0]:
                    found = x
                    break

        # Here we complain:
        print(f"  ID {download_db[i][0]} is a duplicate or separately added attachment of contract no. {found + 1} - skipping contract (adding to the duplicates count).")
        # Here we add this to the duplicates dump:
        duplicates += 1
        i += 1

# The final report:
print('')
print(f"Found {missing_files} contracts out of {number_of_contracts} without any associated files.")
print(f"Skipped download of any files for {skipped} contracts out of {number_of_contracts} due to unresolvable problems with links / attachments.")
print(f"Found {duplicates} duplicate records.")
print(f"In total, no files for {missing_files + skipped + duplicates} contracts out of {number_of_contracts} ({round((missing_files + skipped + duplicates) / number_of_contracts * 100, 2)}%) were downloaded.")
print("Process finished successfully.")
