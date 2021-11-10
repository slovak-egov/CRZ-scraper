# Matej Badin | UHP | 2019                                             |
# Marian Tihanyi | IDRP | 2021                                         |
# -------------------------------------------------------------------- |
# Changelog 10/2021:                                                   |
# - Some fields loaded from XML to the DB contained leading and        |
#   trailing spaces, as well as new lines. This obviously had to be    |
#   handled in order to clean up the view when inspecting manually,    |
#   as well as to debug some processing taking place later.            |
#                                                                      |
# - Link format has changed since 2019, which is reflected in the code.|
#                                                                      |
# - Originally, there was HTML class "page-list" in the CRZ subpage's  |
#   source, which held information about the total number of subpages. |
#   No such class was found to be present there in 2021, hence         |
#   a completely new algorithm had to be implemented in order to       |
#   find out the total number of subpages                              |
#   Since no part of the website's code seemed to hold this info,      |
#   a successive approximation algorithm was introduced. The algorithm |
#   loads a subpage and checks if there are any items. CRZ has a bug,  |
#   which displays even a nonexistent subpage, so the algorithm        |
#   exploits it - see the "find_max_pages" function description.       |
# -------------------------------------------------------------------- |
# Packages needed :  xml.etree.ElementTree, os, proxyhandler (own      |
#                    added module, which enables internet connection   |
#                    via a proxy)                                      |
# -------------------------------------------------------------------- |
# Crawl tables with supplemental agreements on CRZ GOV                 |
# Include them inside DB get by 01_parse_xml.py                        |
# -------------------------------------------------------------------- |
# from typing import List, Any

import requests
import lxml.html as lh
import re
import proxyhandler

proxy_present = False

find_ID = re.compile(r'\d+')


# Function find_max_pages()
#
# RECURSIVE FUNCTION.
#
# REASON: The page-list HTML class referenced in the original code no longer exists
# at crz.gov.sk.
# The class contained total number of subpages, but this is not the case anymore.
# The state of affairs at the time of code update (summer-autumn 2021) is such that
# there is no explicit indicator containing total number of subpages with supplements.
# There is only total number of contracts, which is obviously unusable.
#
# This function finds the last subpage containing supplements, implementing certain bug
# at crz.gov.sk, which displays a non-existent subpage (with no supplements whatsoever),
# when fed with any imaginable number, e.g. 100000, while there may be only 15000
# or so valid subpages.
# In order to speed up the process, the following successive approximation algorithm
# based on trial and error is implemented:
#
#   1. Subpages are tested with maximum step of 10000 for presence of certain HTML class
#      containing string "area area 7". Empty (invalid) subpage dose not contain
#      this class. this means the algorithm starts with subpage 0, follows with subpage
#      10000, 20000 and so on.
#
#   2. If the last subpage from the above process does not contain given class, the
#      function is recursively called with step divided by 10 (e.g. 1000), testing
#      the same condition (the algorithm is unchanged, so we can use recursion).
#
#   3. The recursion goes deeper until the final step value of 1, which is the smallest
#      step used in the process. The number of the last subpage not containing
#      the "area area 7" class is the last valid subpage number + 1, thus the last subpage
#      number must be calculated as result - 1 after the function exits.
#
# The approach described above reduces the time required to find the last subpage when
# compared to linear search (constant step of 1, starting with 0).
# Usually, the last subpage is found within 40 runs.
#
# Function parameters:
# --------------------
#
# stage =           defines step size as follows:
#                   stage = 0: step = 10000
#                   stage = 1: step = 1000
#                   stage = 2: step = 100
#                   stage = 3: step = 10
#                   stage = 4: step = 1
#
# last =            This is a carry variable between recursion levels. It contains
#                   the last subpage index processed in the last parent call.
#                   Its value is 0 at the very first ("external") call.
#
# total_runs =      This is also a carry variable between recursion calls, keeping
#                   track of total runs (the main and all recursive calls) in order
#                   to check, how many runs were required to find the last subpage.
#                   This parameter is only required for internal recursive calls and
#                   is given default value of 0 (starting with 0 runs).
#
# proxyserver =     The function utilises the REQUESTS library, which crawls web pages.
#                   If there is a proxy server between the client where this script is run
#                   and the crz.gov.sk website, this parameter should be passed to the
#                   REQUESTS method, which accesses the website. The value is also passed on
#                   to the recursive child calls of the function.
#                   The type of the parameter is LIST (there is also a http and a https
#                   proxy) and if there is no need for proxy, its value is None.
#
# Return values (two-element tuple):
# ----------------------------------
#
# i =               index of the first subpage contaning no supplements. After the function
#                   exits, 1 is subtracted from this value in order to get the last valid
#                   subpage.
#
# total_runs =      Total number of runs / attempts, in which the last subpage was found.
def find_max_pages(stage, last=0, total_runs=1, proxyserver=None):
    i = last

    while True:
        print("\rAttempt no. " + str(total_runs), end="")

        test_url = "https://crz.gov.sk/dodatky-k-zmluvam/?page=" + str(i)

        if proxyserver is None:
            page = requests.get(test_url)
        else:
            page = requests.get(test_url, proxies=proxyserver)

        doc = lh.fromstring(page.content)

        found = len(doc.find_class('area area7'))

        if found == 0:
            if stage < 4:
                if stage == 0:
                    i -= 10000
                elif stage == 1:
                    i -= 1000
                elif stage == 2:
                    i -= 100
                else:
                    i -= 10

                stage += 1
                total_runs += 1

                if proxyserver is None:
                    i, total_runs = find_max_pages(stage, i, total_runs)
                else:
                    i, total_runs = find_max_pages(stage, i, total_runs, proxyserver=proxyserver)
                break

            else:
                break

        if stage == 0:
            i += 10000
        elif stage == 1:
            i += 1000
        elif stage == 2:
            i += 100
        elif stage == 3:
            i += 10
        else:
            i += 1

        total_runs += 1

    return i, total_runs

# end find_max_pages


print("Starting search for the last subpage of total supplement list by variable step approximation...")

# check, if we need a proxy, whether it's valid and online:
proxyname1, proxyname2 = proxyhandler.query_proxy()

if proxyname1 != "" or proxyname2 != "":
    # create the object, assign it to a variable
    if proxyname1 != "" and proxyname2 != "":
        proxy = {"http": proxyname1, "https": proxyname2}
    elif proxyname1 != "":
        proxy = {"http": proxyname1, "https": ""}
    elif proxyname2 != "":
        proxy = {"http": "", "https": proxyname2}

    proxy_present = True


if proxy_present is True:
    last_page, total = find_max_pages(0, proxyserver=proxy)
else:
    last_page, total = find_max_pages(0)

print(f'\n\rThe last subpage is: {last_page - 1}, found after {total} attempts.')
print('Going to crawl CRZ GOV and build DB_supplements.')

page_ID = 0
supplements_ID = []
proceed = True
cont = ""
IDs = []
supplement = []
error_counter = 0

start_ID = input("Please enter starting subpage number for processing or (C)ancel: ")

if start_ID.isnumeric():
    if int(start_ID) < 0:
        print("Negative numbers not allowed. Using 0 as starting number.")
        page_ID = 0

    elif int(start_ID) > last_page:
        print("Starting number higher than last subpage index. Exiting...")
        proceed = False

    else:
        page_ID = int(start_ID)
else:
    if start_ID == 'c' or start_ID == 'C':
        print("Exiting...")
        proceed = False
    else:
        print("Invalid input. Using 0 as starting number.")
        page_ID = 0

if proceed:
        page_ID = int(start_ID)

        while(page_ID <= last_page - 1):
            # for page_ID in range(int(start_ID), last_page):
            try:
                url = 'https://crz.gov.sk/dodatky-k-zmluvam/?page=' + str(page_ID)

                print(f'Processing subpage {page_ID} of {last_page - 1}...')

                if proxy_present is True:
                    page = requests.get(url, proxies=proxy)
                else:
                    page = requests.get(url)

                doc = lh.fromstring(page.content)

                tr_elements = doc.xpath('//tr')

                supplements = [supplement for supplement in tr_elements if len(supplement) == 5]

                IDs = [find_ID.findall(supplement[1][0].attrib['href'])[0] for supplement in supplements[1:] if supplement[1][0].attrib['href'] != ""]

                operstring = '\n'.join(IDs)

                f = open('IDs.txt', 'a')
                f.write(operstring + '\n')
                f.close()

                supplements_ID = supplements_ID + IDs

                if page_ID == last_page - 1000:
                    print("1000 subpages to end, forcing refresh of total subpages")

                    if proxy_present is True:
                        last_page, total = find_max_pages(0, proxyserver = proxy)
                    else:
                        last_page, total = find_max_pages(0)

                    print(f'\n\rThe last subpage is: {last_page - 1}, found after {total} attempts.')
                # end if

                # Resetting error_counter if current subpage went on successful:
                error_counter = 0
                page_ID += 1

            except Exception as e:
                print(f'{repr(e)}, retrying...')

                # Only if loop crashes 100 times in a row (without a single successful one),
                # the execution is terminated.
                error_counter += 1

                if error_counter < 100:
                    pass
                else:
                    print("Retried for 100 times without success, terminating...")
                    break
