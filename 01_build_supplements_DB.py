# Matej Badin | UHP | 2019                                             |
# Marian Tihanyi | IDRP | 2021 - debugged + added proxy support        |
# -------------------------------------------------------------------- |
# Packages needed :  xml.etree.ElementTree, os, proxyhandler (own      |
#                    module to enable internet connection via proxy)   |
# -------------------------------------------------------------------- |
# Crawl tables with supplemental agreements on CRZ GOV                 |
# Include them inside DB get by 01_parse_xml.py                        |
# -------------------------------------------------------------------- |

import requests
import lxml.html as lh
import pandas as pd
import re
import time
from functools import wraps

import proxyhandler

proxy_present = False

find_price_dot = re.compile(r'\d+\.\d+')
find_price_without_dot = re.compile(r'\d+')


# Decorator "retry" downloaded from: http://www.saltycrane.com/blog/2009/11/trying-out-retry-decorator-python/
# Original from: http://wiki.python.org/moin/PythonDecoratorLibrary#Retry
#
# Reason:
# Connection to crz.gov.sk seemed to have been randomly interrupted or dropped, respectively. That fired Timeout error,
# which belongs to ConnectionError class in requests. Despite attempts to catch the error (some of them successful),
# it would eventually crash, so I decided to google up some working solutions. This is one of them.
def retry(ExceptionToCheck, tries=10, delay=3, backoff=2, logger=None):
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


def find_price(string):
	if len(find_price_dot.findall(string)) > 0:
		return float(find_price_dot.findall(string)[0])
	else:
		return float(find_price_without_dot.findall(string)[0])


find_ID = re.compile(r'\d+')
header = ['Nazov','ID_supplement','ID_zmluva','Inner-ID','Objednavatel','Dodavatel','Datum_podpisu','Datum_platnosti','Datum_ucinnosti','Poznamka','Prilohy']

fo = open('IDs.txt', 'r')
IDs = fo.readlines()
fo.close()

row_list = []
print('Going to build supplemental agreements DB of', len(IDs))

proxyname1, proxyname2 = proxyhandler.query_proxy()

if proxyname1 != "" or proxyname2 != "":
	if proxyname1 != "" and proxyname2 != "":
		proxy = {"http": proxyname1, "https": proxyname2}
	elif proxyname1 != "":
		proxy = {"http": proxyname1, "https": ""}
	elif proxyname2 != "":
		proxy = {"http": "", "https": proxyname2}

	proxy_present = True

for i, ID in enumerate(IDs):
	try:
		ID = ID.strip()
		print('Processing ID:',ID,' ',i+1,'out of',len(IDs))
		url = 'https://www.crz.gov.sk/index.php?ID='+ID+'&l=sk'

		if proxy_present is True:
			# Instead of urllib, we use requests.get to download files. Thanks to random connection dropouts, the function is treated with a decorator.
			@retry(requests.ConnectionError, tries=10, delay=3, backoff=2)
			def urlopen_with_retry():
				return requests.get(url, allow_redirects=True, proxies=proxy)

		else:
			# Instead of urllib, we use requests.get to download files. Thanks to random connection dropouts, the function is treated with a decorator.
			@retry(requests.ConnectionError, tries=10, delay=3, backoff=2)
			def urlopen_with_retry():
				return requests.get(url, allow_redirects=True)

		page = urlopen_with_retry()

		doc = lh.fromstring(page.content)

		# Metadata about price and dates
		dates_area = doc.find_class('area area1')[0][0][0]

		if len(dates_area) == 4:
			date_signed = dates_area[0][1].text_content()
			date_efficiency = dates_area[1][1].text_content()
			date_validity = dates_area[2][1].text_content()
			price = find_price(dates_area[3][1].text_content())
		else:
			date_signed = 'neuvedené'
			date_efficiency = dates_area[0][1].text_content()
			date_validity = dates_area[1][1].text_content()
			price = find_price(dates_area[2][1].text_content())

		# Metadata about name, number, supplier and purchaser
		text_area = doc.find_class('b_right area area3')[0][1][0]

		supplement_number = text_area[0][1].text_content()
		supplement_purchaser = text_area[1][1].text_content().strip().replace('\n', ' ')
		supplement_supplier = text_area[2][1].text_content().strip().replace('\n', ' ')
		supplement_name = text_area[3][1].text_content().strip().replace('\n', ' ')

		if len(text_area) == 5:
			supplement_note = text_area[4][1].text_content()
		else:
			supplement_note = ''

		# Link to the contract ID in CRZ.gov.sk
		contract_ID  = find_ID.findall(doc.find_class('area5')[0][0].attrib['href'])[0]

		# Supplement attachments
		supplement_attachments = []
		attachments = doc.find_class('area area2')[0][1]

		for attachment in attachments:
			att_size = float(re.findall("\d+\.\d+", attachment.text_content())[0])

			attachment_link = 'https://www.crz.gov.sk'+attachment[1].attrib['href']
			attachment_name = attachment[1].text_content()

			if 'Text' in attachment[0].attrib['alt']:
				attachment_text = True
			else:
				attachment_text = False

			supplement_attachments.append([attachment_name,attachment_link,int(round(att_size * 1000)),attachment_text])

		data = [supplement_name, ID, contract_ID, supplement_number, supplement_purchaser, supplement_supplier, date_signed, date_validity, date_efficiency, supplement_note, supplement_attachments]
		row_list.append(dict((label,data[i]) for i, label in enumerate(header)))

	except Exception as e:
		# print(repr(e))
		# input("Press any key to continue")
		pass

	if i % 50 == 0:
		DB = pd.DataFrame(row_list, columns = header)
		DB.to_csv('CRZ_DB_supplements.csv', header = header, sep = '|')
