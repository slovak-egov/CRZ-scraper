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

import proxyhandler

proxy_present = False

find_price_dot = re.compile(r'\d+\.\d+')
find_price_without_dot = re.compile(r'\d+')


def find_price(string):
	if len(find_price_dot.findall(string)) > 0:
		return float(find_price_dot.findall(string)[0])
	else:
		return float(find_price_without_dot.findall(string)[0])


find_ID = re.compile(r'\d+')
header  = ['Nazov','ID_supplement','ID_zmluva','Inner-ID','Objednavatel','Dodavatel','Datum_podpisu','Datum_platnosti','Datum_ucinnosti','Poznamka','Prilohy']

fo  = open('IDs.txt','r')
IDs = fo.readlines()
fo.close()

row_list = []
print('Going to build supplemental agreements DB of',len(IDs))

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
			page = requests.get(url, proxies=proxy)
		else:
			page = requests.get(url)

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

	except:
		pass

	if i % 50 == 0:
		DB = pd.DataFrame(row_list, columns = header)
		DB.to_csv('CRZ_DB_supplements.csv', header = header, sep = '|')
