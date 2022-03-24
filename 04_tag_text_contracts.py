# Matej Badin    | UHP  | 2019                                         |
# Marian Tihanyi | IDRP | 2021 (update, improvements, port to Linux)   |
# -------------------------------------------------------------------- |
# Packages needed :  re, os, pandas, ast                               |
# -------------------------------------------------------------------- |
# Tag interesting contracts based on keywords                          |
# -------------------------------------------------------------------- |

import os
import re
import pandas as pd
import ast

find_txt = re.compile('.txt')
working_dir = os.getcwd()+'/contracts_text/'

extract_ID = re.compile(r'\d+')

contracts = {}

# Import clean table to extract metadata from it
DB_clean = pd.read_csv('CRZ_DB_clean.csv', delimiter='|', dtype=str)
dir_list = os.listdir(working_dir)

print("Loading filenames...")

for i, f in enumerate(dir_list):
	percentage = i/len(dir_list) * 100

	if not os.path.isfile(os.path.join(working_dir, f)):
		if f in DB_clean['ID'].tolist():
			contracts[f] = []

			for c in os.listdir(working_dir + f):
				if '.txt' in c:
					contracts[f].append(c)

	print(f"\r{round(percentage,1)}%", end="")

contracts_txt = {}

# We add only those contracts, which text is actually there:
for cid, fl in contracts.items():
	contracts_txt[cid] = []

	for f in fl:
		if len(find_txt.findall(f)) > 0:
			contracts_txt[cid].append(f)

print("\nFilenames loading and list generation completed.")

# Keywords are stored in keywords.txt provided in rows as categories separated by comma, first word is name of the category
# Script searches for keywords, keywords are prepared by lowercasing

fo = open('keywords.txt', 'r', encoding='utf-8')
lines = fo.readlines()
fo.close()

categories = []

# Import keywords from keywords.txt and prepare data structure
for line in lines:
	line = line.split(',')

	category_name = line[0]
	keywords = []
	hits = []
	hits_per_category = 0

	for i, item in enumerate(line):
		if i > 0:
			keywords.append(item.strip().casefold())
			hits.append(0)

	categories.append([category_name, keywords, hits, hits_per_category])

# Prepare header for export
header_metadata = ['Nazov','ID','Inner-ID','Objednavatel_ICO','Objednavatel','Objednavatel_adresa','Dodavatel_ICO','Dodavatel','Dodavatel_adresa',
					'Datum_zverejnenia','Datum_podpisu','Datum_platnosti','Datum_ucinnosti','Posledna_zmena','Cena_konecna','Cena_podpisana','Rezort','Typ',
					'Prilohy_IDs','Prilohy_nazvy','Prilohy_odkazy','Prilohy_velkosti','Prilohy_datum']

header_sum_cat = ['Výskyty']
header_categories = [category[0] for category in categories]
header_keywords = []

for category in categories:
	header_keywords = header_keywords + category[1]

header = header_metadata + header_sum_cat + header_categories + header_keywords

row_list = []
N = len(contracts_txt)
i = 0

number_of_characters = []

# Go through all processed text files, lowercase it, for every keyword count number of occurrences
for contract, filelist in contracts_txt.items():
	print('Analysing contract ID: ', contract, ', ', i + 1, 'out of', N)

	text = ''

	for f in filelist:
		print("...filename:", f)

		fo = open(working_dir + contract + '/' + f, 'r', encoding='utf-8')
		lines = fo.readlines()
		fo.close()

		for line in lines:
			text += line.casefold().replace('\n',' ')

		del lines

	for category in categories:
		category[3] = 0
		for j, keyword in enumerate(category[1]):
			category[2][j] = text.count(keyword.casefold())
			category[3] += category[2][j]

	# Extract metadata and join it with counted hits
	# row = DB_clean.loc[DB_clean['ID'] == int(extract_ID.findall(contracts[contract])[0])]
	row = DB_clean.loc[DB_clean['ID'] == contract]

	meta_data = [row.iloc[0, j] for j in range(1, 19)]
	attachment_data = ast.literal_eval(row.iloc[0, 20])

	IDs = []
	names = []
	links = []
	sizes = []
	date = ""

	# The cell containing info about attachments has variable length depending on
	# actual number of attachments (there may be multiple IDs, links, etc.).
	# However, there is always only one date.
	for m, attachment in enumerate(attachment_data):
		# Save IDs and names in one step:
		# The condition is following:
		# If a number is saved as string, it is an ID
		# Name always follows ID as next element
		if type(attachment) == str and str(attachment).isnumeric():
			IDs.append(attachment)

			if m + 1 < len(attachment_data):
				names.append(str(attachment_data[m + 1]))

		# Save links (must contain https://):
		if str(attachment).find("https://") >= 0:
			links.append(str(attachment))

		# Sizes are always stored as integer:
		if type(attachment) == int:
			sizes.append(str(attachment))

		# Saving dates requires checking for a specific regex pattern:
		if not re.search("\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", str(attachment)) is None:
			date = str(attachment)

	# If there are links and dates, meta data are valid:
	if links and date != "":
		meta_data = meta_data + [", ".join(IDs), ", ".join(names), ", ".join(links), ", ".join(sizes), date]

	else:
		meta_data = meta_data + ["","","","",""]

	data_hits = []

	for category in categories:
		data_hits += category[2]

	sum_data = 0
	for category in categories:
		sum_data += category[3]

	data = meta_data + [str(sum_data)] + [category[3] for category in categories] + data_hits

	row_list.append(dict((label, data[k]) for k, label in enumerate(header) if k < len(data)))

	# Count number of characters in contract:
	number_of_characters.append(len(text))

	i += 1

# Save unranked csv table
DB_clean_tagged = pd.DataFrame(row_list, columns = header)
DB_clean_tagged.to_csv('DB_clean_text_tagged.csv', header = header, sep='|')

# Rank contracts according to number of keywords, number of characters in contract and price.
# Ranking is based on three categories listed above, in each category 10 points are distributed
# according to logarithmic scale and then added. Contracts are sorted by the rank.

# Insert new column -- number of characters
DB_clean_tagged.insert(24, 'Pocet_znakov', number_of_characters)
DB_clean_tagged = DB_clean_tagged.sort_values(by=['Výskyty','Pocet_znakov','Cena_konecna'], ascending = False)

# Sort rows by number of hits, number of characters and final prize
delete_rows = []

for index, row in DB_clean_tagged.iterrows():
	# if str(row['Pozícia']).isnumeric() and str(row['Popis práce']).isnumeric():
	#if float(row['Pozícia']) == 0 and float(row['Popis práce']) == 0:
	#	delete_rows.append(index)

	# if str(row['Výskyty']).isnumeric():
	if float(row['Výskyty']) == 0:
		delete_rows.append(index)

print('Sorted : ', N, '| Filtered out : ', len(delete_rows))

DB_clean_tagged = DB_clean_tagged.drop(delete_rows)
DB_clean_tagged.to_csv('CRZ_DB_clean_text_tagged.csv', sep='|')
