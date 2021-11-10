# Matej Badin | UHP | 2019                                             |
# Marian Tihanyi | IDRP | 2021										   |
# -------------------------------------------------------------------- |
# Packages needed :  numpy, re, os, pandas                             |
# -------------------------------------------------------------------- |
# Tag and filter extracted tables based on keywords                    |
# -------------------------------------------------------------------- |

import os
import re
import pandas as pd
import ast
import shutil


def natural_sort(l):
	convert = lambda text: int(text) if text.isdigit() else text.lower()
	alphanum_key = lambda key: [convert(c) for c in re.split('([0-9]+)', key)]
	return sorted(l, key = alphanum_key)


find_txt = re.compile('txt')
working_dir = os.getcwd()+'/contracts_mandayrates/'
final_dir = os.getcwd()+'/contracts_mandayrates_tables/'
extract_number = re.compile(r'\d+')

# Check if the "contracts_mandayrates_tables" directory exists.
# If yes, delete its contents. if no, create it:
if os.path.isdir(final_dir):
	shutil.rmtree(final_dir)
else:
	os.mkdir(final_dir)

# List all subdirectories with tables in working dir:
subdirectories = []

for ndir in os.listdir(working_dir):
	if os.path.exists(os.path.join(working_dir, ndir + "/tables")):
		if os.path.isdir(os.path.join(working_dir, ndir + "/tables")):
			if os.listdir(os.path.join(working_dir, ndir + "/tables")):
				subdirectories.append(ndir + '/tables/')

# Import file with keywords
fo = open('keywords.txt', 'r', encoding='utf-8')
lines = fo.readlines()
fo.close()

categories = []

# Import keywords from keywords.txt and prepare data structures
for line in lines:
	line = line.split(',')

	category_name = line[0]
	keywords = []
	hits = []
	hits_per_category = 0

	for i, item in enumerate(line):
		if i > 0:
			keywords.append(item.strip())
			hits.append(0)

	categories.append([category_name, keywords, hits, hits_per_category])

header_categories = [category[0] for category in categories]
header_keywords = []

# Import metadata from text_tagged file
DB_import = pd.read_csv('CRZ_DB_clean_text_tagged.csv', delimiter='|')

try:
	DB_import = DB_import.drop('Unnamed: 0')

except:
	pass

header_import = ['Nazov', 'ID', 'Inner-ID', 'Objednavatel_ICO', 'Objednavatel', 'Objednavatel_adresa', 'Dodavatel_ICO', 'Dodavatel', 'Dodavatel_adresa',
				 'Datum_zverejnenia', 'Datum_podpisu', 'Datum_platnosti', 'Datum_ucinnosti', 'Posledna_zmena', 'Cena_konecna', 'Cena_podpisana', 'Rezort', 'Typ',
				 'Prilohy_IDs', 'Prilohy_nazvy', 'Prilohy_odkazy', 'Prilohy_velkosti', 'Prilohy_datum', 'Pocet_stran', 'Pocet_tabuliek', 'Tabulky_strany', 'Pocet_znakov']

header_metadata = ['Nazov','ID','Inner-ID','Objednavatel_ICO','Objednavatel','Objednavatel_adresa','Dodavatel_ICO','Dodavatel','Dodavatel_adresa',
				   'Datum_zverejnenia','Datum_podpisu','Datum_platnosti','Datum_ucinnosti','Posledna_zmena','Cena_konecna','Cena_podpisana','Rezort','Typ',
				   'Prilohy_IDs','Prilohy_nazvy','Prilohy_odkazy','Prilohy_velkosti','Prilohy_datum','Pocet_stran','Tabulka_strana','Tabulka_cislo','Pocet_znakov']

len_header_import = len(header_import)
DB_import = DB_import.drop(DB_import.columns.difference(header_import), axis=1)

# Produce new header for the new file:
header_sum_cat = ['Výskyty']
header_categories = [category[0] for category in categories]
header_keywords = []

for category in categories:
	header_keywords = header_keywords + category[1]

header = header_metadata + header_sum_cat + header_categories + header_keywords + ['Tabulka_subor']

# Tag tables for each CSV table in each subdirectory,
# produce another CSV in which each row contains (meta)information about some table
N_dir = len(subdirectories)
row_list = []

for index, directory in enumerate(subdirectories):
	table_file_list = []
	print('Processing contract ID:', directory.strip('/tables'), '-', index + 1, 'of', N_dir)

	table_dir = os.path.join(working_dir, directory)
	tables = [f for f in os.listdir(table_dir) if os.path.isfile(os.path.join(table_dir, f)) and f[-4:].casefold() == ".csv"]

	# Sort tables according to number in table_number.csv
	sort_keys = {int(table.split('_')[len(table.split('_')) - 1].strip('.csv')): table for table in tables}
	tables = [val for sort_key, val in sorted(sort_keys.items())]

	for table in tables:
		fo = open(os.path.join(table_dir, table), 'r', encoding='utf-8')
		lines = fo.readlines()
		fo.close()

		text = ''

		for line in lines:
			text += line.casefold().replace('\n', ' ')

		del lines

		for category in categories:
			category[3] = 0
			for j, keyword in enumerate(category[1]):
				category[2][j] = text.count(keyword.casefold())
				category[3] += category[2][j]

		# Extract metadata and join it with counted hits
		s = table_dir.split("/")

		if s[len(s) - 3].isnumeric():
			row = DB_import.loc[DB_import['ID'] == int(s[len(s) - 3])]

			meta_data = []

			for i in range(0, len(header_import) - 1):
				try:
					meta_data.append(row.iat[0, i])
				except Exception as e:
					print(e)
					pass

			local_index = list(sort_keys.keys())[list(sort_keys.values()).index(table)]
			meta_data.append(local_index)

			# Insert number of pages:
			if float(row['Pocet_tabuliek']) > 0 and local_index <= float(row['Pocet_tabuliek']):
				meta_data[len(meta_data)-2] = ast.literal_eval(meta_data[len(meta_data)-2])[local_index - 1]

			else:
				meta_data[len(meta_data)-2] = 0

			data_hits = []

			for category in categories:
				data_hits += category[2]

			sum_data = 0
			for category in categories:
				sum_data += category[3]

			data = meta_data + [sum_data] + [category[3] for category in categories] + data_hits + [table]
			row_list.append(dict((label, data[i]) for i, label in enumerate(header)))

# Save unranked CSV table:
DB_export = pd.DataFrame(row_list, columns=header)
DB_export.to_csv('CRZ_DB_clean_tables.csv', header=header, sep='|')

print("Filtering out contracts...")

# Filter out all irrelevant tables and produce CSV, which has only tables with at least:
# - One job position, or:
# - One job description, or:
# - Total non-zero sum in the quantifier field.
# Note: This prerequisite together with corresponding code might be removed
# or significantly changed in the future, mainly due to the fact that it is
# incompatible with the need to have flexible code able to process
# any desired keywords from the keywords.txt file.
delete_rows = []

for index, row in DB_export.iterrows():
	print("Processing ID:", row['ID'], end="")

	if not(((float(row['Pozícia']) > 0) or (float(row['Popis práce']) > 0)) or (float(row['Kvantifikátor']) > 0)):
		delete_rows.append(index)
		print(" ... filtered out.")
	else:
		print(" ... included.")

print('Number of tables : ', DB_export.shape[0],'| Filtered out : ', len(delete_rows))

DB_export = DB_export.drop(delete_rows)
DB_export.to_csv('CRZ_DB_clean_tables.csv', sep='|')

# Copy all relevant tables into directory _tables
for index, row in DB_export.iterrows():
	src_dir = working_dir + str(row['ID']) + '/tables/'
	dest_dir = final_dir + str(row['ID'])

	if not os.path.exists(dest_dir):
		os.makedirs(dest_dir)

	source = os.path.join(src_dir, str(row['Tabulka_subor']))
	destination = os.path.join(dest_dir, str(row['Tabulka_subor']))
	os.system('cp '+source+' '+destination)
