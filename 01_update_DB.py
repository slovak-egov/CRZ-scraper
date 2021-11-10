# Matej Badin | UHP | 2019                                             |
# -------------------------------------------------------------------- |
# Packages needed :  numpy, xml.etree.ElementTree, os                  |
# -------------------------------------------------------------------- |

import requests
import lxml.html as lh
import pandas as pd
import re

print("Loading source databases, please wait...")

DB = pd.read_csv('CRZ_DB.csv', sep='|', dtype=str)
DB_supplements = pd.read_csv('CRZ_DB_supplements.csv', sep='|', dtype=str)

print("Databases loaded successfully.")

DB['Dodatky'] = ''

# Structure dictionary (ID_contract,[list of supplements])
supplements = dict()

count = 0
n_supplements = DB_supplements.shape[0]
n_contracts = DB.shape[0]

# Crawl DB_supplements
for index, row in DB_supplements.iterrows():
	count += 1
	print('Merging metadata for supplemental agreement', count, 'of', n_supplements)

	ID_contract = row['ID_zmluva']

	header_import = ['Nazov','ID_supplement','Inner-ID','Objednavatel','Dodavatel','Datum_podpisu','Datum_platnosti','Datum_ucinnosti','Poznamka','Prilohy']
	supplement_data = []

	for item in header_import:
		supplement_data.append(row[item])

	supplement_data.append('https://www.crz.gov.sk/index.php?ID='+str(row['ID_supplement'])+'&l=sk')

	if ID_contract in supplements:
		supplements[ID_contract].append(supplement_data)
	else:
		supplements[ID_contract] = [supplement_data]

n_supplements = len(supplements)
not_found = 0
ID_counter = 0
used_IDs = []
duplicate_rows = []
duplicate_counter = 0

for index, ID_contract in enumerate(supplements):
	print('Contract ID:', ID_contract, 'supplement:', index + 1, 'of', n_supplements, '| Processed:', ID_counter, 'of', n_contracts, 'total contracts.', end="")

	location = DB.index[DB['ID'] == ID_contract]

	if len(location) > 0:
		for i, row_index in enumerate(location.tolist()):
			if i == 0:
				DB.at[location[0], 'Dodatky'] = supplements[ID_contract]
			else:
				duplicate_rows.append(row_index)
				duplicate_counter += 1

		if len(location) == 1:
			print(" ...supplements added. ", end="")
		else:
			print(" ...supplements added and duplicates of the contract removed. ", end="")

	else:
		print(" ...contract not found in source database. ", end="")
		not_found += 1

	if ID_contract not in used_IDs:
		used_IDs.append(ID_contract)
		ID_counter += 1

	print('Details: ', supplements[ID_contract])

if len(duplicate_rows) > 0:
	DB.drop(duplicate_rows)

print(f"Totally {index + 1} supplements of {n_supplements} processed for totally {ID_counter} of {n_contracts} contracts.")
print(f"Totally {duplicate_counter} duplicates were removed from the database.")
print(not_found,'contracts not merged since contracts are from corrupted XML files ...')
DB.to_csv('CRZ_DB_with_supplements.csv', sep='|')
