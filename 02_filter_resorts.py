# This module is currently not used.									|
# Matej Badin | UHP | 2019                                              |
# --------------------------------------------------------------------  |
# Packages needed :  pandas                                             |
# This is the original script, unchanged.								|
# --------------------------------------------------------------------  |
# Filter contracts from supplier list                                   |

# import os
# import xml.etree.cElementTree as ET
# import numpy as np
import pandas as pd

DB = pd.read_csv('CRZ_DB_with_supplements.csv', delimiter = '|')
number_of_contracts = DB.shape[0]

remove = []
for index, row in DB.iterrows():
	if index % 1000 == 0:
		print('Processed:', index, '', number_of_contracts)

	# Only contracts from resort 114723 are kept:
	if row['Rezort'] != 114723:
		remove.append(index)

print('Find relevant: ', number_of_contracts-len(remove),' out of ', number_of_contracts)

# Clean irrelevant
DB_clean = DB.drop(DB.index[remove])

# Produce the rest
header = ['Nazov','ID','Inner-ID','Objednavatel_ICO','Objednavatel','Objednavatel_adresa','Dodavatel_ICO','Dodavatel','Dodavatel_adresa',
		  'Datum_zverejnenia','Datum_podpisu','Datum_platnosti','Datum_ucinnosti','Posledna_zmena','Cena_konecna','Cena_podpisana','Rezort','Typ','Stav','Prilohy','Dodatky']

DB_clean.iloc[:,2:].to_csv('CRZ_DB_resort.csv', header = header, sep='|')
