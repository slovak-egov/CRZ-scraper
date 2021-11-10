# Matej Badin | UHP | 2019                                              |
# --------------------------------------------------------------------  |
# Packages needed :  pandas, ast                                        |
# This is the original version of the script from 2019, unchanged.	    |
# --------------------------------------------------------------------  |

import pandas as pd
import ast

DB_clean = pd.read_csv('CRZ_DB_clean.csv', delimiter = '|')

header_import = ['Nazov', 'ID', 'Inner-ID', 'Objednavatel_ICO',
	'Objednavatel', 'Objednavatel_adresa', 'Dodavatel_ICO', 'Dodavatel',
	'Dodavatel_adresa', 'Datum_zverejnenia', 'Datum_podpisu',
	'Datum_platnosti', 'Datum_ucinnosti', 'Posledna_zmena', 'Cena_konecna',
	'Cena_podpisana', 'Rezort', 'Typ', 'Stav', 'Prilohy', 'Dodatky']

header_export = ['Nazov', 'ID', 'Inner-ID', 'Objednavatel_ICO',
	'Objednavatel', 'Objednavatel_adresa', 'Dodavatel_ICO', 'Dodavatel',
	'Dodavatel_adresa', 'Datum_zverejnenia', 'Datum_podpisu',
	'Datum_platnosti', 'Datum_ucinnosti', 'Posledna_zmena', 'Cena_konecna',
	'Cena_podpisana', 'Rezort', 'Typ', 'Stav']

DB_clean = DB_clean.drop(DB_clean.columns.difference(header_import), axis=1)
number_of_contracts = DB_clean.shape[0]

row_list = []
for index, row in DB_clean.iterrows():
	print('Processing contract:', index+1, 'out of',number_of_contracts)

	# Copy old row into the new one
	new_row = dict((label, row[label]) for label in header_export)

	attachments = ast.literal_eval(row['Prilohy'])

	supplements = []
	if not pd.isnull(row['Dodatky']):
		supplements = ast.literal_eval(row['Dodatky'].replace(' nan,', ' "nan",'))

	attachment_number = 0
	for attachment in attachments:
		attachment_number += 1

		new_row['Dodatok'] = 'FALSE'
		new_row['Dodatok_nazov'] = ''
		new_row['Dodatok_ID'] = ''
		new_row['Dodatok_cislo'] = ''
		new_row['Dodatok_datum_podpisu'] = ''
		new_row['Dodatok_datum_ucinnosti'] = ''
		new_row['Dodatok_datum_platnosti'] = ''
		new_row['Dodatok_poznamka'] = ''
		new_row['Dodatok_link'] = ''

		new_row['Priloha_ID'] = attachment[0]
		new_row['Priloha_nazov'] = attachment[1]
		new_row['Priloha_link'] = attachment[2]
		new_row['Priloha_velkost'] = attachment[3]
		new_row['Priloha_cislo'] = attachment_number

		row_list.append(new_row)

	supplement_number = 0
	for supplement in supplements:
		supplement_number += 1

		supplement_attachments = ast.literal_eval(supplement[9])
		supplement_attachment_number = 0

		for attachment in supplement_attachments:
			supplement_attachment_number += 1

			new_row['Dodatok'] = 'TRUE'
			new_row['Dodatok_nazov'] = supplement[0]
			new_row['Dodatok_ID'] = supplement[1]
			new_row['Dodatok_cislo'] = supplement_number
			new_row['Dodatok_datum_podpisu'] = supplement[5]
			new_row['Dodatok_datum_ucinnosti'] = supplement[6]
			new_row['Dodatok_datum_platnosti'] = supplement[7]
			new_row['Dodatok_poznamka'] = supplement[8]
			new_row['Dodatok_link'] = supplement[10]

			new_row['Priloha_ID'] = ''
			new_row['Priloha_nazov'] = attachment[1]
			new_row['Priloha_link'] = attachment[0]
			new_row['Priloha_velkost'] = ''
			new_row['Priloha_cislo'] = supplement_attachment_number

			row_list.append(new_row)

DB = pd.DataFrame(row_list)
DB.to_csv('CRZ_DB_with_supplements_unrolled.csv', sep='|')
