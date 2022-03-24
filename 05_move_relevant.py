# Matej Badin | UHP | 2019                                             |
# Marian Tihanyi | IDRP | 2021										   |
# -------------------------------------------------------------------- |
# Packages needed :  numpy, re, os, pandas, ast                        |
# -------------------------------------------------------------------- |
# The script moves folders inlcuding their content for contracts,      |
# which are listed in the ...text_tagged.csv database into the		   |
# ./contracts_mandayrates/ folder.									   |
# This process might become integrated into one of other scripts       |
# in the future.													   |
# -------------------------------------------------------------------- |

import os
import pandas as pd

source_dir = os.getcwd()+'/contracts_text/'
direction_dir = os.getcwd()+'/contracts_relevant/'

if not os.path.exists('contracts_relevant'):
	os.makedirs('contracts_relevant')

print("Loading database...")
DB_clean_tagged = pd.read_csv('CRZ_DB_clean_text_tagged.csv', delimiter='|', dtype=str)
total = DB_clean_tagged.shape[0]

for index, row in DB_clean_tagged.iterrows():
	percentage = index/total * 100
	contract = str(row['ID'])
	print(f"\r[{round(percentage,1):>5}%] Copying contract ID: {contract}", end="")

	if not os.path.exists(direction_dir + contract):
		os.makedirs(direction_dir + contract)

	os.system('cp ' + source_dir + contract + '/* ' + direction_dir + contract)
