# Matej Badin | UHP | 2019                                              |
# Marian Tihanyi | IDRP | 2021											|
# ----------------------------------------------------------------------|
# Changelog 10/2021:							 						|
# The filtering options were enhanced by:								|
# 	- Total minimum contract value (total amount of money paid for		|
# 	  contracted services)												|
# 	- Soonest publishing date (keep all the contracts after that date)	|
# --------------------------------------------------------------------  |
# Packages needed :  pandas, re											|
# --------------------------------------------------------------------  |
# Filter contracts from supplier list                                   |

import pandas as pd
import re


# Recursive function date_is_equal_or_later()
#
# Returns if date1 is equal or later than date2. The processing takes place as follows:
#
# 1. Level 0 of both dates (year) is compared:
# 		- If year of date1 is later than year of date2, True is returned.
# 		- If year of date2 is later than year of date1, False is returned.
# 		- If both years are the same, level is incremented by 1 and the function calls
# 		  itself again (in deeper level) ->
# 		  ->
# 2. Level 1 of both dates (month) is compared:
# 		- If month of date1 is later than month of date2, True is returned.
# 		- If month of date2 is later than month of date1, False is returned.
# 		- If both months are the same, level is incremented by 1 and the function calls
# 		  itself again (in deeper level) ->
# 		  ->
# 3. Level 2 of both dates (day) is compared:
# 		- The drill is the same (either one value is bigger or smaller, or in case both
# 		  values are equal, deeper level is required). ->
# 		  ->
# ...
# 6. As the last possible recursive run, level 5 of both dates (seconds) is compared:
# 		- In case either seconds value is bigger or smaller, respective value is
# 		  returned.
# 		- In case both seconds values are equal, True is returned, which is a more
# 	      philosophical than numeric issue - the function is called
# 	      "date_is_equal_or_later", which means that in case both dates are equal
# 	      to the second, date1 is equal to date2, so that in order to follow
# 	      the function's name, it must return True.
# 		  In case the name was "date_is_later" (without the "equal" wording),
# 		  it would return False.
#
# Parameters:
# -----------
# date1, date2 =	The submitted dates must be in the following format:
# 					YYYY-MM-DD hh:mm:ss
# 					The actual placing of delimiters might differ (although there
# 					would not be much of a use for that), but there always must be
# 					maximum one delimiter of a kind between two values, from
# 					the following group: dash, space, colon).
#
# level =			Level of recursion for the function to know which value
# 					to extract from the dates:
# 					(year, month, day, hour, minute, second).
#
# Return value:
# -------------
# True =			date1 is equal or later than date2
# False =			date2 is later than date1
# None =			malformed dates were submitted to the function (ValueError)
#
def date_is_equal_or_later(date1, date2, level):
	date_time = [re.split("[\-\ \:]", date1, maxsplit=6), re.split("[\-\ \:]", date2, maxsplit=6)]

	try:
		if int(date_time[0][level]) < int(date_time[1][level]):
			return False

		elif int(date_time[0][level]) > int(date_time[1][level]):
			return True

		elif int(date_time[0][level]) == int(date_time[1][level]):
			if level < 5:
				level += 1
				return date_is_equal_or_later(date1, date2, level)

			else:
				return True

	except ValueError:
		return None


print("Loading databases (contracts and filter), please wait...")

DB = pd.read_csv('CRZ_DB_with_supplements.csv', delimiter='|', dtype=str)
print('DB of contracts loaded to memory.')
print('Filtering relevant contracts')
number_of_contracts = DB.shape[0]

# The companies.csv file contains data about a narrow list of companies
# and their CINs for CIN-based filtering:
companies = pd.read_csv('companies.csv', delimiter='|', dtype=str)
companies = companies['ICO'].tolist()
companies = [str(x) for x in companies]
remove = []
control_db = []

j = 0
input_ok = False
contract_vol = 0.0    # We want to let the interpreter know that this is a float, not integer.
test_date = ""

# Minimum contract financial volume - user input including verification:
while input_ok is False:
	query = input("Please enter minimum financial volume of contract to include (leave blank to omit filtering by financial volume): ")

	try:
		if query != "":
			contract_vol = float(query)

			if contract_vol < 0:
				contract_vol = abs(contract_vol)
				print(f"Negative values are not allowed. Using absolute value of {contract_vol} for input.")
		else:
			contract_vol = 0

		input_ok = True
	except:
		print("Invalid input. Try again.")

# The soonest publishing date of a contract - user input including verification:
input_ok = False

while input_ok is False:
	query = input("Please enter starting date of contract publishing to include in formats YYYY-MM-DD or YYYY-MM-DD hh:mm:ss (leave blank to omit filtering by date): ")

	if not re.search("\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", query) is None:
		test_date = query
		input_ok = True

	elif not re.search("\d{4}-\d{2}-\d{2}", query) is None:
		print("Since no time was entered, assuming 00:00:00 as reference time.")
		test_date = query + " 00:00:00"
		input_ok = True

	elif query == "":
		print("Disabling filtering by date (automatically including all contracts meeting other criteria regardless of date).")
		test_date = "2011-01-01 00:00:00"
		input_ok = True

	else:
		print("Invalid input. Try again.")

input("Press any key to start the process.")

# Scan the dumped DB and remove contracts, in which the supplier does not match any CIN from the provided list (companies.csv):
for i in range(0, number_of_contracts):
	percentage = i/number_of_contracts * 100
	tosslevel = 0

	print(
		f"\r{round(percentage, 1):>5}% completed. Item no: {i:>10}, saved: {j:>10} of {number_of_contracts:>10}, ID: {str(DB.iloc[i, 3]):>10},"
		f"name: {str(DB.iloc[i, 2]) if len(str(DB.iloc[i, 2])) < 50 else str(DB.iloc[i, 2])[:46] + '...'}".ljust(146), end=" -> "
	)

	# These fields are extracted mainly for the purpose of further filtering or changing output format:
	attachments = str(DB.iloc[i, 21])
	price = str(DB.iloc[i, 17])
	publishing_date = str(DB.iloc[i, 11])

	# If CIN is not in companies database, remove the record:
	if not(str(DB.iloc[i, 8]).replace(" ","") in companies):
		remove.append(i)
		tosslevel = 1

	elif attachments == '[]' or attachments == '' or 'https://' not in attachments:
		remove.append(i)
		tosslevel = 2

	# Here we also check minimum financial volume and delete duplicates:
	else:
		# Check for minimum financial volume:
		try:
			if float(price) < contract_vol:
				remove.append(i)
				tosslevel = 3

		# If price is some gibberish, we exclude such contract by default:
		except ValueError:
			remove.append(i)
			tosslevel = 3

		# Filtering by date, if desired:
		if tosslevel == 0:
			if test_date != "":
				if date_is_equal_or_later(publishing_date, test_date, 0) is False or date_is_equal_or_later(publishing_date, test_date, 0) is None:
					remove.append(i)
					tosslevel = 4

		# If the contract passed all the above tests, we check, whether it is a duplicate
		# of any already added contract or not:
		if tosslevel == 0:
			contract_ID = str(DB.iloc[i, 3])
			contract_name = str(DB.iloc[i, 2]).strip().replace('\n', ' ')

			# Contract already in control DB, toss out:
			if [contract_name, contract_ID] in control_db:
				remove.append(i)
				tosslevel = 5

			# Contract not in control DB, we let it in, but add it to the control DB as well.
			else:
				control_db.append([])
				control_db[j].append(contract_name)
				control_db[j].append(contract_ID)
				j += 1

	if tosslevel == 0:
		print("saved in target database.")

		# Loop through  particular items in a row and replace new lines with spaces.
		# This is only valid for columns before the one containing attachment info.
		for k in range(1, 19):
			if type(DB.iloc[i, k]) == str:
				DB.iloc[i, k] = DB.iloc[i, k].strip().replace('\n', ' ')

	elif tosslevel == 1:
		print("discarded for non-matching CIN.", end="")

	elif tosslevel == 2:
		print("discarded for nonexistent attachments.")

	elif tosslevel == 3:
		print("discarded for low total financial volume.")

	elif tosslevel == 4:
		print("discarded for being published before threshold date.")

	elif tosslevel == 5:
		print("discarded for being a duplicate of another record.")


print('')
print('Found relevant: ', number_of_contracts-len(remove),' out of ',number_of_contracts)

# Clean irrelevant
DB_clean = DB.drop(DB.index[remove])

# Produce the rest
header = ['Nazov','ID','Inner-ID','Objednavatel_ICO','Objednavatel','Objednavatel_adresa','Dodavatel_ICO','Dodavatel','Dodavatel_adresa',
			'Datum_zverejnenia','Datum_podpisu','Datum_platnosti','Datum_ucinnosti','Posledna_zmena','Cena_konecna','Cena_podpisana','Rezort','Typ','Stav','Prilohy','Dodatky']

DB_clean.iloc[:,2:].to_csv('CRZ_DB_clean.csv', header = header, sep='|')
