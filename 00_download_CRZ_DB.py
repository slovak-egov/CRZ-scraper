# Matej Badin | UHP | 2019                                             |
# Marian Tihanyi | IDRP | 2021 - added proxy support and user prompt   |
#                                to enter the date of the last record, |
#                                as well as specify starting date.     |
# -------------------------------------------------------------------- |
# Packages needed :  urllib, zipfile, dateutil, datetime, os, re,      |
#                    proxyhandler (own module enabling proxy support)  |
# -------------------------------------------------------------------- |
# Crawler for downloading and categorizing dump from CRZ GOV database  |
# Downloads and categorizes every contract from now to given date      |
# -------------------------------------------------------------------- |

import urllib.request
import zipfile
import os
import proxyhandler
import re
import datetime
# from datetime import date
from dateutil.rrule import rrule, DAILY

start_date = datetime.date(2011, 1, 1)

dates = []


# Function returns whether the passed year value is a leap year or not.
def is_leap_year(year):
	if (year % 4) == 0:
		if (year % 100) == 0:
			if (year % 400) == 0:
				return True
			else:
				return False


# Check today's date for reference and calculate yesterday's date (last valid date for database xml file):
x = datetime.datetime.now() - datetime.timedelta(days=1)

# Default ending date is yesterday:
end_date = x

print("*** Downloading DB of contracts from crz.gov.sk. ***")
print('')

# Gather and check starting date:
input_date = input("Please enter starting date in the form 'd.m.yyyy', 'b' for beginning of DB records, or 'y' for yesterday: ")

if input_date != 'y' and input_date != 'Y' and input_date != 'b' and input_date != 'B':
	check_date = [int(s) for s in re.findall(r'\b\d+\b', input_date)]

	if len(check_date) == 3:
		# We need to start in 2011 or later:
		if check_date[2] < int(start_date.year):
			check_date[2] = int(start_date.year)

		# We cannot start in future years:
		if check_date[2] > int(x.year):
			check_date[2] = int(x.year)

		# There are no more months in a year than 12:
		if check_date[1] > 12:
			check_date[1] = 12

		# There is no "zero" or "negative" month:
		if check_date[1] < 1:
			check_date[1] = 1

		# We cannot start in future months:
		if check_date[2] == int(x.year) and check_date[1] > int(x.month):
			check_date[1] = int(x.month)

		# There is no "zero" or "negative" day:b
		if check_date[0] < 1:
			check_date[0] = 1

		# There are no more days in a month than 31:
		if check_date[0] > 31:
			check_date[0] = 31

		# In february, there are either 28 or 29 days depending on whether the year is or is not leap:
		if check_date[1] == 2:
			if is_leap_year(check_date[2]) is True:
				if check_date[0] > 29:
					check_date[0] = 29

			else:
				if check_date[0] > 28:
					check_date[0] = 28

		# In April, June, September and November, there are only 30 days in a month:
		if check_date[1] == 4 or check_date[1] == 6 or check_date[1] == 9 or check_date[1] == 11:
			if check_date[0] > 30:
				check_date[0] = 30

		# We cannot start today or in the future, because the last xml DB file exists for yesterday:
		if check_date[2] == int(x.year) and check_date[1] == int(x.month) and check_date[0] > int(x.day):
			check_date[0] = int(x.day)

		start_date = datetime.date(check_date[2], check_date[1], check_date[0])

		print(f"Using starting date: {start_date}")

	# Reverting back to default, if some gibberish was entered:
	else:
		print("Invalid input, using default starting date: the 1st of January, 2011.")

# We decided to start at the very beginning of record keeping:
elif input_date == 'b' or input_date == 'B':
	print("Using the beginning of record keeping as the starting date: the 1st of January, 2011.")

# We decided to start yesterday:
else:
	start_date = x

# Gather and check ending date:
input_date = input("Please enter ending date in the form 'd.m.yyyy' or 'y' for yesterday: ")

if input_date != 'y':
	check_date = [int(s) for s in re.findall(r'\b\d+\b', input_date)]

	if len(check_date) == 3:
		# We need to end in 2011 or later (we cannot end before starting):
		if check_date[2] < int(start_date.year):
			check_date[2] = int(start_date.year)

		# We cannot end in future years from now, since the database records end yesterday:
		if check_date[2] > int(x.year):
			check_date[2] = int(x.year)

		# There are no more months in a year than 12:
		if check_date[1] > 12:
			check_date[1] = 12

		# There is no "zero" or "negative" month:
		if check_date[1] < 1:
			check_date[1] = 1

		# We cannot end in future months from now, since the database records end yesterday:
		if check_date[2] == int(x.year) and check_date[1] > int(x.month):
			check_date[1] = int(x.month)

		# There is no "zero" or "negative" day:
		if check_date[0] < 1:
			check_date[0] = 1

		# There are no more days in a month than 31:
		if check_date[0] > 31:
			check_date[0] = 31

		# In february, there are either 28 or 29 days depending on whether the year is or is not leap:
		if check_date[1] == 2:
			if is_leap_year(check_date[2]) is True:
				if check_date[0] > 29:
					check_date[0] = 29

			else:
				if check_date[0] > 28:
					check_date[0] = 28

		# In April, June, September and November, there are only 30 days in a month:
		if check_date[1] == 4 or check_date[1] == 6 or check_date[1] == 9 or check_date[1] == 11:
			if check_date[0] > 30:
				check_date[0] = 30

		# We cannot end today or in the future, because the last xml DB file exists for yesterday:
		if check_date[2] == int(x.year) and check_date[1] == int(x.month) and check_date[0] > int(x.day):
			check_date[0] = int(x.day)

		end_date = datetime.date(check_date[2], check_date[1], check_date[0])

		print(f"Using ending date: {end_date}")

	# Reverting back to default, if some gibberish was entered:
	else:
		print("Invalid input, using yesterday as default ending date.")


for dt in rrule(DAILY, dtstart=start_date, until=end_date):
	dates.append(dt.strftime("%Y-%m-%d"))

proxyname1, proxyname2 = proxyhandler.query_proxy()

proxy = []

if proxyname1 != "" or proxyname2 != "":
	# create the object, assign it to a variable
	if proxyname1 != "" and proxyname2 != "":
		proxy = urllib.request.ProxyHandler({'http' : proxyname1, 'https': proxyname2})
	elif proxyname1 != "":
		proxy = urllib.request.ProxyHandler({'http': proxyname1})
	elif proxyname2 != "":
		proxy = urllib.request.ProxyHandler({'https': proxyname2})

	# construct a new opener using proxy settings
	opener = urllib.request.build_opener(proxy)

	# install the opener on the module-level
	urllib.request.install_opener(opener)

for date in dates:
	print('Downloading date : '+date, end='')

	# Download
	try:
		urllib.request.urlretrieve('http://www.crz.gov.sk//export/'+date+'.zip', 'CRZ_DB/'+date+'.zip')

		print(' ...OK')

		# Unzip
		zip_ref = zipfile.ZipFile('CRZ_DB/' + date + '.zip', 'r')
		zip_ref.extractall('CRZ_DB/')
		zip_ref.close()

		# Delete
		os.system('rm CRZ_DB/' + date + '.zip')

	except Exception as e:
		print(f'{repr(e)}, skipping...')
		pass