# Matej Badin | UHP | 2019                                             |
# Marian Tihanyi | IDRP | 2021 - added proxy support and user prompt   |
#                                to enter the date of the last record, |
#                                as well as specify starting date.     |
# -------------------------------------------------------------------- |
# Packages needed :  urllib, zipfile, dateutil, datetime, os,          |
#                    proxyhandler (own module enabling proxy support)  |
# -------------------------------------------------------------------- |
# Crawler for downloading and categorizing dump from CRZ GOV database  |
# Downloads and categorizes every contract from now to given date      |
# -------------------------------------------------------------------- |

import urllib.request
import zipfile
import os
import proxyhandler
import datetime
# from datetime import date
from dateutil.rrule import rrule, DAILY

start_date = datetime.date(2011, 1, 1)

dates = []

# Check today's date for reference and calculate yesterday's date (last valid date for database xml file):
yeseterday_date = datetime.datetime.now() - datetime.timedelta(days=1)
yeseterday_date = yeseterday_date.date()

# Default ending date is yesterday:
end_date = yeseterday_date

def validateDate(date_str : str ,default : datetime.date):
	try:
		# Try to convert date to datetime format
		check_date=datetime.datetime.strptime(date_str,"%d.%m.%Y").date()
	except:
		# Reverting back to default, if some gibberish was entered:
		print("Invalid input, using default date")
		check_date= default
	
	# We need to start in 2011 or later:
	if check_date < start_date:
		check_date=start_date
	
	# We cannot start in future:
	if check_date > yeseterday_date:
		check_date=yeseterday_date
	
	return check_date



print("*** Downloading DB of contracts from crz.gov.sk. ***")
print('')

# Gather and check starting date:
input_date = input("Please enter starting date in the form 'd.m.yyyy', 'b' for beginning of DB records, or 'y' for yesterday: ")

if input_date.lower() not in ['y','b']:
	start_date=validateDate(input_date,start_date)

	print(f"Using starting date: {start_date}")


# We decided to start at the very beginning of record keeping:
elif input_date.lower() == 'b':
	print("Using the beginning of record keeping as the starting date: the 1st of January, 2011.")

# We decided to start yesterday:
else:
	start_date = yeseterday_date

# Gather and check ending date:
input_date = input("Please enter ending date in the form 'd.m.yyyy' or 'y' for yesterday: ")

if input_date.lower() != 'y':
	end_date=validateDate(input_date,yeseterday_date)

	print(f"Using ending date: {end_date}")


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
		print('Downloading date : '+date)

		# Download
		try:
			urllib.request.urlretrieve('http://www.crz.gov.sk//export/'+date+'.zip', 'CRZ_DB/'+date+'.zip')

			# Unzip
			zip_ref = zipfile.ZipFile('CRZ_DB/' + date + '.zip', 'r')
			zip_ref.extractall('CRZ_DB/')
			zip_ref.close()

			# Delete
			os.system('rm CRZ_DB/' + date + '.zip')

		except Exception as e:
			print(f'{repr(e)}, skipping...')
			pass
