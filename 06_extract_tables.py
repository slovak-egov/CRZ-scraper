# Matej Badin | UHP | 2019                                              |
# Marian Tihanyi | IDRP | 2021											|
# --------------------------------------------------------------------  |
# Packages needed :  numpy, os, pandas, camelot, time, sys	            |
#                    pdfminer.six -- requirement already met			|
#                    for Camelot										|
# --------------------------------------------------------------------  |
# Extract tables from already tagged and filtered relevant contracts    |
# --------------------------------------------------------------------  |

# Several notes regarding the installation of camelot for extraction
# of tables from PDF files:
#
# 1. DO NOT INSTALL package called Camelot (or camelot). This is not
#    the right camelot. If you already have the wrong camelot, please
#	 uninstall it (python3 -m pip uninstall Camelot).
#
# 2. Install the right camelot using the commands below:
#
# 		python3 -m pip install camelot-py --break-system-packages
#		python3 -m pip install camelot-py[cv] --break-system-packages
#		python3 -m pip install ghostscript python3-tk --break-system-packages
#		python3 -m pip install opencv-python-headless
#		python3 -m pip install 'pypdf2<3.0.0' --break-system-packages
#
# 	 The last line is added, because pypdf2 of higher version than
#	 3.0.0 already uses different tool for parsing PDF files. It will
#	 raise an error that <some_PDF_tool> is deprecated and was replaced
#	 by <some_other_PDF_tool>. Hence pypdf2 version must by less than
#	 3.0.0.
#
#	 The only change int he code itself is to replace the import line
#	 with: import camelot.io as camelot (already done).

import os
import numpy as np
import pandas as pd
import camelot.io as camelot			# $ python3.9 -m pip install camelot-py
						# $ python3.9 -m pip install "camelot-py[cv]"
import time
import signal
import sys

# pdfminer for extracting information about number of pages
from pdfminer.pdfparser import PDFParser
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfinterp import resolve1

# Timeout class
class Timeout:

	def __init__(self, seconds=1, error_message='Timeout'):
		self.seconds = seconds
		self.error_message = error_message

	def handle_timeout(self, signum, frame):
		raise TimeoutError(self.error_message)

	def __enter__(self):
		signal.signal(signal.SIGALRM, self.handle_timeout)
		signal.alarm(self.seconds)

	def __exit__(self, type, value, traceback):
		signal.alarm(0)


# Destination:
working_dir = os.getcwd()+'/contracts_relevant/'

# Open tagged spreadsheet and read information, sorted
DB_clean_tagged = pd.read_csv('CRZ_DB_clean_text_tagged.csv', delimiter='|', dtype=str)

# Remove the "Unnamed: 0" cell from the spreadsheet:
try:
	DB_clean_tagged = DB_clean_tagged.drop('Unnamed: 0')

except Exception as e:
	print(e)
	pass

number_of_contracts = len(DB_clean_tagged.index)

# If column with number of PDF pages in contract does not exist,
# then calculate it ...
if 'Pocet_stran' not in DB_clean_tagged.columns:
	DB_clean_tagged.insert(24, 'Pocet_stran', np.zeros(number_of_contracts))

	# Identify how many pages there are in every contract
	# Useful to estimate time needed to extract all tables from PDF
	print('Counting and processing pages in PDFs...')

	total_start_time = time.time()

	for index, row in DB_clean_tagged.iterrows():
		contract_dir = working_dir + str(row['ID']) + '/'
		target_dir = contract_dir + 'tables'

		# PDF extension is not always present, hence instead of including PDF files, we exclude TXT files,
		# as well as eventual 'tables' directory from the content:
		content = [x for x in os.listdir(contract_dir) if x[-4:].casefold() != '.txt' and os.path.isfile(contract_dir + x)]

		if 'Pocet_tabuliek' not in DB_clean_tagged.columns:
			# Insert information about number of tables into the database:
			DB_clean_tagged.insert(25, 'Pocet_tabuliek', np.zeros(number_of_contracts))
			empty_column = [None] * number_of_contracts

			# Insert information about tables pages into the database:
			if 'Tabulky_strany' not in DB_clean_tagged.columns:
				DB_clean_tagged.insert(26, 'Tabulky_strany', empty_column)

		dir_len = 0
		contracts_len = len(str(number_of_contracts))
		ID_len = 0
		fname_len = 0
		content_len = len(str(len(content)))

		pages = 0

		# Loop through all pdf attachments in contract's ID folder:
		for k, f in enumerate(content):
			# We want to print the following for the whole contract only, regardless of number of files:
			if len(contract_dir) > dir_len:
				dir_len = len(contract_dir)

			if len(row['ID']) > ID_len:
				ID_len = len(row['ID'])

			if len(f) > fname_len:
				fname_len = len(f)

			sys.stdout.flush()

			# Visual "envelope" for anything that happens with particular contract:
			print(
				f"ID: {row['ID'].rjust(ID_len)}, {str(index + 1).rjust(contracts_len)} of {str(number_of_contracts).rjust(contracts_len)}, "
				f"file: {f.ljust(fname_len)}, path: {contract_dir.ljust(dir_len + 1)}, {(k + 1):>3} of {str(len(content)).rjust(content_len)}", end=" -> "
			)

			contract_file = open(contract_dir + f, 'rb')

			try:
				parser = PDFParser(contract_file)
				document = PDFDocument(parser)

				p = resolve1(document.catalog['Pages'])['Count']
				pages += p

				print(f" Number of pages: {p}, starting process...")

				if not os.path.exists(target_dir):
					os.makedirs(target_dir)

				number_of_tables = 0
				tables_pages = []

				# We monitor both time for each contract and the overall duration from the start of the process.
				start_time = time.time()
				time_list = []

				for page in range(1, p + 1):
					time_list = []

					hours, reminder = divmod(time.time() - start_time, 3600)
					minutes, seconds = divmod(reminder, 60)

					time_list.append(f"{int(hours)}:{int(minutes):02d}:{int(seconds):02d}")

					hours, reminder = divmod(time.time() - total_start_time, 3600)
					minutes, seconds = divmod(reminder, 60)

					time_list.append(f"{int(hours)}:{int(minutes):02d}:{int(seconds):02d}")

					# There are files, which are essentially PDF files, but they were uploaded
					# to CRZ without any extension. Some PDF processing tools have problem
					# with that, so we make it right:
					# If the file is not a PDF, we have to proceed accordingly.
					# 1. Check if it is not a .docx or a .doc file, which can be read
					# 2. If there is no extension, those are PDFs, but the extension
					#    is missing.
					if f[-4:].casefold() != '.pdf':
						if f[-5:].casefold() == '.docx':
							print(f"{f}: This is a MS Word XML format file. We need to convert it to pdf first...")
							os.system('ebook-convert ' + f + ' ' + f.rstrip(".docx") + '.pdf > /dev/null')
							contract = f.rstrip(".docx") + ".pdf"

						elif f[-4:].casefold() == '.doc':
							print(f"{f}: This is a MS Word binary file. We need to convert it to pdf first...")
							os.system('unoconv ' + f)
							f = f.rstrip(".docx") + ".pdf"

						else:
							print(f"{f}: invalid extension, changing to {f}.pdf...")
							os.system('cp ' + contract_dir + f + ' ' + contract_dir + f + '.pdf')
							f += '.pdf'

					try:
						TimeoutReached = False
						with Timeout(seconds=10):
							tables = camelot.read_pdf(contract_dir + f, pages=str(page), flavor="hybrid", suppress_stdout=True, parallel=True)

					except TimeoutError as e:
						TimeoutReached = True
						pass
						break

					if len(tables) > 0:
						for m in range(0, len(tables)):
							fname = target_dir + '/table_' + f + '_' + str(number_of_tables + m + 1) + '.csv'

							print(
								f"\r\tProcessing page: {page} of {p} ...found tables, report: {tables[m].parsing_report}, "
								f"output file: {'table_' + f + '_' + str(number_of_tables + m + 1) + '.csv'}, "
								f"time elapsed: {time_list[0]}, total from the start: {time_list[1]}", end=""
							)

							tables[m].to_csv(fname, sep='|')
							tables_pages.append(page)

						number_of_tables += len(tables)

					else:
						print(f"\r\tProcessing page: {page} of {p}, time elapsed: {time_list[0]}, total from the start: {time_list[1]}", end="")

					sys.stdout.flush()

				end_time = time.time()

				hours, reminder = divmod(time.time() - start_time, 3600)
				minutes, seconds = divmod(reminder, 60)

				if len(time_list) <= 2:
					time_list.append(f"{int(hours)}:{int(minutes):02d}:{int(seconds):02d}")
				else:
					time_list[2] = f"{int(hours)}:{int(minutes):02d}:{int(seconds):02d}"

				if not TimeoutReached:
					print('\n\tProcessed ', p, ' pages in ', time_list[len(time_list) - 1])
				else:
					print('\n\tUnrecoverable errors encountered while processing contract ID: ', row['ID'].rjust(ID_len),', skipping to the next contract.')

				DB_clean_tagged.at[index, 'Pocet_tabuliek'] = int(number_of_tables)
				DB_clean_tagged.at[index, 'Tabulky_strany'] = str(tables_pages)

				# Save information about number of pages
				DB_clean_tagged.to_csv('CRZ_DB_clean_text_tagged.csv', sep='|')

			except Exception as e:
				print("\n", e, ", skipping...")
				pass

			contract_file.close()

		DB_clean_tagged.at[index, 'Pocet_stran'] = int(pages)

	# Save partial result
	DB_clean_tagged.to_csv('CRZ_DB_clean_text_tagged.csv', sep='|')

else:
	# This script also writes in to the opened source database. It should be pristine in order for the code to work:
	print("The field 'Pocet_stran' was already found in the source database. Please use clean DB for processing.")