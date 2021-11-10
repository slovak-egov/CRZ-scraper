# Matej Badin    | UHP  | 2019                                          |
# Marian Tihanyi | IDRP | 2021 (update, improvements, integration with  |
#                               OCR)                                    |
# Original name of the script: filter_text_contracts.py					|
# --------------------------------------------------------------------  |
# Packages needed :  re, os, ast, pandas, subprocess                    |
# --------------------------------------------------------------------  |
# OS packages needed:													|
# 1. tesseract															|
# 	 Debian flavour: # apt install tesseract-ocr-all					|
# 2. poppler-utils														|
# 	 Debian flavour: # apt install poppler-utils						|
# --------------------------------------------------------------------  |
# Parsing downloaded data from CRZ GOV obtained by download_dump script.|
# Extracting text from PDF files - either directly, if possible, or     |
# using tesseract OCR engine, if the text cannot be extracted directly  |
# (if the PDF is a scanned document).									|
# The script was updated by OCR, so the original OCR.py script became	|
# obsolete.																|
# User is given a choice at the very beginning, whether to:				|
# 1. Process only textual contracts (extractable PDFs) - fast			|
# 2. Process only scanned contracts (OCR) - very slow					|
# 3. Process both - very slow + "fast" textual processing (even slower)	|
# any other key: Quit.													|

import os
import re
import ast
import pandas as pd
import subprocess

# There are duplicates of the same contracts uploaded to the CRZ database.
# That's why a control DB is implemented to prevent duplicate processing
# of the same contracts:
control_db = []
contracts = []
process_level = 0

find_number = re.compile(r'\d+')
FNULL = open(os.devnull, 'w')


# Sorting function for processed files:
def natural_sort(l_text):
	convert = lambda text: int(text) if text.isdigit() else text.lower()
	alphanum_key = lambda key: [convert(c) for c in re.split('([0-9]+)', key)]
	return sorted(l_text, key=alphanum_key)


if not os.path.exists('contracts_text'):
	os.makedirs('contracts_text')

# Source dir:
raw_dir = os.getcwd()+'/contracts/'

# Destination dir:
text_dir = os.getcwd()+'/contracts_text/'

DB_clean = pd.read_csv(os.getcwd() + '/CRZ_DB_clean.csv', delimiter='|', dtype=str)
number_of_contracts = DB_clean.shape[0]

j = 0
k = 0
textual = 0
scanned = 0

# Explanation of the following options presented to the user.
#
# Originally, there were 2 scripts:
#
# 1. filter_text_contracts.py, which extracted text from contracts directly
# if possible ("textual" contracts), while putting those contracts
# in contracts_text folder and the not detectable (scanned) ones into
# the contracts_scan folder (without extracting any text).
#
# 2. OCR.py, which took the contracts from the contracts_scan folder, OCR'd
# their contents and put the results in contracts_text folder as well.
#
# After optimisation, these two scripts have been merged together, because
# the intermediary step - copying contracts into the contracts_scan folder,
# was redundant (and once not required, there was also no need for two
# separate scripts).
#
# The automated decision-making process was simplified in the following fashion:
#
# - Does the source contract file contain computer-readable text?
#    -> Yes: Extract text and save both PDF and TXT in contracts_text folder.
#    -> No: OCR the scanned PDF and save both PDF and TXT
#           in the contracts_text folder (further processing does not care
#           about whether the original was textual or scanned).
#
# However, the OCR process runs multiple-fold times longer than direct
# extraction of text and by running just filter_text_contracts before, the user
# was free of waiting for the OCR process to finish in exchange to be able
# to further process textual contracts only (this is especially useful in
# "needs to be done fast" scenarios).
# Similarly, it was possible to process only OCR when required
# (e.g. afterwards or even in parallel, finishing later).
#
# In order not to lose this flexibility after the merge, the following
# options were added to the beginning of the script. The options
# are self-explanatory.
print("Please select one of the following options:")
print("\t[1] (fast) Process only textual contracts (skip scanned documents)")
print("\t[2] (slow) Process only scanned documents using OCR (skip textual documents)")
print("\t[3] (1+2)  Process both textual and scanned contracts")
print("\t[Any other key, e.g. 'Q'] Quit the process now")
answer = input("Your choice: ")

if answer == '1' or answer == '2' or answer == '3':
	process_level = int(answer)

if process_level > 0:
	# Read IDs of contracts, excluding duplicates and contracts not having any attachment files:
	for i in range(0, number_of_contracts):
		attachments = ast.literal_eval(DB_clean.iloc[i, 20])

		contract_name = str(DB_clean.iloc[i, 1])
		contract_display_name = contract_name.strip()[:50] + "..." if len(contract_name) > 50 else contract_name
		contract_ID = str(DB_clean.iloc[i, 2])

		percentage = round(i/number_of_contracts*100, 1)

		# We need to have some attachments to proceed:
		if attachments:
			# The contract must not be a duplicate:
			if [contract_name, contract_ID] not in control_db:
				print(f"[{percentage:>5}%][t: {textual:>5}/s: {scanned:>5}] Processing contract ID: {contract_ID}, name: {contract_display_name}")

				# Update control DB for future checks for duplicates:
				control_db.append([])  # Add a new sublist into the control list
				control_db[j].append(contract_name)  # Add contract name to the sublist
				control_db[j].append(contract_ID)    # Add ID to the sublist

				dirpath = os.listdir(raw_dir)		 # List all contract dirs, which are IDs

				# If a contract with such ID is downloaded to the disk, we enter
				# the corresponding directory / folder:
				if control_db[j][1] in dirpath:
					# Save the complete path to the contract folder:
					contract_dir = raw_dir + control_db[j][1]

					# If the folder contains anything, we can parse it:
					if os.listdir(contract_dir):
						listed_dir = os.listdir(contract_dir)
						listed_dir_lower_case = [item.casefold() for item in listed_dir]

						# Clean up any mess from former processing - delete files with any other
						# extension than pdf. Also keep files without extension and dirs. ,
						# if there are pdf files created from extension-less pdfs with the same name,
						# we should delete them despite the fact the processing changing extensions
						# does not make copies (but user might).
						for fl in listed_dir:
							if os.path.exists(contract_dir + '/' + fl) and (fl[-4:].casefold() == '.txt' or fl[-4:].casefold() == '.png'):
								try:
									os.system("rm " + contract_dir + '/' + fl)

								except Exception as e:
									print(repr(e))
									pass

							elif os.path.exists(contract_dir + '/' + fl) and fl[-4] != '.' and fl[-3:].casefold() != 'pdf':
								if fl.casefold() + '.pdf' in listed_dir_lower_case:
									try:
										f_name = listed_dir[listed_dir_lower_case.index(fl.casefold() + '.pdf')]
										os.system('rm ' + contract_dir + '/' + f_name)

									except Exception as e:
										print(repr(e))
										pass

						# In case any files were deleted, we need to reload the source dir and the total number
						# of files in the folder:
						listed_dir = os.listdir(contract_dir)
						total_files = len(listed_dir)
						fcount = 0

						# The following loop goes through files within the contract_ID (listed_dir) folder, doing
						# the following set of actions:
						# 1. If there are files lacking PDF extension, they are "corrected". Extensive testing
						# 	 proved that absolute majority (e.g.99.9%) of files are PDF files regardless of
						# 	 whether they have extension or not. Other file types usually have their extension.
						# 3. PDFs are opened, trying to extract text directly.
						# 4. If the text is extractable, the file is treated as textual.
						# 	 If not, the file is treated as a scanned document.
						# 5. Depending on user choice made at the very beginning of the script,
						# 	 the files are appropriately handled with (either skipped or processed).
						# 6. If particular option is enabled, the text is either directly extracted from the PDF
						# 	 and saved as TXT file, or the PDF is OCR'd and that text is saved as TXT file.
						while fcount < total_files:
							if os.path.isfile(contract_dir + '/' + listed_dir[fcount]):
								f = listed_dir[fcount]

								print(f"\tProcessing file {fcount + 1} of {total_files}: {f}")

								# There are files, which are essentially PDF files, but were uploaded
								# to CRZ without any extension. Some PDF processing tools have problem
								# with that, so we "correct" them by adding PDF extension:
								if f[-4:].casefold() != '.pdf':
									if f[-4] != '.':
										print(f"\t - {f}: invalid extension, changing to {f}.pdf...")

										os.system('mv ' + contract_dir + '/' + f + ' ' + contract_dir + '/' + f + '.pdf')
										f += '.pdf'

								contracts.append(contract_dir + '/' + f)
								contract = contracts[k]

								operstrlist = contract.split("/")

								# Particular contracts are saved in folders equalling their ID in the contracts_text directory.
								target_id_dir = operstrlist[len(operstrlist) - 2]

								# Clean name of the contract, without path:
								bare_name = operstrlist[len(operstrlist) - 1]

								print("\t - Analysing document...")

								# Take any of the PDF files in the folder and try to convert it to text
								# not using OCR (but just extracting text from the corresponding fields
								# of the PDF format).
								# The pdftotext and pdftoppm commands belong to the poppler-utils package, documentation:
								# https://www.mankier.com/package/poppler-utils
								# Installation in Debian linux: # apt install poppler-utils
								os.system('pdftotext -q ' + contract + ' output.txt')

								# File containing detected text:
								fo = open('output.txt', 'r', encoding='utf8')
								lines = fo.readlines()
								fo.close()

								file = ''
								for line in lines:
									file += line

								del lines

								check_file = re.sub(r'\s+', '', file, flags=re.UNICODE)

								# If the extracted text is longer than 0 characters, the PDF is "textual":
								if len(check_file) > 0:
									if process_level == 1 or process_level == 3:
										if not os.path.exists(text_dir + target_id_dir):
											os.system("mkdir " + text_dir + target_id_dir)

										# Such a file is copied into the contracts_text folder.
										# Here the original "move" command was replaced with "copy", which does not
										# destroy original file source. Any possible debug was difficult after
										# the files have been moved.
										print("\t - This is a textual document. Direct extraction of text successful.")
										print(f"\t - Saving source document into the target folder as {text_dir}{target_id_dir}/{bare_name}")
										print(f"\t - Saving generated text document as {text_dir}{target_id_dir}/{bare_name}.txt")
										os.system('cp ' + contract + ' ' + text_dir + target_id_dir + '/' + bare_name)
										os.system('mv output.txt ' + text_dir + target_id_dir + '/' + bare_name + '.txt')

										# This counts textual contracts:
										textual += 1
									else:
										print("\t - This is a textual document. Skipping on user request.")

								# If the extracted text has zero length, the file is a scanned document.
								# PNG pictures are created from particular PDF pages and OCR processing is applied..
								else:
									if process_level == 2 or process_level == 3:
										print("\t - This is a scanned document, OCR processing required.")

										# If the PDF cannot be processed as textual, it's time to OCR its contents.
										# First, we convert it to PNG images (count matching total number of pages).
										# (Unfortunately, there is no way of monitoring the progress).
										print("\t\t 1. Creating PNG images from particular pages, please wait...")
										retcode = subprocess.call('pdftoppm -q -png -r 350 -gray ' + os.path.join(contract) + ' ' + os.path.join(contract_dir, 'output'), stdout=FNULL, stderr=subprocess.STDOUT, shell=True)

										print("\t\t    ...finished creating images.")

										output_files = []

										for f in os.listdir(contract_dir):
											if os.path.isfile(os.path.join(contract_dir, f)) and 'output' in f and '.txt' not in f:
												output_files.append(f)

										output_files = natural_sort(output_files)

										print("\t\t 2. Performing OCR...")

										# Perform OCR on generated images, using Tesseract, documentation: https://man.cx/tesseract(1)
										# Installation in Debian linux: # apt install tesseract-ocr-all
										text = ''

										for p, page in enumerate(output_files):
											print('\t\t    ...on page:', p + 1, 'of', len(output_files))
											retcode = subprocess.call('tesseract ' + os.path.join(contract_dir, page) + ' ' + os.path.join(contract_dir, 'output') + ' -l eng+ces+slk', stdout=FNULL, stderr=subprocess.STDOUT, shell=True)
											fo = open(os.path.join(contract_dir, 'output.txt'), 'r', encoding='utf-8')
											lines = fo.readlines()
											fo.close()

											for line in lines:
												text += line

										path_exists = False

										# Target directory in contracts_text folder might or might not exist,
										# depending whether there already was or was not any textual document
										# associated with the particular contract ID.
										if not os.path.exists(text_dir + target_id_dir):
											os.makedirs(text_dir + target_id_dir)
										else:
											path_exists = True

										fname = ""

										# If there are any txt files from former processing, we delete them first.
										# We take any name we find, add txt extension to it, because that is the
										# method of creating txt files from pdf files (only adding extension).
										if '.txt' not in bare_name.casefold():
											fname = bare_name + '.txt'
										else:
											fname = bare_name

										if path_exists is True:
											t = 0

											# If the guessed txt file exists, we should delete it before creating new one:
											if os.path.exists(text_dir + target_id_dir + '/' + fname):
												os.system('rm ' + text_dir + target_id_dir + '/' + fname)

											# Furthermore, if there were multiple text files, which are created with an underscore and a number,
											# they should be deleted too.
											while os.path.exists(text_dir + target_id_dir + '/' + fname.strip('.txt') + '_' + str(t) + '.txt') and t < 10000:
												os.system('rm ' + text_dir + target_id_dir + '/' + fname.strip('.txt') + '_' + str(t) + '.txt')
												t += 1

										print("\t - OCR process successful.")
										print(f"\t - Saving source document into the target folder as {text_dir}{target_id_dir}/{bare_name}")
										print(f"\t - Saving generated text document as {text_dir}{target_id_dir}/{fname}")

										# PDF file:
										os.system('cp ' + contract + ' ' + text_dir + target_id_dir + '/' + bare_name)

										# New text file to hold the OCR'd text:
										fo = open(os.path.join(text_dir, target_id_dir, fname), 'w', encoding='utf-8')
										fo.write(text)
										fo.close()

										# This counts scanned contracts:
										scanned += 1

										# Delete temporary files
										try:
											os.system('rm ' + os.path.join(contract_dir, 'output*'))

										except:
											fcount += 1
											k += 1
											pass
									else:
										print("\t - This is a scanned document. Skipping on user request.")

								k += 1

							# fcount works with all items in a directory, e.g. dirs included, hence is outside "if os.path.isfile():"
							fcount += 1
					else:
						print(f"[{percentage:>5}%][t: {textual:>5}/s: {scanned:>5}] Skipping nonexistent contract, no. {i}, ID: {contract_ID}, name: {contract_display_name}")
					j += 1
				else:
					print("\tNot found.")
			else:
				print(f"[{percentage:>5}%][t: {textual:>5}/s: {scanned:>5}] Skipping duplicate contract, no. {i}, ID: {contract_ID}, name: {contract_display_name}")
		else:
			print(f"[{percentage:>5}%][t: {textual:>5}/s: {scanned:>5}] Skipping contract with no attachments, no. {i}, ID: {contract_ID}, name: {contract_display_name}")

else:
	print("Quitting...")