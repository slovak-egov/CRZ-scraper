# Matej Badin | UHP | 2019                                             |
# Marian Tihanyi | IDRP | 2021										   |
# -------------------------------------------------------------------- |
# Packages needed :  numpy, re, os, pandas, operator, shutil, hunspell |
# -------------------------------------------------------------------- |
# Clean up extracted tables based on keywords and further criteria     |
# -------------------------------------------------------------------- |

import os
import re
import pandas as pd
import operator
import shutil

# Import standard Slovak vocabulary corpus and dictionary
import hunspell


# Prepare a list of words for checking:
def parse_text(txt):
	slovak_alphabet = 'aáäbcčdďeéfghiíjklĺľmnňoóôpqrŕsštťuúvwxyýzž'

	txt = txt.casefold()
	wrds = []

	new_word = ''
	wrd = True
	for character in text:
		if character in slovak_alphabet:
			new_word = new_word + character
			wrd = True
		else:
			if wrd:
				wrds.append(new_word)
				new_word = ''
			wrd = False

	return wrds


# Files
working_dir = os.getcwd()

# Source:
tables_dir = working_dir+'/contracts_mandayrates_tables'
# Destination:
clean_tables_dir = working_dir+'/contracts_mandayrates_clean_tables'

if os.path.isdir(clean_tables_dir):
	shutil.rmtree(clean_tables_dir)
else:
	os.mkdir(clean_tables_dir)

tables_csv = {}
N_tables = 0

# tables_csv is a dictionary having the following structure:
# {key1 : [list_of_files1], key2 : [list_of_files2], key3 : [list_of_files3]...}
# Key is a complete path to the particular contract ID folder,
# not containing any target file name, e.g.: working_dir/contracts_mandays_tables/2796717
for directory in sorted(os.listdir(tables_dir)):
	if not os.path.isfile(os.path.join(tables_dir, directory)):
		current_dir = tables_dir + '/' + directory
	else:
		current_dir = ''

	if not current_dir == '':
		tables_csv[current_dir] = []

		for f in os.listdir(current_dir):
			if os.path.isfile(os.path.join(current_dir, f)) and f.casefold() != "temp.csv":
				tables_csv[current_dir].append(f)
				N_tables += 1

			elif f.casefold() == "temp.csv":
				os.system("rm " + os.path.join(current_dir, f))

normal_SK = os.path.join(working_dir, 'Dicts/sk_SK')
english_US = os.path.join(working_dir, 'Dicts/en_US')
special_SK = os.path.join(working_dir, 'Dicts/sk_SK_special_with_keywords')

# Dictionary with standard Slovak language and words from contracts in this sector by build_special_dictionary.py
hunspell_normal = hunspell.HunSpell(normal_SK + '/sk_SK.dic', normal_SK + '/sk_SK.aff')
hunspell_english = hunspell.HunSpell(english_US + '/en_US.dic', english_US + '/en_US.aff')
hunspell_special = hunspell.HunSpell(normal_SK + '/sk_SK.dic', special_SK + '/sk_SK_special.dic')


# Own spellcheck function also making sure word is case-folded and whitespace is stripped
def spell(word):
	word = word.casefold().strip()
	return (hunspell_normal.spell(word.encode('utf-8')) or hunspell_english.spell(word.encode('utf-8')) or hunspell_special.spell(word.encode('utf-8')))


# Import keywords and add them to the special dictionary for spell checking
fo = open('keywords.txt', 'r', encoding='utf-8')

lines = fo.readlines()
fo.close()

all_keywords = []

categories = []
add_words = []

# Import keywords from keywords.txt and put them inside special dictionary
for line in lines:
	line = line.split(',')

	category_name = line[0]
	keywords = []

	for i, item in enumerate(line):
		if i > 0:
			keywords.append(item.strip().casefold())
			all_keywords.append(item.strip().casefold())

	categories.append([category_name, keywords])

# Add keywords to special if they are wrong
for keyword in all_keywords:
	words = keyword.split()

	for word in words:
		if not spell(word):
			add_words.append(word)

# Copy special dictionary and append lines with keywords, reload hunspell_special
special_dic_with_keywords = os.path.join(working_dir, 'Dicts/sk_SK_special_with_keywords')
# Copy file
# Change number in first line
# Append lines to file

# Reload special Hunspell dictionary
hunspell_special = hunspell.HunSpell(normal_SK, special_dic_with_keywords + '/sk_SK_special.dic')

# Empty dictionary to be filled with suggested keywords
suggested_keywords = dict()

#####################################################################################################
# Analysis starts here
#####################################################################################################

i = 0

for table_path, table_csv in tables_csv.items():
	for table_file in table_csv:
		p = table_path.split('/')
		print('Processing table:', p[-1] + '/' + table_file, ' ', i + 1, 'out of', N_tables)

		# Step 1
		# Read CSV and destroy any new line characters between " characters
		print(table_path + '/' + table_file)

		fo = open(os.path.join(table_path + '/' + table_file), 'r', encoding='utf-8')
		lines = fo.readlines()
		fo.close()

		remainder = 0

		text = ''
		for line in lines:
			for char in line:
				if char == '"':
					remainder += 1
				remainder = remainder % 2

				if char == '\n' and remainder == 1:
					pass
				else:
					text += char

		# Step 1a
		# Create temporary file to hold various stages of processed table:
		fo = open(os.path.join(table_path + '/temp.csv'), 'w', encoding='utf-8')
		fo.writelines(text)
		fo.close()

		# Step 2
		# Import CSV table into pandas and delete empty columns:
		table = pd.read_csv(os.path.join(table_path + '/temp.csv'), delimiter='|')
		empty = dict((column, True) for column in table.columns)

		for column in table.columns:
			for index, row in table.iterrows():
				if not((str(row[column]).rstrip() == '') or (str(row[column]) == 'nan')):
					empty[column] = False
					break

		delete = [column for column in table.columns if empty[column]]
		table = table.drop(columns=delete, axis=1)

		# Step 3
		# Try to identify columns with just dummy characters and not any meaningful word
		# and ... also destroy them
		dummy = dict((column, False) for column in table.columns)
		for column in table.columns:

			correct = 0
			wrong = 0

			for index, row in table.iterrows():
				words = str(row[column]).casefold().split()
				for word in words:
					if spell(word):
						correct += 1
					else:
						wrong += 1

			# Arbitrarily chosen number - lower threshold value will result in cleaner tables,
			# but there might be less of them due to harsher filter.
			if wrong / (wrong + correct) > 0.75:
				dummy[column] = True

		delete = [column for column in table.columns if dummy[column]]
		table = table.drop(columns=delete, axis=1)

		# Save clean table (at least cleaned so far):
		table.to_csv(os.path.join(table_path + '/temp.csv'), sep='|')

		# Step 4
		# Identify if the first row is the header
		header = False

		# Select keywords in categories 'Hlavička tabuľky'
		selected_keywords = []
		selected_categories = ['Hlavička tabuľky']

		# but still code in general ;)
		for category in categories:
			if category[0] in selected_categories:
				for keyword in category[1]:
					selected_keywords.append(keyword)

		# Pandas already tried to infer header from CSV - such nice of it ...
		number_of_hits = 0
		for keyword in selected_keywords:
			for column in table.columns:
				number_of_hits += column.count(keyword)

		# Arbitrarily chosen boundary
		if number_of_hits > 2:
			header = True

		# Step 5
		# Identify if there is a specific column with 'Pozicia'
		selected_keywords = []
		selected_categories = ['Pozícia', 'Popis práce']
		number_of_hits = dict((column, 0) for column in table.columns)

		for category in categories:
			if category[0] in selected_categories:
				for keyword in category[1]:
					selected_keywords.append(keyword)

		for column in table.columns:
			for row in table[column]:
				row = str(row).casefold()
				for keyword in selected_keywords:
					if keyword in row:
						number_of_hits[column] += 1

		# Sorted columns with 'Pozicia'-like keywords if number of hits is at least > 1
		positions_columns = [(column, number_of_hits[column]/table.shape[0]) for column in table.columns if number_of_hits[column] > 0]
		positions_columns = sorted(positions_columns, key=lambda tup: tup[1], reverse=True)

		# Step 6
		# .. also try to identify columns which have significant number of rows containing numbers or prices
		find_number = re.compile(r'\d+')
		price_header = ['']

		selected_keywords = []
		selected_categories = ['Hlavička cena']

		for category in categories:
			if category[0] in selected_categories:
				for keyword in category[1]:
					selected_keywords.append(keyword)

		prices_columns = []

		if header:
			for column in table.columns:
				for keyword in selected_keywords:
					if keyword in column:
						if column not in prices_columns:
							prices_columns.append(column)

		ratio_of_number_rows = dict((column, 0) for column in table.columns)

		for column in table.columns:
			for row in table[column]:
				row = str(row)
				if len(find_number.findall(row)) > 0:
					ratio_of_number_rows[column] += 1

			ratio_of_number_rows[column] = ratio_of_number_rows[column]/table.shape[0]

		if len(prices_columns) > 0:
			prices_columns = [(column, ratio_of_number_rows[column]) for column in prices_columns if ratio_of_number_rows[column] > 0.75]
		else:
			prices_columns = [(column, ratio_of_number_rows[column]) for column in table.columns if ratio_of_number_rows[column] > 0.75]

		positions_columns_names = [column[0] for column in positions_columns]

		for column in prices_columns:
			if column[0] in positions_columns_names:
				prices_columns.remove(column)

		prices_columns = sorted(prices_columns, key=lambda tup: tup[1], reverse=True)

		# Suggest new keywords based on data in the rows identified as this
		#  new keywords are given points according to the relative number of rows which already contain
		#  some selected keyword.
		if len(positions_columns) > 0 and len(prices_columns) > 0:
			for column in positions_columns:
				for row in table[column[0]]:
					row = str(row).casefold()
					words = parse_text(row)
					for word in words:
						if word not in selected_keywords:
							if word in suggested_keywords:
								suggested_keywords[word] += column[1]
							else:
								suggested_keywords[word] = column[1]

		# Save to clean directory only if there is at least a single price column
		if len(prices_columns) > 0:
			# Get last folder name of a table:
			c_table_dir = table_path.split('/')[-1]

			if not os.path.exists(clean_tables_dir + '/' + c_table_dir):
				os.makedirs(clean_tables_dir + '/' + c_table_dir)

			table.to_csv(os.path.join(clean_tables_dir + '/' + c_table_dir + '/' + table_file), sep='|')

		# Step 7
		# Identify in which column there is VAT or whether there is no such column
		with_VAT = False
		without_VAT = False

		for column in table.columns:
			if 's DPH' in column: with_VAT = True
			if 'bez DPH' in column: without_VAT = True

		# Print metadata
		print('Header:', header)
		print('Positions:', positions_columns)
		print('Prices:', prices_columns)
		print('s DPH:', with_VAT)
		print('bez DPH:', without_VAT)

		# Delete temporary file:
		os.system("rm " + os.path.join(table_path + '/temp.csv'))

		i += 1

# Print suggested keywords
print('Creating list of sugested keywords... ', end="")
suggested_keywords = sorted(suggested_keywords.items(), key=operator.itemgetter(1), reverse=True)

fo = open('suggested_keywords.txt','w')
for keyword in suggested_keywords:
	fo.write(keyword[0]+'\t\t\t'+str(keyword[1])+'\n')
fo.close()

print("Done.")
