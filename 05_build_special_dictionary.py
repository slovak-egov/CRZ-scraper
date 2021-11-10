# Matej Badin    | UHP  | 2019                                         |
# Marian Tihanyi | IDRP | 2021	(update, improvements)                 |
# -------------------------------------------------------------------- |
# Packages needed :  re, os, hunspell, operator                        |
# -------------------------------------------------------------------- |
# Build Slovak dictionary from words in contracts                      |
# which are not part of standard Slovak corpus                         |
# -------------------------------------------------------------------- |

import os
import re

# HunSpell installation in Linux requires:
# As root:			# apt install hunspell hunspell-sk libhunspell-dev
# As regular user: 	$ python3.X -m pip install hunspell (python3.9 used the last time)
import hunspell
import operator

slovak_alphabet = 'aáäbcčdďeéfghiíjklĺľmnňoóôpqrŕsštťuúvwxyýzž'


def parse_text(txt):
	txt = txt.casefold()
	wrds = []

	new_word = ''
	wrd = True
	for char in txt:
		if char in slovak_alphabet:
			new_word = new_word + char
			wrd = True
		else:
			if wrd:
				wrds.append(new_word)
				new_word = ''
			wrd = False

	return wrds


# Import standard Slovak dictionary
normal_SK = os.path.join(os.getcwd(), 'Dicts/sk_SK/')
english_US = os.path.join(os.getcwd(), 'Dicts/en_US/')

hunspell_normal = hunspell.HunSpell(normal_SK + 'sk_SK.dic', normal_SK + 'sk_SK.aff')
hunspell_english = hunspell.HunSpell(english_US + 'en_US.dic', english_US + 'en_US.aff')


def check_normal(wrd):
	return hunspell_normal.spell(wrd) or hunspell_english.spell(wrd)


# Find all text/OCR'd contracts:
find_txt = re.compile('txt')
working_dir = os.getcwd()+'/contracts_text/'
dir_list = os.listdir(working_dir)

print("Loading filenames...")

contracts = {}

# Each contract is saved within its own directory, which name is contract ID. This is changed in comparison
# to the original script from 2019, which changed filenames and put all contracts including their supplements
# into the same folder.
#
# The "contracts" dictionary (python data type, not the result of this script) has the following structure:
#
# { contract_ID_1 : [txt_filename_1a, txt_filename_1b,...], contract_ID_2 : [txt_filename_2a, txt_filename_2b,...] }
#
# The dictionary contains all files with .txt extension within the contract_ID folders.
for i, f in enumerate(dir_list):
	percentage = i/len(dir_list) * 100

	if not os.path.isfile(os.path.join(working_dir, f)):
		contracts[f] = []

		for c in os.listdir(working_dir + f):
			if '.txt' in c.casefold():
				contracts[f].append(c)

	print(f"\r{round(percentage,1)}%", end="")

contracts_txt = {}

# We add only those contracts, which text is actually there - hence the "contracts_txt"
# dictionary differs from "contracts" by those txt files, which are empty (they are not
# included in "contracts_txt"):
for cid, fl in contracts.items():
	contracts_txt[cid] = []

	for f in fl:
		if len(find_txt.findall(f)) > 0:
			contracts_txt[cid].append(f)

print("\nFilenames loading completed.")

N = len(contracts_txt)
N_words = 0
new_words = dict()
i = 0

# Analyse words in every text/OCR'd contract and/or supplement
for contract, filelist in contracts_txt.items():
	print('Analysing contract ID: ', contract, ', ', i + 1, 'out of', str(N) + ':')

	for f in filelist:
		print('...filename:', f, end='')

		fo = open(working_dir + contract + '/' + f, 'r', encoding='utf-8')
		lines = fo.readlines()
		fo.close()

		text = ''

		for line in lines:
			text += line.casefold().replace('\n',' ')

		del lines

		words = parse_text(text)
		for word in words:
			if not check_normal(word):
				if word in new_words:
					new_words[word] +=1
				else:
					new_words[word] = 1

		print(' - number of words:', len(words), '| new words:', len(new_words))
		N_words += len(words)

	i += 1

print('Total number of words: ', N_words)
print('Total new words: ', len(new_words))

sorted_new_words = sorted(new_words.items(), key=operator.itemgetter(1), reverse=True)

# Keep only words if their number of occurrences is at least 5:
sorted_new_words = [word[0] for word in sorted_new_words if word[1] > 5]

print('Filtered new words: ', len(sorted_new_words))

# Finally, write the words database into a file:
target_dir = os.path.join(os.getcwd(), 'Dicts/sk_SK_special_with_keywords')

if not os.path.exists(target_dir):
	os.makedirs(target_dir)

fo = open(target_dir + '/sk_SK_special.dic', 'w', encoding='utf-8')
fo.write(str(len(sorted_new_words))+'\n')

for word in sorted_new_words:
	fo.write(word+'\n')

fo.close()
