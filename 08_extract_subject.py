import os
import re
import pandas as pd
import ast
import shutil

find_txt = re.compile('txt')
working_dir = os.getcwd()+'/contracts_text/'
checkpoint = False
strip = 0

# Import metadata from text_tagged file
DB = pd.read_csv('CRZ_DB_clean_text_tagged.csv', delimiter='|')
header_subject = ['Predmet zmluvy']
subject_data = []
count = DB.shape[0]

for index, row in DB.iterrows():
    ID = str(DB.iloc[index, 3])
    contract_name = str(DB.iloc[index, 2])

    subject_data.append('Nenájdené')

    if len(contract_name) >= 80:
        contract_name = contract_name[0:76] + "..."

    print(f'Processing contract {index + 1} of {count}, ID: {ID}, title: {contract_name}, [{round(index/(count - 1)*100, 1):>5}]% completed.')

    try:
        if os.listdir(working_dir + '/' + ID):
            listed_dir = os.listdir(working_dir + '/' + ID)

            for i, f in enumerate(listed_dir):
                checkpoint = False

                if f[-4:] == '.txt' and os.path.isfile(working_dir + '/' + ID + '/' + f):
                    print(f'Loading file {f} and searching ', end='')
                    fo = open(working_dir + '/' + ID + '/' + f, 'r', encoding='utf-8')
                    result_str = fo.read().replace('\n',' ')
                    fo.close()

                    position = result_str.casefold().find("predmet zmluvy")

                    if position < 0:
                        position = result_str.casefold().find("úvodné ustanovenia")

                        if position >= 0:
                            position = position + len("úvodné ustanovenia ")
                    else:
                        position = position + len("predmet zmluvy ")

                    if position >= 0:
                        if len(result_str) > position + 601:
                            subject_data[index] = result_str[position:position + 600]
                        else:
                            subject_data[index] = result_str[position:]

                        print("... SUCCESS.")
                    else:
                        print("... SEARCH STRING NOT FOUND.")

                    checkpoint = True

    except Exception as e:
        if checkpoint is False:
            subject_data.append('')

        print(f"Error while processing - {e}, skipping...")
        pass

print("Inserting acquired data into the database...")
DB.insert(4, 'Predmet zmluvy', subject_data)

print("Saving the resulting DB file CRZ_DB_final.csv...")
DB.to_csv('CRZ_DB_final.csv', sep='|')

print("Done.")



