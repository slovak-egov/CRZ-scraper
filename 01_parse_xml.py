# Matej Badin | UHP | 2019                                              |
# Marian Tihanyi | IDRP | 2021											|
# --------------------------------------------------------------------  |
# Packages needed :  numpy, xml.etree.ElementTree, os                   |
# --------------------------------------------------------------------  |
# Parsing downloaded data from CRZ GOV obtained by download_dump script |

import os
import xml.etree.cElementTree as ET
from lxml import etree
import numpy as np
import pandas as pd

et_parser = etree.XMLParser(encoding='utf-8', recover=True)

working_dir = os.getcwd() + '/CRZ_DB/'

if not os.path.exists(os.getcwd() + '/Corrupted_XML_files'):
    os.system('mkdir ' + os.getcwd() + '/Corrupted_XML_files')

corrupted_dir = os.getcwd()+'/Corrupted_XML_files/'

# Folders might sometimes appear in the CRZ_DB directory, so filter for files is adequate:
files = [f for f in sorted(os.listdir(working_dir)) if os.path.isfile(os.path.join(working_dir, f))]

table = []
result_list = []

def operation(node):
    # """Just a sample function that prints the tag of a node."""
    result_list.append([node.tag, node.text])


def recur_node(node, f):
    # """Applies function f on given node and goes down recursively to its
    #   children.

    #   Keyword arguments:
    #   node - the root node
    #   f - function to be applied on node and its children

    # """
    if node is not None:
        f(node)
        for item in list(node):
            recur_node(item, f)
    else:
        return 0


def find_in_list_of_list(mylist, char):
    for sub_list in mylist:

        found = False
        if char in sub_list:
            found = True
            return mylist.index(sub_list)

    if found is False:
        return -1
    # raise ValueError("'{char}' is not in list".format(char = char))

start_index = 0


total_index = 0
att_proceed = False

for fl in files:
    try:
        file = ET.parse(working_dir+fl, parser=et_parser)
        contracts = file.getroot()

        print(f'Parsing file ... {fl}, number of contracts: {len(list(contracts))}')

        if len(list(contracts)) > 0:
            for i, contract in enumerate(contracts):
                result_list = []
                recur_node(contract, operation)

                contract_attachments = []
                contract_supplements = ""

                if result_list is not None:
                    # The contract_name field often contained new lines including
                    # leading and trailing spaces, which is definitely not what we want:
                    contract_name = str(result_list[5][1]).strip().replace('\n', ' ')
                    contract_ID   = result_list[2][1]

                    contract_inner_ID = result_list[1][1]

                    # The same situation as above:
                    contract_purchaser = str(result_list[3][1]).strip().replace('\n', ' ')

                    # The same situation as above:
                    contract_purchaser_address = str(result_list[22][1]).strip().replace('\n', ' ')
                    contract_purchaser_ICO = result_list[21][1]

                    # The same situation as above: (2x)
                    contract_supplier = str(result_list[4][1]).strip().replace('\n', ' ')
                    contract_supplier_address = str(result_list[20][1]).strip().replace('\n', ' ')
                    contract_supplier_ICO = result_list[14][1]

                    contract_date_publication = result_list[13][1]
                    contract_date_signed = result_list[25][1]
                    contract_date_validity = result_list[7][1]
                    contract_date_efficiency = result_list[6][1]
                    contract_date_last_change = result_list[17][1]

                    contract_price_final = result_list[9][1]
                    contract_price_signed = result_list[8][1]

                    contract_resort = result_list[12][1]

                    contract_type = result_list[24][1]
                    contract_state = result_list[15][1]

                    att_proceed = False
                    if find_in_list_of_list(result_list, 'priloha') > -1:
                        att_proceed = True

                    # Work with attachment sublist, if there are any attachments:
                    if att_proceed is True:
                        start_index = find_in_list_of_list(result_list, 'priloha') + 1
                        # print(start_index)
                        attachments = result_list[start_index:]

                        if not attachments is None:
                            print(attachments)

                            # Primary attachment
                            if len(attachments) >= 3:
                                if not attachments[0][1] is None:
                                    contract_attachments.append(attachments[0][1])	# ID of first attachment

                                if not attachments[1][1] is None:
                                    contract_attachments.append(attachments[1][1])  # name of first attachment
                                else:
                                    contract_attachments.append('')

                                if len(attachments) >= 6:
                                    if not attachments[4][1] is None:
                                        contract_attachments.append(attachments[4][1])  # Filename of additional attachment

                                        if not attachments[5][1] is None:
                                            contract_attachments.append(int(attachments[5][1]))  # Size of additional attachment
                                        else:
                                            contract_attachments.append(0)

                                        # This link was modified to match the actual semantics in 2021:
                                        contract_attachments.append("https://www.crz.gov.sk/data/att/" + attachments[4][1]) # Link to additional attachment

                                    if not attachments[6][1] is None:
                                        contract_attachments.append(attachments[6][1])  # Date of attachment

                                if not attachments[2][1] is None and not attachments[3][1] is None:
                                    contract_attachments.append(attachments[2][1])  # Filename of base attachment
                                    contract_attachments.append(int(attachments[3][1]))  # Size of base attachment

                                    # This link was modified to match the actual semantics in 2021:
                                    contract_attachments.append("https://www.crz.gov.sk/data/att/" + attachments[2][1])  # Link to base attachment

                            # Secondary attachment:
                            if len(attachments) >= 11:
                                if not attachments[8][1] is None:
                                    contract_attachments.append(attachments[8][1])  # ID of second attachment

                                if not attachments[9][1] is None:
                                    contract_attachments.append(attachments[9][1])  # Name of second attachment
                                else:
                                    contract_attachments.append('')

                                if len(attachments) >= 14:
                                    if not attachments[12][1] is None:
                                        contract_attachments.append(attachments[12][1])  # Filename of additional attachment
                                        contract_attachments.append(int(attachments[13][1]))  # Size of additional attachment

                                        # This link was modified to match the actual semantics in 2021:
                                        contract_attachments.append("https://www.crz.gov.sk/data/att/" + attachments[12][1])  # Link to additional attachment

                                    if not attachments[14][1] is None:
                                        contract_attachments.append(attachments[14][1])  # Date of attachment

                                if not attachments[10][1] is None and not attachments[11][1] is None:
                                    contract_attachments.append(attachments[10][1])			# Filename of base attachment
                                    contract_attachments.append(int(attachments[11][1]))	# Size of base attachment

                                    # This link was modified to match the actual semantics in 2021:
                                    contract_attachments.append("https://www.crz.gov.sk/data/att/" + attachments[10][1])  # Link to base attachment

                    table.append([total_index,contract_name, contract_ID, contract_inner_ID, contract_purchaser_ICO, contract_purchaser, contract_purchaser_address,
                                contract_supplier_ICO, contract_supplier, contract_supplier_address, contract_date_publication, contract_date_signed, contract_date_validity, contract_date_efficiency,
                                contract_date_last_change, contract_price_final, contract_price_signed, contract_resort, contract_type, contract_state, contract_attachments, contract_supplements])

                    total_index = total_index + 1

    except Exception as e:
        print(f'Error parsing {fl}, {repr(e)}')
        input()
        os.system('cp '+working_dir+fl+' '+corrupted_dir+fl)

header = ['Index','Nazov','ID','Inner-ID','Objednavatel_ICO','Objednavatel','Objednavatel_adresa','Dodavatel_ICO','Dodavatel','Dodavatel_adresa',
            'Datum_zverejnenia','Datum_podpisu','Datum_platnosti','Datum_ucinnosti','Posledna_zmena','Cena_konecna','Cena_podpisana','Rezort','Typ','Stav','Prilohy','Dodatky']

table = np.asarray(table, dtype='object')
#
# # Pandas export better to UTF-8 CSV than raw NumPy
pd.DataFrame(table).to_csv('CRZ_DB_with_supplements.csv', header = header, sep='|')