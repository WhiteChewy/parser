r'''
Testing for DFA Media.
Author: Nikita Kulikov
Date: 03.2023

Save XLSX to database and then calculate 
'''
from XLSX_Parser import XlsxReader, XlsxToDatabase, EstimatedTotalByData
from config import DB_USERNAME, DB_PASSWORD, DB_HOST, DB_NAME



if __name__ == '__main__':
    file = XlsxReader(r'table.xlsx')
    estimated_totals = EstimatedTotalByData(DB_USERNAME, DB_PASSWORD, DB_HOST, DB_NAME)
    dt = estimated_totals.get_all()
    for company in dt.keys():
        for types in dt[company].keys():
            for value in dt[company][types].keys():
                print('\n%s %s %s\n' % (company, types, value), dt[company][types][value])
    # saver = XLSX_to_database('admin', 'sasuke007192', 'localhost', 'dfa_media')
    # saver.save(file)

