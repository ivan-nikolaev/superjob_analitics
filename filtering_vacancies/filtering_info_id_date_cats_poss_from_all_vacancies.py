import os
import shutil
import pandas

from project_tools.read_write_pickle import write_to_pickle
from project_tools.os_tools import generator_files_in_dir
from project_tools.os_tools import extract_zip_by_vacancy


def filter_keys(vacancy, filter_keys):
    new_vacancy = {filter_key: vacancy[filter_key] for filter_key in filter_keys if filter_key in vacancy.keys()}
    return new_vacancy


def normalize_catalogues_positions(vacancy):
    try:
        new_vacancy = {'id': vacancy['id'], 'date_published': vacancy['date_published']}
        new_vacancy['catalogues'] = [catalog['id'] for catalog in vacancy['catalogues']]
        new_vacancy['positions'] = [[position['id'] for position in catalog['positions']] for catalog in
                                    vacancy['catalogues']]
    except:
        new_vacancy = {}

    return new_vacancy


project_dir = "F:\\superjob.ru"
vacancies_zips = project_dir + '\\vacancies_zip_by_million'

files_zip = [file for file in generator_files_in_dir(vacancies_zips, extension='.zip')]
print(files_zip)

#%%
filtered_dir_name = project_dir + f'\\filtered_id_catalogues_date'

if os.path.exists(filtered_dir_name):
    shutil.rmtree(filtered_dir_name)

os.mkdir(filtered_dir_name)

for file_zip in files_zip[:]:
    name = file_zip.split('\\')[-1]

    filename = filtered_dir_name + f'\\id_date_cats_poss_{name}_df.pkl'
    if os.path.exists(filename):
        print(f'Файл уже существует {name}')
        continue

    filtered_normalized_vacancies = []
    for vacancy in extract_zip_by_vacancy(file_zip):
        new_vacancy = filter_keys(vacancy, ['id', 'catalogues', 'date_published'])

        if(new_vacancy != {}):
            normalize_new_vacancy = normalize_catalogues_positions(new_vacancy)
            filtered_normalized_vacancies.append(new_vacancy)
            #print(normalize_new_vacancy)

    df = pandas.DataFrame(filtered_normalized_vacancies)
    write_to_pickle(filename, df)
