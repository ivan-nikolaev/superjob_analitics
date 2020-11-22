import os
import shutil
import pandas
import zipfile

from project_tools.read_write_pickle import write_to_pickle
from project_tools.os_tools import generator_files_in_dir
from project_tools.functions_for_vacancies import extract_zip_by_vacancy

from project_tools.functions_for_vacancies import normalize_catalogues_positions
from project_tools.functions_for_vacancies import filter_keys

project_dir = "F:\\superjob.ru"
vacancies_zips = project_dir + '\\vacancies_zip_by_million'

files_zip = [file for file in generator_files_in_dir(vacancies_zips, extension='.zip')]
print(files_zip)

#%%
filtered_dir_name = project_dir + f'\\filtered_id_catalogues_date'

if os.path.exists(filtered_dir_name):
    shutil.rmtree(filtered_dir_name)

os.mkdir(filtered_dir_name)

for file_zip in files_zip[:2]:
    name = file_zip.split('\\')[-1].split('.')[0]

    filename_pkl = filtered_dir_name + f'\\id_date_cats_poss_{name}_df.pkl'
    if os.path.exists(filename_pkl):
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
    write_to_pickle(filename_pkl, df)

    with zipfile.ZipFile(filename_pkl.replace('.pkl','.zip'), 'w') as myzip:
        myzip.write(filename_pkl, os.path.basename(filename_pkl))

    os.remove(filename_pkl)
