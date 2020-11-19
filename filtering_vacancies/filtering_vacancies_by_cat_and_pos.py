import time
import zipfile
import sys
import os
from tqdm import tqdm
from bs4 import BeautifulSoup as bs
from zipfile import ZipFile
import pandas
import datetime

from project_tools.read_write_pickle import read_from_pickle
from project_tools.read_write_pickle import write_to_pickle

from project_tools.os_tools import generator_files_in_dir
from project_tools.os_tools import extract_zip_by_vacancy


def get_catalogues_from_vacancy(vacancy):
    try:
        return [catalog['key'] for catalog in vacancy['catalogues']]
    except:
        return []


def get_positions_from_vacancy(vacancy):
    try:
        return [position['key'] for catalog in vacancy['catalogues'] for position in catalog['positions']]
    except:
        return []


def filter_vacancy_by_catalogues_and_positions(vacancy, cat_set=set(), pos_set=set(), hard_match=True):
    try:
        vacancy_cats = set(get_catalogues_from_vacancy(vacancy))
        vacancy_poss = set(get_positions_from_vacancy(vacancy))
    except:
        return False

    if hard_match:
        if (cat_set == vacancy_cats or cat_set == set()) and (pos_set == vacancy_poss or pos_set == set()):
            return True
        else:
            return False
    else:
        if (cat_set.issubset(vacancy_cats) or cat_set == set()) and (
                pos_set.issubset(vacancy_poss) or pos_set == set()):
            return True
        else:
            return False


#%%

superjob_dir = "F:\superjob.ru"
vacancies_zips = superjob_dir + '\\vacancies_zip_by_million'

files_zip = [file for file in generator_files_in_dir(vacancies_zips, extension='.zip')]
print(files_zip)



#%%

#TPLs = [({33},{48}), ({33},{51}), ({33},{48,51})]
TPLs = [({33},{})]


for cat_pos_tpl in TPLs:
    CATALOGs_KEYs = cat_pos_tpl[0]
    CATALOGs_KEYs_str = 'CK_'+'_'.join([str(key) for key in CATALOGs_KEYs])

    POSITIONs_KEYs = cat_pos_tpl[1]
    POSITIONs_KEYs_str = 'PK_'+'_'.join([str(key) for key in POSITIONs_KEYs])


    print(CATALOGs_KEYs_str, POSITIONs_KEYs_str)
    #%%
    import os
    import shutil

    project_dir = "F:\superjob.ru"
    name_dir = f"filtered_vacancies\\{CATALOGs_KEYs_str}_{POSITIONs_KEYs_str}"

    filtered_dir_name = f"{project_dir}\{name_dir}"
    filtered_tmp_dir_name = filtered_dir_name + '\\tmp'


    if os.path.exists(filtered_dir_name ):
        shutil.rmtree(filtered_dir_name )

    os.mkdir(filtered_dir_name )
    os.mkdir(filtered_tmp_dir_name)
    #%%

    vacancies = []
    part=1

    for file_zip in files_zip[:]:
        print(f"Фильтруем вакансии по CATALOGs_KEYs = {CATALOGs_KEYs}, POSITIONs_KEYs = {POSITIONs_KEYs}")
        name = file_zip.split('\\')[-1].split('.')[0]
        file_pkl = f'{filtered_tmp_dir_name}\\filtered_{name}'

        #if os.path.exists(file_pkl):
        #    print(f'Файл уже существует {name}')
        #    continue

        for vacancy in extract_zip_by_vacancy(file_zip):
            if filter_vacancy_by_catalogues_and_positions(vacancy, cat_set=CATALOGs_KEYs, pos_set=POSITIONs_KEYs):
                vacancies.append(vacancy)

            if(len(vacancies)==100):
                write_to_pickle(f"{file_pkl}_{str(part).zfill(5)}.pkl", vacancies)
                vacancies = []
                part+=1

        new_file_zip = filtered_dir_name +"\\"+ file_zip.split('\\')[-1]
        files_pkl = [file for file in generator_files_in_dir(filtered_tmp_dir_name, extension = '.pkl')]

        if(len(files_pkl)>0):
            shutil.make_archive(new_file_zip, 'zip', filtered_tmp_dir_name)

    #    print('Записываем все pkl из папке tmp в zip')
    #    with ZipFile(new_file_zip, 'w') as zipObj:
    #      for file_pkl in tqdm(files_pkl):
    #        zipObj.write(file_pkl)

        print('Отчищаем папку tmp от pkl файлов')
        shutil.rmtree(filtered_tmp_dir_name)
        os.mkdir(filtered_tmp_dir_name)
