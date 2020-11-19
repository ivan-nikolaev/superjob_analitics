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


# import os
#
#
# def generator_files_in_dir(top_directory, extension=''):
#     for root, dirs, files in os.walk(top_directory):
#         for filename in filter(lambda fname: fname.endswith(extension), files):
#             filename_full = os.path.join(root, filename)
#             if os.path.isfile(filename_full) is True:
#                 yield filename_full


# def extract_zip_by_vacancy(input_zip):
#     print(f"\nОткрываем zip файл: {input_zip}")
#     with zipfile.ZipFile(input_zip) as opened_zip:
#         for pickle_file_in_zip in tqdm(opened_zip.namelist()[:], desc="files in zip"):
#             with opened_zip.open(pickle_file_in_zip, 'r') as mypicklefile:
#                 block_vacancies = pickle.load(mypicklefile)
#                 for vacancy in block_vacancies:
#                     yield vacancy
#%%

superjob_dir = "F:\superjob.ru"
vacancies_zips = superjob_dir + '\\vacancies_zip_by_million'

files_zip = [file for file in generator_files_in_dir(vacancies_zips, extension='.zip')]
print(files_zip)





#%%
for cat_pos_tpl in [({33},{48}), ({33},{51}), ({33},{48,51})]:
    CATALOGs_KEYs = cat_pos_tpl[0]
    CATALOGs_KEYs_str = 'CK_'+'_'.join([str(key) for key in CATALOGs_KEYs])

    POSITIONs_KEYs = cat_pos_tpl[1]
    POSITIONs_KEYs_str = 'PK_'+'_'.join([str(key) for key in POSITIONs_KEYs])


    print(CATALOGs_KEYs_str, POSITIONs_KEYs_str)
    #%%
    import os
    import shutil

    project_dir = "F:\superjob.ru"
    name_dir = f"filtered_vacancies_{CATALOGs_KEYs_str}_{POSITIONs_KEYs_str}"

    filtered_dir_name = f"{project_dir}\{name_dir}"
    filtered_tmp_dir_name = filtered_dir_name + '\\tmp'


    if os.path.exists(filtered_dir_name ):
        shutil.rmtree(filtered_dir_name )

    os.mkdir(filtered_dir_name )
    os.mkdir(filtered_tmp_dir_name)
    #%%


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

    def filter_vacancies_by_catalogues_and_positions(vacancy, cat_set = {}, pos_set = {}):

        try:
          if cat_set == set(get_catalogues_from_vacancy(vacancy)) or cat_set=={}:
            if pos_set == set(get_positions_from_vacancy(vacancy)) or pos_set=={}:
              return True
            else:
              return False
          else:
            return False
        except:
          return False

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
            if filter_vacancies_by_catalogues_and_positions(vacancy, cat_set=CATALOGs_KEYs, pos_set=POSITIONs_KEYs):
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
