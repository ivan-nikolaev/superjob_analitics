import os
import zipfile
from tqdm import tqdm
import pickle
from bs4 import BeautifulSoup as bs

def generator_files_in_dir(top_directory, extension=''):
    for root, dirs, files in os.walk(top_directory):
        for filename in filter(lambda fname: fname.endswith(extension), files):
            filename_full = os.path.join(root, filename)
            if os.path.isfile(filename_full) is True:
                yield filename_full

def extract_zip_by_vacancy(input_zip):
    print(f"\nОткрываем zip файл: {input_zip}")
    with zipfile.ZipFile(input_zip) as opened_zip:
        for pickle_file_in_zip in tqdm(opened_zip.namelist()[:], desc="files in zip"):
            with opened_zip.open(pickle_file_in_zip, 'r') as mypicklefile:
                block_vacancies = pickle.load(mypicklefile)
                for vacancy in block_vacancies:
                    yield vacancy

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


#%%
PROJECT_DIR = r"E:\\!Projects\\superjob.ru\\"
filtered_vacancies_zips = PROJECT_DIR + 'filtered_vacancies\\filtered_vacancies_CK_33_PK_'
files_zip = [file for file in generator_files_in_dir(filtered_vacancies_zips, extension='.zip')]
print(files_zip)


#извлекаем информацию из файлов pkl из файла zip с отфильтрованными вакансиями
vacancies = []
for file_zip in files_zip:
    for vacancy in extract_zip_by_vacancy(file_zip):
        vacancies.append(vacancy)