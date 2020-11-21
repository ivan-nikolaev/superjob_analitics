import pickle
from tqdm import tqdm
import zipfile
from .os_tools import generator_files_in_dir


def extract_zip_by_vacancy(input_zipfile, n=-1):
    #print(f"\nОткрываем zip файл: {input_zipfile}")
    with zipfile.ZipFile(input_zipfile) as opened_zip:

        tmp_files = opened_zip.namelist()[:]
        tmp_files_ = tmp_files[:] if n==-1 else tmp_files[:n]

        #for pickle_file_in_zip in tqdm(tmp_files_, desc="files in zip"):
        for pickle_file_in_zip in tmp_files_:
            with opened_zip.open(pickle_file_in_zip, 'r') as mypicklefile:
                block_vacancies = pickle.load(mypicklefile)
                for vacancy in block_vacancies:
                    yield vacancy


def get_vacancies_from_dir_with_zips(dir_with_zips):
    files_zip = [file for file in generator_files_in_dir(dir_with_zips, extension='.zip')]
    print(files_zip)

    vacancies = []
    for file_zip in files_zip:
        for vacancy in extract_zip_by_vacancy(file_zip):
            vacancies.append(vacancy)

    return vacancies


def generator_vacancies_from_dir_with_zips(dir_with_zips):
    files_zip = [file for file in generator_files_in_dir(dir_with_zips, extension='.zip')]

    for file_zip in files_zip:
        for vacancy in extract_zip_by_vacancy(file_zip):
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
        if (cat_set.issubset(vacancy_cats) or cat_set == set()) and (pos_set.issubset(vacancy_poss) or pos_set == set()):
            return True
        else:
            return False

def get_filtered_vacancies_gen(vacancies, cat_set=set(), pos_set=set()):
    for vacancy in vacancies:
        if filter_vacancy_by_catalogues_and_positions(vacancy, cat_set, pos_set):
            yield vacancy
