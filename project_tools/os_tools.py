import os
import pickle
from tqdm import tqdm
import zipfile


def generator_files_in_dir(top_directory, extension=''):
    for root, dirs, files in os.walk(top_directory):
        for filename in filter(lambda fname: fname.endswith(extension), files):
            filename_full = os.path.join(root, filename)
            if os.path.isfile(filename_full) is True:
                yield filename_full


def extract_zip_by_vacancy(input_zipfile, n=-1):
    print(f"\nОткрываем zip файл: {input_zipfile}")
    with zipfile.ZipFile(input_zipfile) as opened_zip:

        tmp_files = opened_zip.namelist()[:]
        tmp_files_ = tmp_files[:] if n==-1 else tmp_files[:n]

        for pickle_file_in_zip in tqdm(tmp_files_, desc="files in zip"):
            with opened_zip.open(pickle_file_in_zip, 'r') as mypicklefile:
                block_vacancies = pickle.load(mypicklefile)
                for vacancy in block_vacancies:
                    yield vacancy