
#%%
import os
from datetime import datetime
#from project_tools.os_tools import generator_by_one_vacancies_from_dir
from project_tools.read_write_pickle import  write_to_pickle


def get_catalogues_and_positions_in_vacancy(vacancy):
    data = {}
    try:
        data['id'] = vacancy['id']
        catalogues = []
        catalogues_titles = []

        for catalog in vacancy['catalogues']:
            catalogues.append(catalog['id'])
            catalogues_titles.append(catalog['title'])

            positions = []
            positions_titles = []
            for position in catalog['positions']:
                positions.append(position['id'])
                positions_titles.append(position['title'])
            data['positions'] = positions
        data['catalogues'] = catalogues
        data['date_published'] = vacancy['date_published']
        return data
    except:
        return None

hh_dir = ""
main_dir = r"F:\superjob.ru\vacancies"
batch_size = 100000
res_dir = "F:\superjob.ru\datepub_catalogues_and_positions_in_vacancies"
if not os.path.exists(res_dir):
    os.makedirs(res_dir)

batch_part = []
A = 0
for vacancy in generator_by_one_vacancies_from_dir(main_dir):
    #print(vacancy)
    if(len(batch_part)>=batch_size):
        #datetime.strftime(datetime.now(), "%Y.%m.%d_%H.%M.%S")
        filename = f"{res_dir}\\{str(A).zfill(10)}_{str(A+batch_size).zfill(10)}.pickle"
        print(f"записываем в файл: {filename}")
        write_to_pickle(filename, batch_part)
        A +=batch_size
        batch_part = []

    res = get_catalogues_and_positions_in_vacancy(vacancy)
    if res != None:
        batch_part.append(res)
        #print(res)

