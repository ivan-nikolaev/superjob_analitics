#%%
import sys
sys.path.append("Y:\!all_python_projects")

from my_tools.files_and_dirs import generator_files_in_dir, is_file_exist
from my_tools.pickle_tools import read_from_pickle, write_to_pickle
from my_tools.my_json_tools import pp_json

from tqdm import tqdm

def find_requirements_and_duties_in_vacancies(vacancies, dict_rdc={}):
    for vacancy in vacancies:
        find_requirements_and_duties_in_vacancy(vacancy, dict_rdc)
    return dict_rdc

def find_requirements_and_duties_in_vacancy(vacancy, dict_rdc={}):
    main_tags = ['Обязанности', 'Требования', 'Условия']
    for tag in main_tags:
        second_tags = [tag, tag+":", "<b>"+tag+":</b>"]
        for s_tag in second_tags:
            try:
                if s_tag in vacancy['vacancyRichText']:
                    if s_tag not in dict_rdc:
                        dict_rdc[s_tag] = 1
                    else:
                        dict_rdc[s_tag] += 1
            except:
                pass
    return dict_rdc

def count_vacancies_positions(vacancies, dict_catalogues={}, catalog_id=None):
    for vacancy in vacancies:
        try:
            for catalog in vacancy['catalogues']:
                if catalog['id'] not in dict_catalogues:
                    dict_catalogues[catalog['id']] = {'count': 1, 'positions': {}}
                else:
                    dict_catalogues[catalog['id']]['count'] += 1

                for position in catalog['positions']:
                    if position['id'] not in dict_catalogues[catalog['id']]['positions']:
                        dict_catalogues[catalog['id']]['positions'][position['id']] = {'count':1}
                    else:
                        dict_catalogues[catalog['id']]['positions'][position['id']]['count'] += 1
        except:
            #print(vacancy)
            if (vacancy == {}):
                if -1 not in dict_catalogues:
                    dict_catalogues[-1] = {'count': 1, 'name': 'void_vacansies'}
                else:
                    dict_catalogues[-1]['count'] += 1
            else:
                if vacancy['error']['code'] not in dict_catalogues:
                    dict_catalogues[vacancy['error']['code']] = {'count': 1,'name':vacancy['error']['error']}
                else:
                    dict_catalogues[vacancy['error']['code']]['count'] += 1
    return dict_catalogues



def get_vacancies_by_catalog_id(vacancies, catalog_id):
    vacancies_by_catalog = []
    for vacancy in vacancies:
        try:
            if (catalog_id in get_catalogues_and_positions_in_vacancy(vacancy)[0]):
                vacancies_by_catalog.append(vacancy)
        except:
            continue
    return vacancies_by_catalog

def filter_vacancies_by_catalog_id(src_dir, dst_dir, catalog_id):
    for filename_src in tqdm(generator_files_in_dir(src_dir, extension='.pickle')):
        filename_dst = '\\'.join([dst_dir, filename_src.split('\\')[-1]])
        if is_file_exist(filename_dst):
            print(filename_dst, '- exists')
            continue
        vacancies = read_from_pickle(filename_src)
        vacancies_by_catalog = get_vacancies_by_catalog_id(vacancies, catalog_id)
        if(len(vacancies_by_catalog)>0):

            write_to_pickle(filename_dst, vacancies_by_catalog)


#%%
hh_dir = ""
main_dir = r"F:\superjob.ru\vacancies"


dict_catalogues = {}
dict_rdc = {}
for filename in tqdm(generator_files_in_dir(main_dir)):
    vacancies = read_from_pickle(filename)
    count_vacancies_positions(vacancies, dict_catalogues)
    find_requirements_and_duties_in_vacancies(vacancies, dict_rdc)
pp_json(dict_catalogues)
pp_json(dict_rdc)


write_to_pickle('dict_catalogues.pickle',dict_catalogues)
write_to_pickle('dict_rdc.pickle', dict_rdc)

#%%
data = read_from_pickle('dict_catalogues.pickle')
print(data)
#%%

catalogues = read_from_pickle(r"F:\superjob.ru\catalogues.pickle")
for c in catalogues[:]:

    print(pp_json(c))


#%%
for c in catalogues:
    print(c['key'], c['title'])
print(len(catalogues))


#%%

vacancies = read_from_pickle(r"F:\superjob.ru\vacancies_test\32497900_32498000.pickle")
print(vacancies[:3])


vacancies_catalog = []
for filename in generator_files_in_dir("F:\\superjob.ru\\vacancies_test"):
    vacancies = read_from_pickle(filename)
    vacancies_by_catalog = get_vacancies_by_catalog_id(vacancies, 33)
    print(len(vacancies), len(vacancies_by_catalog))
    vacancies_catalog.extend(vacancies_by_catalog)

print(len(vacancies_catalog))

#%%
src_dir = r"F:\superjob.ru\vacancies"
dst_dir = r"F:\superjob.ru\vacancies_test_filtered_33"
filter_vacancies_by_catalog_id(src_dir, dst_dir, 33)

#%%


files = []
for filename in generator_files_in_dir("F:\\superjob.ru\\vacancies_test_filtered_33"):
    files.append(filename)
print(len(files))
#%%
from bs4 import BeautifulSoup, NavigableString, Tag

def split_function(text):
    splitters = ['- ', '•', ';', '']

    max = 0
    max_splitted_text = []
    for splitter in splitters:
        splitted_text = text.split(splitter)
        if (len(splitted_text)>=max):
            max = len(splitted_text)
            max_splitted_text = splitted_text
    max_splitted_text = [text.strip() for text in max_splitted_text if(text!='')]

    return max_splitted_text

def get_block_info(soup, tag):
    try:
        block = soup.find(text=tag).next_element
        #print(tag)
        parts = block#split_function(block)
        #[print(part) for part in parts]

        return parts
    except:
        print(f"error {tag}")
        return []


def get_info_from_vacancy(vacancy):
    data = {'id': vacancy['id']}
    try:
        html = vacancy['vacancyRichText']
        soup = BeautifulSoup(html, 'html.parser')
        data['vacancyRichText']=vacancy['vacancyRichText']
        print(vacancy['vacancyRichText'])
    except:
        return data


    try:
        for e in soup.findAll('br'):
            e.extract()
    except:
        pass

    try:
        tags = soup.findAll('b')
        tags = [tag.text for tag in tags]
    except:
        pass

    try:
        for tag in tags:
            data[tag] = get_block_info(soup, tag)
    except:
        pass

    return data
list_data = []
for file in tqdm(files[:]):
    vacancies = read_from_pickle(file)
    for i in range(len(vacancies)):
        data = get_info_from_vacancy(vacancies[i])
        #print(data)
        list_data.append(data)

write_to_pickle('drc.pickle', list_data)

#for d in list_data:
#    print(d)
