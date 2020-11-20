# %%
import os
import zipfile
from tqdm import tqdm
import pickle
import bs4
from bs4 import BeautifulSoup as bs
import pandas


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


# %%
superjob_dir = r"E:\\!Projects\\superjob.ru\\"

filtered_vacancies_zips = superjob_dir + 'filtered_vacancies\\filtered_vacancies_CK_33_PK_'

files_zip = [file for file in generator_files_in_dir(filtered_vacancies_zips, extension='.zip')]
print(files_zip)
# %%

# извлекаем информацию из файлов pkl из файла zip с отфильтрованными вакансиями
vacancies = []
for file_zip in files_zip:
    for vacancy in extract_zip_by_vacancy(file_zip):
        vacancies.append(vacancy)

len(vacancies)

print(vacancies[0])

texts = ['Обязанности:', 'Требования:', 'Условия:']


def count_texts_in_vacancies(vacancies, texts):
    for text in texts:
        count = 0
        for vacancy in vacancies:
            try:
                if text in vacancy['vacancyRichText']:
                    count += 1
            except:
                pass
        print()
        print(text, count, str(round(count / len(vacancies), 2)) + "%")


count_texts_in_vacancies(vacancies, texts)


# %%
def filter_vacancies_by_text_in_vacancyRichText(vacancies, sometext):
    vacancies_with_sometext = []
    vacancies_without_sometext = []

    for vacancy in tqdm(vacancies):
        try:
            if sometext in vacancy['vacancyRichText']:
                vacancies_with_sometext.append(vacancy)
            else:
                vacancies_without_sometext.append(vacancy)
        except:
            pass
    return vacancies_with_sometext, vacancies_without_sometext


w_text_1, wo_text = filter_vacancies_by_text_in_vacancyRichText(vacancies, '• ')

w_text_2, wo_text = filter_vacancies_by_text_in_vacancyRichText(wo_text, '- ')

w_text_3, wo_text = filter_vacancies_by_text_in_vacancyRichText(wo_text, '; ')

w_text_4 = wo_text
print()
print(len(wo_text))
# %%
some_vacancy = vacancies[0]
print(some_vacancy['vacancyRichText'])

# %%
soup = bs(vacancies[0]['vacancyRichText'].replace('<br />', ''), "html.parser")

# %%

# prettyHTML = soup.prettify()
# print(prettyHTML)
# %%
b_block = soup.find_all("b", text='Требования:')

print(b_block)

# %%
print(len(vacancies))


# %%
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


def filter_vacancies_by_catalogues_and_positions(vacancy, cat_set={}, pos_set={}):
    try:
        if cat_set == set(get_catalogues_from_vacancy(vacancy)) or cat_set == {}:
            if pos_set == set(get_positions_from_vacancy(vacancy)) or pos_set == {}:
                return True
            else:
                return False
        else:
            return False
    except:
        return False


def get_filtered_vacancies_gen(vacancies, cat_set={}, pos_set={}):
    for vacancy in vacancies:
        if filter_vacancies_by_catalogues_and_positions(vacancy, cat_set, pos_set):
            yield vacancy




# %%

def clear_attr(lst_attrs):
    clear_attrs = []
    for at in lst_attrs:
        if at == '':
            continue
        if at == 'br/':
            continue

        at = at.strip()
        at = at.replace('br/', '')
        clear_attrs.append(at)
    return clear_attrs


def get_attr_from_navigablestring(nvgbstr):
    ss = ['• ', '* ', '- ', '; ', 'br/']
    res = [nvgbstr.count(s) for s in ss]

    max_i = res.index(max(res))
    if nvgbstr.count(ss[max_i]) >= 1:
        return clear_attr(nvgbstr.split(ss[max_i]))

    # if nvgbstr.count('• ') >= 1:
    #     return [r.strip() for r in nvgbstr.split('•') if r != '']
    # elif nvgbstr.count('* ') >= 1:
    #     return [r.strip() for r in nvgbstr.split('* ') if r != '']
    # elif nvgbstr.count('- ') >= 1:
    #     return [r.strip() for r in nvgbstr.split('- ') if r != '']
    # elif nvgbstr.count() >= 1:
    #     return [r.strip() for r in nvgbstr.split('; ') if r != '']
    # else:
    #     return []


def get_attr_from_ul(ul):
    strs = [li.text for li in ul.find_all('li') if li.text != '']
    # если в списке одна строка
    if len(strs) == 1:
        # print(strs)
        if strs[0].count(';') >= 1:
            res = [r for r in strs[0].replace('\n', ' ').split(';')]
        elif strs[0].count('.\n') >= 1:
            res = [r for r in strs[0].split('.\n')]
        elif strs[0].count('.-') >= 1:
            res = [r for r in strs[0].split('.-')]
        elif strs[0].count('• ') >= 1:
            res = [r for r in strs[0].split('• ')]
        elif strs[0].count(';\n') >= 1:
            res = [r for r in strs[0].split(';\n')]
        else:
            res = []
    elif (len(strs) > 1):
        res = strs
    else:
        res = []
    return res
    #return [r.strip() for r in res if r != '']


def clear_vacancyRichText(vacancyRichText):
    text = vacancyRichText.replace('\r', '; ')
    text = text.replace('<br />', '<br/>')
    text = text.replace('<br/>', 'br/')

    while (text.find('br/br/') > 0):
        text = text.replace('br/br/', 'br/')

    text = text.replace('  ', ' ')
    return text


def get_attr_from_vacansy(vacancy, text, text_stop=None):
    if vacancy['vacancyRichText'] == None:
        return []

    clear_text = clear_vacancyRichText(vacancy['vacancyRichText'])

    soup = bs(clear_text, "html.parser")
    try:
        b_block = soup.find("b", text=text)
    except:
        b_block = None
    try:
        p_block = soup.find("p", text=text)
    except:
        p_block = None

    block = b_block if (b_block != None) else p_block

    try:
        if type(block.next_sibling) == bs4.element.NavigableString:
            res = get_attr_from_navigablestring(block.next_sibling)
            return [(vacancy['id'], r) for r in clear_attr(res)]
    except:
        pass

    try:
        if (block.next_sibling.name == 'ul'):
            res = get_attr_from_ul(block.next_sibling)
            return [(vacancy['id'], r) for r in clear_attr(res)]
    except:
        pass

    try:
        # print(block)
        if (block.find_next('ul').name == 'ul'):
            res = get_attr_from_ul(block.find_next('ul'))
            return [(vacancy['id'], r) for r in res]
    except:
        pass

    try:
        attr = []
        while block.next_sibling.name == 'p':
            # print(block.next_sibling.text)
            attr.append(block.next_sibling.text)
            block = block.next_sibling
            if (block == None or text_stop == block.next_sibling.text):
                break
        #return [(vacancy['id'], a.strip()) for a in attr if a != '' and a != 'br/']
        return [(vacancy['id'], r) for r in clear_attr(attr)]

    except Exception as inst:
        pass
        # print(type(inst))  # the exception instance
        # print(inst.args)  # arguments stored in .args
        # print(inst)

    try:
        block = block.parent
        attr = []
        while block.next_sibling.name == 'p':
            # print(block.next_sibling.text)
            attr.append(block.next_sibling.text)
            block = block.next_sibling
            # print(block, block.text, block.next_sibling)
            if (
                    block.next_sibling == None or text_stop in block.next_sibling.text):  # or text_stop == block.next_sibling.text
                break
        return [(vacancy['id'], r) for r in clear_attr(attr)]

    except Exception as inst:
        pass

    return []


requirements = []
duties = []
conditions = []

count_v = 0
count = 0

CK = {33}
PK = {}

for vacancy in tqdm(get_filtered_vacancies_gen(vacancies[:], cat_set=CK, pos_set=PK)):
    count_v += 1

    # print(count_v)
    # if vacancy['id'] == 34875462:
    #     print(bs(vacancy['vacancyRichText'], "html.parser").prettify())
    #     requirements.extend(get_attr_from_vacansy(vacancy, 'Обязанности:', 'Требования:'))
    #     duties.extend(get_attr_from_vacansy(vacancy, 'Требования:', 'Условия:'))
    #     conditions.extend(get_attr_from_vacansy(vacancy, 'Условия:', '<br/>'))

    requirements.extend(get_attr_from_vacansy(vacancy, 'Обязанности:', 'Требования:'))
    duties.extend(get_attr_from_vacansy(vacancy, 'Требования:', 'Условия:'))
    conditions.extend(get_attr_from_vacansy(vacancy, 'Условия:'))

print(count_v)
r = len(list(set([i[0] for i in requirements])))
d = len(list(set([i[0] for i in duties])))
c = len(list(set([i[0] for i in conditions])))

print('Обязанностей найдено: ', len(requirements), 'в вакансиях', r, round(r / count_v, 2), "%")
print('Требований найдено: ', len(duties), 'в вакансиях', d, round(d / count_v, 2), "%")
print('Условий найдено: ', len(conditions), 'в вакансиях', c, round(c / count_v, 2), "%")

#%%
r_new = [(i[0], i[1], 'requirements') for i in requirements]
d_new = [(i[0], i[1], 'duties') for i in duties]
c_new = [(i[0], i[1], 'conditions') for i in conditions]


all = r_new + d_new + c_new
print(len(all))

#%%
def filter_keys(vacancy, filter_keys):
    new_vacancy = {filter_key: vacancy[filter_key] for filter_key in filter_keys if filter_key in vacancy.keys()}
    return new_vacancy


def normalize_catalogues_positions(vacancy):
    try:
        new_vacancy = {'id': vacancy['id'], 'date_published': vacancy['date_published']}
        new_vacancy['catalogues'] = [catalog['id'] for catalog in vacancy['catalogues']]
        new_vacancy['positions'] = [[position['id'] for position in catalog['positions']] for catalog in vacancy['catalogues']]
    except:
        new_vacancy = {}

    return new_vacancy

vacancies_ = []
for vacancy in vacancies:
    new_vacancy = filter_keys(vacancy, ['id', 'catalogues', 'date_published'])
    try:
      del new_vacancy['catalogues'][0]['title']
      for p in new_vacancy['catalogues'][0]['positions']:
        del p['title']
    except:
      pass

    vacancies_.append(new_vacancy)

normalize_vacancies = [normalize_catalogues_positions(vacancy) for vacancy in tqdm(vacancies)]
normalize_vacancies_filtered = [vacancy for vacancy in tqdm(normalize_vacancies) if vacancy != {}]
df = pandas.DataFrame(normalize_vacancies_filtered)

#%%
print(df.head())


#%%
df_all = pandas.DataFrame(all, columns=['id_', 'attr', 'rdc'])
print(df_all.head())

#%%

df_all_data_cat_pos = pandas.merge(df_all, df, left_on='id_', right_on='id', how='left').drop('id_', axis=1)
print(df_all_data_cat_pos.head())
print(df_all_data_cat_pos.iloc[0])

#%%
df_all_data_cat_pos.to_pickle(f'rdc\\df_all_rdc_date_cat_{CK}_pos_{PK}.pkl', protocol=4)
df_all_data_cat_pos.to_csv(f'rdc\\df_all_rdc_date_cat_{CK}_pos_{PK}.csv')


#%%
def count_symbols(text_, dict_):
    for s in text_:
        if s in dict_:
            dict_[s] += 1
        else:
            dict_[s] = 1


dict_symbols = {}
for vacancy in tqdm(get_filtered_vacancies_gen(vacancies[:], cat_set={}, pos_set={})):
    try:
        text = bs(vacancy['vacancyRichText'], "html.parser").text
    except Exception as ex:
        print(ex)
        text = ''
    count_symbols(text, dict_symbols)
# %%
a = 'абвгдеёжзийклмнопрстуфхцчшщъыьэюя'
a = a + a.upper()
a = a + '0123456789'
a = a + 'abcdefghijklmnopqrstuvwxyz' + 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'

for key, value in dict_symbols.items():
    if key not in a:
        print(key, value)
