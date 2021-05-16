# %%
import os
from tqdm import tqdm
import bs4
from bs4 import BeautifulSoup as bs
import pandas
import logging

from project_tools.tools_for_vacancies import generator_vacancies_from_dir_with_zips


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
    # return [r.strip() for r in res if r != '']


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
        # return [(vacancy['id'], a.strip()) for a in attr if a != '' and a != 'br/']
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


# %%

hard_match = True

superjob_dir = r"f:\\superjob.ru\\"

filtered_vacancies_dir = superjob_dir + f"filtered_vacancies_by_cats_poss"

import shutil
name_new_dir = filtered_vacancies_dir + "_only_rdc"
if os.path.exists(name_new_dir):
    shutil.rmtree(name_new_dir)
os.mkdir(name_new_dir)



logging.basicConfig(handlers=[logging.FileHandler(filename=f'{name_new_dir}\\log.log',
                              encoding='utf-8',
                              mode='a+')],
                    format="%(asctime)s %(name)s:%(levelname)s:%(message)s",
                    datefmt="%F %A %T",
                    level=logging.INFO)

#%%

#sub_dir = os.listdir(filtered_vacancies_dir)[0]
for sub_dir in tqdm(os.listdir(filtered_vacancies_dir), desc="sub_dirs"):

    sub_filtered_dir = filtered_vacancies_dir + "\\" + sub_dir
    print()
    print(sub_filtered_dir, end = '')
    logging.info("Каталог: " + sub_filtered_dir)

    #%%
    # def filter_vacancies_by_text_in_vacancyRichText(vacancies, sometext):
    #     vacancies_with_sometext = []
    #     vacancies_without_sometext = []
    #
    #     for vacancy in tqdm(vacancies):
    #         try:
    #             if sometext in vacancy['vacancyRichText']:
    #                 vacancies_with_sometext.append(vacancy)
    #             else:
    #                 vacancies_without_sometext.append(vacancy)
    #         except:
    #             pass
    #     return vacancies_with_sometext, vacancies_without_sometext
    #
    #
    # w_text_1, wo_text = filter_vacancies_by_text_in_vacancyRichText(vacancies, '• ')
    #
    # w_text_2, wo_text = filter_vacancies_by_text_in_vacancyRichText(wo_text, '- ')
    #
    # w_text_3, wo_text = filter_vacancies_by_text_in_vacancyRichText(wo_text, '; ')
    #
    # w_text_4 = wo_text
    # print()
    # print(len(wo_text))
    #%%

    requirements = []
    duties = []
    conditions = []

    count = 0
    for vacancy in generator_vacancies_from_dir_with_zips(sub_filtered_dir):
        # print(count_v)
        # if vacancy['id'] == 34875462:
        #     print(bs(vacancy['vacancyRichText'], "html.parser").prettify())
        #     requirements.extend(get_attr_from_vacansy(vacancy, 'Обязанности:', 'Требования:'))
        #     duties.extend(get_attr_from_vacansy(vacancy, 'Требования:', 'Условия:'))
        #     conditions.extend(get_attr_from_vacansy(vacancy, 'Условия:', '<br/>'))
        count += 1
        print(f" items {count}", end = '')
        requirements.extend(get_attr_from_vacansy(vacancy, 'Обязанности:', 'Требования:'))
        duties.extend(get_attr_from_vacansy(vacancy, 'Требования:', 'Условия:'))
        conditions.extend(get_attr_from_vacansy(vacancy, 'Условия:'))
        x = len(str(count))
        print('\b'*(x+7), end='')



    print(count)
    r = len(list(set([i[0] for i in requirements])))
    d = len(list(set([i[0] for i in duties])))
    c = len(list(set([i[0] for i in conditions])))


    text = f'Всего вакансий: {count}\n' + \
           f'Обязанностей найдено: {len(requirements)} в вакансиях {r} {round(r / count, 2)}%\n' + \
           f'Требований найдено: {len(duties)} в вакансиях {d} {round(d / count, 2)} %\n' + \
           f'Условий найдено: {len(conditions)} в вакансиях {c} {round(c / count, 2)} %\n'

    print(text)
#%%
    logging.info(text)

    #%%
    print("Собираем df из RDC")
    r_new = [(i[0], i[1], 'requirements') for i in requirements]
    d_new = [(i[0], i[1], 'duties') for i in duties]
    c_new = [(i[0], i[1], 'conditions') for i in conditions]


    rdc = r_new + d_new + c_new
    df_rdc = pandas.DataFrame(rdc, columns=['id_', 'attr', 'rdc'])
    print(df_rdc.head())
    print(len(df_rdc))

    #%%
    from project_tools.tools_for_vacancies import filter_keys
    from project_tools.tools_for_vacancies import normalize_catalogues_positions

    new_vacancies = []
    for vacancy in generator_vacancies_from_dir_with_zips(sub_filtered_dir):
        new_vacancy = filter_keys(vacancy, ['id', 'catalogues', 'date_published'])

        if (new_vacancy != {}):
            normalize_new_vacancy = normalize_catalogues_positions(new_vacancy)
            new_vacancies.append(normalize_new_vacancy)
            # print(normalize_new_vacancy)

    df = pandas.DataFrame(new_vacancies)
    print(df.head())
    print(len(df))

    #%%

    print("Собираем общий датафрейм из RDC и краткой информации о вакансии")
    df_id_rdc_date_cat_pos = pandas.merge(df_rdc, df, left_on='id_', right_on='id', how='left').drop('id_', axis=1)
    print(df_id_rdc_date_cat_pos.head())
    print(df_id_rdc_date_cat_pos.iloc[0])

    #%%

    filename = sub_dir+"_df_id_rdc_date_cat_pos"

    df_id_rdc_date_cat_pos.to_pickle(f'{name_new_dir}\\{filename}.pkl', protocol=4)
    df_id_rdc_date_cat_pos.to_csv(f'{name_new_dir}\\{filename}.csv')
    logging.info('='*20)

