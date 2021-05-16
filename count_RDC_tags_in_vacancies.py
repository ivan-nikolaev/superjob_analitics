#%%
from project_tools.tools_for_vacancies import get_vacancies_from_dir_with_zips


#%%
CATALOGs_KEYs = {33}
CATALOGs_KEYs_str = 'CK_' + '_'.join([str(key) for key in CATALOGs_KEYs])

POSITIONs_KEYs = set()
POSITIONs_KEYs_str = 'PK_' + '_'.join([str(key) for key in POSITIONs_KEYs])

print(CATALOGs_KEYs_str, POSITIONs_KEYs_str)


hard_match = True

#%%
superjob_dir = r"f:\superjob.ru"

filtered_vacancies_zips_dir = superjob_dir + f"\\filtered_vacancies_by_cats_poss\\{CATALOGs_KEYs_str}_{POSITIONs_KEYs_str}_{'hard' if hard_match else 'soft'}"
print(filtered_vacancies_zips_dir)

vacancies = get_vacancies_from_dir_with_zips(filtered_vacancies_zips_dir)

#%%
print(CATALOGs_KEYs_str, POSITIONs_KEYs_str)

print("Всего вакансий найдено:", len(vacancies))

print("Пример вакансии:")
print(vacancies[0])


#%%
tags = ['Обязанности:', 'Требования:', 'Условия:']

def count_tag_in_vacancies(vacancies, tag):
    count = 0
    for vacancy in vacancies:
        try:
            if tag in vacancy['vacancyRichText']:
                count += 1
        except:
            pass
    return count

print(CATALOGs_KEYs_str, POSITIONs_KEYs_str)
for tag in tags:
    count = count_tag_in_vacancies(vacancies, tag)
    print(f"Текст '{tag}' есть в {count} вакансия | {str(round(count / len(vacancies), 2))} %")