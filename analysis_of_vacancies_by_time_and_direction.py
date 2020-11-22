
import pandas
import pickle
import zipfile
from tqdm import tqdm
# %%
from project_tools.os_tools import generator_files_in_dir

filtered_dir = r"E:\Google Drive nikolaev.ivan.rf\vacancies\filtered_id_catalogues_date"

dfs = []
for file_zip in tqdm(generator_files_in_dir(filtered_dir, extension='.zip')):
    with zipfile.ZipFile(file_zip) as opened_zip:
        for pickle_file_in_zip in opened_zip.namelist():
            with opened_zip.open(pickle_file_in_zip, 'r') as mypicklefile:
                df = pickle.load(mypicklefile)
                dfs.append(df)

#%%

df = pandas.concat(dfs, ignore_index=True)
df.head(3)
print(len(df))

#%%
df['date_published_normal'] = df['date_published'].apply(datetime.datetime.fromtimestamp)
df['catalogues_len'] = df['catalogues'].apply(len)
df.head()

#%%

# распределение вакансий по количеству catalogues у одной вакансии
df['catalogues_len'].value_counts()
#%%

df_catalogues_len_1 = df[df['catalogues_len'] == 1]
df_catalogues_len_1['catalogues_1'] = df_catalogues_len_1['catalogues'].apply(lambda x: x[0])
df_catalogues_len_1.head()

#%%
len(df_catalogues_len_1)

#%%

from matplotlib.pyplot import xticks
dfboth = df_catalogues_len_1.groupby([df_catalogues_len_1['date_published_normal'].dt.strftime('%y-%m'),'catalogues_1']).count()['catalogues_len'].unstack()
dfboth.columns = [str(col)+" "+catalogues.get_name_catalog_by_key(col) for col in dfboth.columns]

dfboth.plot(figsize=(30,15), title="Test", xticks=range(len(dfboth)), grid=True, rot=90)