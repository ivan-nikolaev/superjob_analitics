#%%
from project_tools.read_write_pickle import read_from_pickle

data = read_from_pickle(r"data\drc.pickle")
print(data[0]['catalogues'])

#%%
