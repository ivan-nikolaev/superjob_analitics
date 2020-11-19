import pickle

def read_from_pickle(filename):
    with open(filename, 'rb') as f:
        data_new = pickle.load(f)
    return data_new

def write_to_pickle(filename, data):
    with open(filename, 'wb') as f:
        pickle.dump(data, f)
