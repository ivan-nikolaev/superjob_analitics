
import pickle


class Catalog():
    def __init__(self, path):
        self.path = path
        self.catalogues = None
        self.read_catalogs()

    def read_catalogs(self):
        try:
            with open(self.path, 'rb') as f:
                self.catalogues = pickle.load(f)
        except:
            print(f"{self.path} error reading catalog ...")

        #print(self.catalogues)

    def print_all_catalogues(self):
        print(f'key, title_rus')
        for catalog in self.catalogues:
            print(catalog['key'], ' '*(6-len(str(catalog['key']))), catalog['title_rus'])

    def get_catalog_by_key(self, key_catalog):
        for catalog in self.catalogues:
            if catalog['key'] == key_catalog:
                return catalog
        else:
            return -1

    def get_position_by_key(self, key_position):
        for catalog in self.catalogues:
            for position in catalog['positions']:
                if position['key'] == key_position:
                    return position
        else:
            return -1

    def print_all_positions(self, key_catalog):
        catalog = self.get_catalog_by_key(key_catalog)
        print(catalog['key'], catalog['title_rus'])
        for position in catalog['positions']:
            print(position['key'], ' ' * (6 - len(str(position['key']))), position['title_rus'])


if __name__ == "__main__":
    catalog_ = Catalog(r"data\catalogues.pickle")
    # print(catalog.get_catalog_by_number(33))
    # catalog.print_all_catalogues()
    # catalog.print_all_positions(33)

    print(catalog_.get_position_by_key(48))
