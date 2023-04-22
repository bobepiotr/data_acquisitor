import datetime
import os.path
from abc import ABC, abstractmethod
from datetime import datetime
from urllib import request
from zipfile import ZipFile

import kaggle
from sodapy import Socrata
import logging

from sys import argv


logging.basicConfig(level=logging.ERROR)


class DataSet(ABC):

    def __init__(self, filenames):
        self.filenames = filenames

    @abstractmethod
    def download(self):
        pass

    @abstractmethod
    def get_last_mod_date(self):
        pass

    def get_filename(self):
        return self.filenames[0]


class CatalogDataSet(DataSet):

    def __init__(self, filenames, domain, _id, url):
        super().__init__(filenames)
        self._id = _id
        self.domain = domain
        self.url = url

    def download(self):
        request.urlretrieve(self.url, FILES_LOCATION + self.filenames[0])

        return True

    def get_last_mod_date(self):
        with Socrata(self.domain, None) as client:
            metadata = client.get_metadata(self._id)

        return datetime.fromtimestamp(metadata['viewLastModified'])

    def get_filename(self):
        return self.filenames[0]


class KaggleDataSet(DataSet):

    def __init__(self, filenames, url):
        super().__init__(filenames)
        self.url = url

    def download(self):
        api = kaggle.KaggleApi()
        api.authenticate()

        api.dataset_download_files(self.url, path=FILES_LOCATION)
        zf = ZipFile(FILES_LOCATION + self.url.split('/')[-1] + '.zip')
        zf.extractall(path=FILES_LOCATION)
        zf.close()

        self.__remove_zip_file()

        return True

    def get_last_mod_date(self):
        api = kaggle.KaggleApi()
        api.authenticate()

        return api.dataset_view(self.url).lastUpdated

    def get_filename(self):
        return self.filenames[0]

    @staticmethod
    def __remove_zip_file():
        path = FILES_LOCATION + 'historical-hourly-weather-data.zip'
        if os.path.exists(path):
            os.remove(path)


class DataSetManager(object):

    def __init__(self, dataset: DataSet):
        self.dataset = dataset

    def download(self):
        filename = self.dataset.get_filename()
        print('Downloading ' + filename + '...')
        self.dataset.download()
        print('Done downloading ' + filename + '.')

    def remote_dataset_updated(self):
        path = FILES_LOCATION + self.dataset.get_filename()
        last_modified_date = datetime.fromtimestamp(os.path.getmtime(path))

        return last_modified_date < self.dataset.get_last_mod_date()

    def download_not_existing_files(self):
        for f in self.dataset.filenames:
            if not os.path.exists(FILES_LOCATION + f):
                self.download()


def assure_files_exists(datasets):
    for d in datasets:
        m = DataSetManager(d)
        m.download_not_existing_files()


def download_all(datasets):
    for d in datasets:
        m = DataSetManager(d)
        m.download()


def update_all(datasets):
    for d in datasets:
        m = DataSetManager(d)
        filename = d.get_filename()
        if m.remote_dataset_updated():
            print('Remote file updated. Downloading file ' + filename + '...')
            m.download()
        else:
            print('File ' + filename + ' up to date.')


if __name__ == '__main__':

    mode = argv[1] if len(argv) > 1 else None  # allowed values: init - download all files, NONE - update all files

    FILES_LOCATION = './data/'

    if not os.path.exists(FILES_LOCATION):
        os.makedirs(FILES_LOCATION)

    CRIMES_FILENAMES = ['crimes_data.json']
    CRIMES_DOMAIN = 'data.lacity.org'
    CRIMES_ID = '63jg-8b9z'
    CRIMES_URL = 'https://data.lacity.org/api/views/63jg-8b9z/rows.json?accessType=DOWNLOAD'

    COLLISIONS_FILENAMES = ['collisions_data.xml']
    COLLISIONS_DOMAIN = 'data.lacity.org'
    COLLISIONS_ID = 'd5tf-ez2w'
    COLLISIONS_URL = 'https://data.lacity.org/api/views/d5tf-ez2w/rows.xml?accessType=DOWNLOAD'

    WEATHER_FILENAMES = ['pressure.csv', 'temperature.csv', 'weather_description.csv',
                         'wind_direction.csv', 'wind_speed.csv']
    WEATHER_URL = 'selfishgene/historical-hourly-weather-data'

    DATASETS = [
        CatalogDataSet(CRIMES_FILENAMES, CRIMES_DOMAIN, CRIMES_ID, CRIMES_URL),
        CatalogDataSet(COLLISIONS_FILENAMES, COLLISIONS_DOMAIN, COLLISIONS_ID, COLLISIONS_URL),
        KaggleDataSet(WEATHER_FILENAMES, WEATHER_URL)
    ]

    if mode == 'init':
        download_all(DATASETS)
    else:
        assure_files_exists(DATASETS)
        update_all(DATASETS)
