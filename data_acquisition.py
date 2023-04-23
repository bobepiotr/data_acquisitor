import datetime
import logging
import os.path
import time
from abc import ABC, abstractmethod
from datetime import datetime
from sys import argv
from urllib import request
from zipfile import ZipFile

import kaggle
import pandas as pd
from sodapy import Socrata

logging.basicConfig(level=logging.ERROR)


class DataSet(ABC):

    def __init__(self, name, filenames):
        self.name = name
        self.filenames = filenames

    @abstractmethod
    def download(self):
        pass

    @abstractmethod
    def get_last_mod_date(self):
        pass

    def get_filename(self, idx=0):

        return self.filenames[idx]


class CatalogDataSet(DataSet):

    def __init__(self, name, filenames, domain, _id, url):
        super().__init__(name, filenames)
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


class KaggleDataSet(DataSet):

    def __init__(self, name, filenames, url):
        super().__init__(name, filenames)
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

    @staticmethod
    def __remove_zip_file():
        path = FILES_LOCATION + 'historical-hourly-weather-data.zip'
        if os.path.exists(path):
            os.remove(path)


class DataSetManager(object):

    def __init__(self, dataset: DataSet):
        self.dataset = dataset

    def download(self):
        print('Downloading ' + self.dataset.name + '...')
        start_time = time.time_ns()
        self.dataset.download()
        end_time = time.time_ns()
        download_time_s = round((end_time - start_time) / 1_000_000_000, 3)
        files_size_mb = self.measure_size()
        print(f'Done downloading {self.dataset.name}. Total time = {download_time_s}s, total size =  {files_size_mb}MB')
        FILES_DATA[self.dataset.name] = [download_time_s, files_size_mb]

    def remote_dataset_updated(self):
        path = FILES_LOCATION + self.dataset.get_filename(0)
        last_modified_date = datetime.fromtimestamp(os.path.getmtime(path))

        return last_modified_date < self.dataset.get_last_mod_date()

    def assure_dataset_consistent(self):
        for f in self.dataset.filenames:
            if not os.path.exists(FILES_LOCATION + f):
                print('Dataset ' + self.dataset.name + ' inconsistent.')
                self.download()  # at least one file missing -> download whole dataset again
                return True
        print('Dataset ' + self.dataset.name + ' consistent.')
        return False

    def measure_size(self):
        total_size = 0
        for f in self.dataset.filenames:
            total_size += os.path.getsize(FILES_LOCATION + f)

        return round(total_size / 1024 / 1024, 3)


def assure_files_exists(datasets):
    for d in datasets:
        m = DataSetManager(d)
        m.assure_dataset_consistent()


def download_all(datasets):
    for d in datasets:
        m = DataSetManager(d)
        m.download()


def update_all(datasets):
    for d in datasets:
        m = DataSetManager(d)
        if m.remote_dataset_updated():
            print('Remote dataset updated. Downloading dataset ' + m.dataset.name + '...')
            m.download()
        else:
            print('Dataset ' + m.dataset.name + ' up to date.')


def set_up():
    if not os.path.exists(FILES_LOCATION):
        os.makedirs(FILES_LOCATION)

    if not os.path.exists(LOGS_LOCATION):
        os.makedirs(LOGS_LOCATION)


if __name__ == '__main__':

    mode = argv[1] if len(argv) > 1 else None  # allowed values: init - download all files, NONE - update all files

    FILES_LOCATION = './data/'
    LOGS_LOCATION = './data_logs/'
    FILES_DATA = {}

    set_up()

    CRIMES_FILENAMES = ['crimes_data.json']
    CRIMES_DATASET_NAME = 'crimes_dataset'
    CRIMES_DOMAIN = 'data.lacity.org'
    CRIMES_ID = '63jg-8b9z'
    CRIMES_URL = 'https://data.lacity.org/api/views/63jg-8b9z/rows.json?accessType=DOWNLOAD'

    COLLISIONS_FILENAMES = ['collisions_data.xml']
    COLLISIONS_DATASET_NAME = 'collisions_dataset'
    COLLISIONS_DOMAIN = 'data.lacity.org'
    COLLISIONS_ID = 'd5tf-ez2w'
    COLLISIONS_URL = 'https://data.lacity.org/api/views/d5tf-ez2w/rows.xml?accessType=DOWNLOAD'

    WEATHER_FILENAMES = ['pressure.csv', 'temperature.csv', 'weather_description.csv',
                         'wind_direction.csv', 'wind_speed.csv']
    WEATHER_DATASET_NAME = 'weather_dataset'
    WEATHER_URL = 'selfishgene/historical-hourly-weather-data'

    DATASETS = [
        CatalogDataSet(CRIMES_DATASET_NAME, CRIMES_FILENAMES, CRIMES_DOMAIN, CRIMES_ID, CRIMES_URL),
        CatalogDataSet(COLLISIONS_DATASET_NAME, COLLISIONS_FILENAMES, COLLISIONS_DOMAIN, COLLISIONS_ID, COLLISIONS_URL),
        KaggleDataSet(WEATHER_DATASET_NAME, WEATHER_FILENAMES, WEATHER_URL)
    ]

    if mode == 'init':
        download_all(DATASETS)
    else:
        assure_files_exists(DATASETS)
        update_all(DATASETS)

    download_times = pd.DataFrame(FILES_DATA, index=['Download time [s]', 'Total files size [MB]'])
    current_date = datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
    download_times.to_csv(LOGS_LOCATION + 'acquisition_info_' + current_date + '.csv')
