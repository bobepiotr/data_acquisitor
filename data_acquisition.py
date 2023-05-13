import csv
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


class Log(object):
    HEADERS = ['datetime', 'dataset', 'operation', 'cause', 'error']

    def __init__(self, timestamp, dataset_name, operation, cause, error):
        self.timestamp = timestamp
        self.dataset_name = dataset_name
        self.operation = operation
        self.cause = cause
        self.error = error

    def to_list(self):
        return [self.timestamp, self.dataset_name, self.operation, self.cause, self.error]


class DataSet(ABC):

    def __init__(self, name, filenames):
        self.name = name
        self.filenames = filenames

    @abstractmethod
    def download(self, **kwargs):
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

    def download(self, **kwargs):
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

    def download(self, **kwargs):
        api = kaggle.KaggleApi()
        api.authenticate()

        if 'files_to_download' in kwargs.keys():
            files_to_download = kwargs['files_to_download']
            for f in files_to_download:
                api.dataset_download_file(self.url, path=FILES_LOCATION, file_name=f)
                self.__unpack_zip_file(f)
                self.__remove_zip_file(f)
        else:
            api.dataset_download_files(self.url, path=FILES_LOCATION)
            dataset_name = self.url.split('/')[-1]
            self.__unpack_zip_file(dataset_name)
            self.__remove_zip_file(dataset_name)

        return True

    def get_last_mod_date(self):
        api = kaggle.KaggleApi()
        api.authenticate()

        return api.dataset_view(self.url).lastUpdated

    @staticmethod
    def __remove_zip_file(filename):
        path = FILES_LOCATION + filename + '.zip'
        if os.path.exists(path):
            os.remove(path)

    @staticmethod
    def __unpack_zip_file(filename):
        path = FILES_LOCATION + filename + '.zip'
        if os.path.exists(path):
            zf = ZipFile(path)
            zf.extractall(path=FILES_LOCATION)
            zf.close()


class DataSetManager(object):

    def __init__(self, dataset: DataSet):
        self.dataset = dataset

    def download(self, **kwargs):
        if 'files_to_download' in kwargs.keys():
            filenames = kwargs['files_to_download']
            log(f'Downloading files {filenames} from dataset \'{self.dataset.name}\'.')
        else:
            log(f'Downloading \'{self.dataset.name}\'...')
        start_time = time.time_ns()
        self.dataset.download(**kwargs)
        end_time = time.time_ns()
        download_time_s = round((end_time - start_time) / 1_000_000_000, 3)
        files_size_mb = self.measure_size()
        log(f'Done downloading \'{self.dataset.name}\'. Total time = {download_time_s}s, total size =  {files_size_mb}MB')
        ACQUISITION_INFO[self.dataset.name] = [download_time_s, files_size_mb]

    def remote_dataset_updated(self):
        outdated_files = []
        for f in self.dataset.filenames:
            path = FILES_LOCATION + f
            last_modified_date = datetime.fromtimestamp(os.path.getmtime(path))
            dataset_last_modified_date = self.dataset.get_last_mod_date()
            if last_modified_date < dataset_last_modified_date:
                outdated_files.append(f)

        if outdated_files:
            log(f'Dataset \'{self.dataset.name}\' outdated. Outdated files {outdated_files}')

        return bool(outdated_files), outdated_files

    def assure_dataset_consistent(self):
        missing_files = []
        for f in self.dataset.filenames:
            if not os.path.exists(FILES_LOCATION + f):
                missing_files.append(f)

        if missing_files:
            log(f'Dataset \'{self.dataset.name}\' inconsistent. Missing files: {missing_files}')
            self.download(files_to_download=missing_files)  # at least one file missing -> download whole dataset again

        log('Dataset \'' + self.dataset.name + '\' consistent.')

        return missing_files

    def measure_size(self):
        total_size = 0
        for f in self.dataset.filenames:
            total_size += os.path.getsize(FILES_LOCATION + f)

        return round(total_size / 1024 / 1024, 3)


def assure_files_exists(datasets):
    for d in datasets:
        m = DataSetManager(d)
        missing_files = m.assure_dataset_consistent()
        if missing_files:
            persist_log(
                Log(datetime.now(), d.name, 'DOWNLOAD', f'Dataset inconsistent. Missing files {missing_files}', None))


def download_all(datasets):
    for d in datasets:
        m = DataSetManager(d)
        m.download()
        persist_log(Log(datetime.now(), d.name, 'INIT', 'Init dataset', None))


def update_all(datasets):
    for d in datasets:
        m = DataSetManager(d)
        dataset_updated, outdated_files = m.remote_dataset_updated()
        if dataset_updated:
            log('Remote dataset updated. Downloading dataset \'' + m.dataset.name + '\'...')
            m.download()
            persist_log(Log(datetime.now(), d.name, 'UPDATE', f'Outdated files: {outdated_files}', None))
        else:
            log('Dataset \'' + m.dataset.name + '\' up to date.')


def set_up():
    if not os.path.exists(FILES_LOCATION):
        os.makedirs(FILES_LOCATION)

    if not os.path.exists(ACQUISITION_INFO_LOCATION):
        os.makedirs(ACQUISITION_INFO_LOCATION)

    if not os.path.exists(LOGS_LOCATION):
        os.makedirs(LOGS_LOCATION)

    logs_path = os.path.join(LOGS_LOCATION, LOGS_FILENAME)
    if not os.path.exists(logs_path):
        with open(logs_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(Log.HEADERS)


def persist_log(log: Log):
    with open('./logs/acquisition_log.csv', 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(log.to_list())


def log(content):
    current_dt = datetime.now()
    print(f'[{current_dt}: {APP_NAME}] {content}')


if __name__ == '__main__':

    # allowed values: init - download all files, NONE - assure all datasets consistent and up to date
    mode = argv[1] if len(argv) > 1 else None

    APP_NAME = 'ACQ_TOOL'

    # To specify files location set DATA_ACK_FILES_LOCATION env variable
    FILES_LOCATION = os.getenv('DATA_ACK_FILES_LOCATION', './data/')
    ACQUISITION_INFO_LOCATION = './data_logs/'
    ACQUISITION_INFO = {}
    LOGS_LOCATION = './logs/'
    LOGS_FILENAME = 'acquisition_log.csv'

    set_up()

    CRIMES_FILENAMES = ['crimes_data.csv']
    CRIMES_DATASET_NAME = 'crimes_dataset'
    CRIMES_DOMAIN = 'data.lacity.org'
    CRIMES_ID = '63jg-8b9z'
    CRIMES_URL = 'https://data.lacity.org/api/views/63jg-8b9z/rows.csv?accessType=DOWNLOAD'

    COLLISIONS_FILENAMES = ['collisions_data.xml']
    COLLISIONS_DATASET_NAME = 'collisions_dataset'
    COLLISIONS_DOMAIN = 'data.lacity.org'
    COLLISIONS_ID = 'd5tf-ez2w'
    COLLISIONS_URL = 'https://data.lacity.org/api/views/d5tf-ez2w/rows.xml?accessType=DOWNLOAD'

    WEATHER_FILENAMES = ['pressure.csv', 'temperature.csv', 'weather_description.csv',
                         'wind_direction.csv', 'wind_speed.csv', 'humidity.csv', 'city_attributes.csv']
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

    download_times = pd.DataFrame(ACQUISITION_INFO, index=['Download time [s]', 'Total files size [MB]'])
    current_date = datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
    download_times.to_csv(ACQUISITION_INFO_LOCATION + 'acquisition_info_' + current_date + '.csv')
