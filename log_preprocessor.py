import os
import re
from datetime import datetime
import csv
from sys import argv


class LogFile(object):

    def __init__(self, path):
        self.path = path
        self.content = self.get_content()

    def get_content(self):
        with open(self.path) as f:
            return f.readlines()

    def is_failed(self):
        phrase_failed = 'Status : FAILED'
        for line in self.content:
            if phrase_failed in line:
                return True
        return False

    def get_total_exec_time(self):
        maps_time = self.get_maps_time_ms()
        reduces_time = self.get_reduces_time()

        return maps_time + reduces_time

    def get_maps_time_ms(self):
        map_time_phrase = 'Total time spent by all maps in occupied slots (ms)='
        for line in self.content:
            if map_time_phrase in line:
                return int(line.replace(map_time_phrase, ''))
        return 0

    def get_reduces_time(self):
        reduce_time_phrase = 'Total time spent by all reduces in occupied slots (ms)='
        for line in self.content:
            if reduce_time_phrase in line:
                return int(line.replace(reduce_time_phrase, ''))
        return 0

    def get_errors(self):
        content = ''.join(self.content)
        error_blocks = re.findall(r'Error:.*?\n\n', content, re.DOTALL)
        if error_blocks:
            return ''.join(error_blocks)
        else:
            return None


class LogManager(object):
    class Log(object):
        HEADERS = ['datetime', 'process_name', 'status', 'total_time', 'maps_time', 'reduces_time', 'errors']

        def __init__(self, timestamp, process_name, status, total_time, maps_time, reduces_time, errors):
            self.timestamp = timestamp
            self.process_name = process_name
            self.status = status
            self.total_time = total_time
            self.maps_time = maps_time
            self.reduces_time = reduces_time
            self.errors = errors

        def to_list(self):
            return [self.timestamp, self.process_name, self.status, self.total_time, self.maps_time, self.reduces_time, self.errors]

    def __init__(self, log_path, process_name):
        self.__assure_file_system_consistent()

        self.log_path = log_path
        self.process_name = process_name

    def append_log_data(self):
        status, total_time, maps_time, reduces_time, errors = self.__extract_process_data()
        current_datetime = datetime.now()
        log_data = self.Log(current_datetime, self.process_name, status, total_time, maps_time, reduces_time, errors)
        self.__append_to_log_file(log_data.to_list())
        log(f'Preprocessed data from process \'{self.process_name}\' has been appended to {LOG_FILE_PATH}.')

    def __assure_file_system_consistent(self):
        if not os.path.exists(LOG_FILE_LOCATION):
            os.makedirs(LOG_FILE_LOCATION)
            log('Preprocessed log home directory initialized.')
        if not os.path.exists(LOG_FILE_PATH):
            with open(LOG_FILE_PATH, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(self.Log.HEADERS)
                log('Preprocessed log file initialized.')

    @staticmethod
    def __append_to_log_file(log_data):
        with open(LOG_FILE_PATH, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(log_data)

    def __extract_process_data(self):
        f = LogFile(self.log_path)
        status = 'FAILED' if f.is_failed() else 'SUCCESS'
        maps_time = f.get_maps_time_ms()
        reduces_time = f.get_reduces_time()
        total_time = maps_time + reduces_time
        errors = f.get_errors()

        return status, total_time, maps_time, reduces_time, errors


def log(content, level='INFO'):
    current_dt = datetime.now()
    print(f'[{current_dt}: {APP_NAME}] {level}: {content}')


if __name__ == '__main__':

    APP_NAME = 'LG_PRCSR'

    if len(argv) <= 2:
        log('Required parameters not supplied at position 1 and 2.', level='ERROR')
        exit(1)

    LOG_TO_PREPROCESS_PATH = argv[1]
    PROCESS_NAME = argv[2]

    LOG_FILE_LOCATION = os.getenv('HADOOP_LOGS_FILE_LOCATION', './hadoop_logs/')
    LOG_FILE_NAME = os.getenv('HADOOP_LOGS_FILE_NAME', 'hadoop_logs.csv')
    LOG_FILE_PATH = os.path.join(LOG_FILE_LOCATION, LOG_FILE_NAME)

    # log(f'Preprocessed logs file set to {LOG_FILE_PATH}.')

    m = LogManager(LOG_TO_PREPROCESS_PATH, PROCESS_NAME)
    m.append_log_data()
