#!/bin/bash

export DATA_ACK_FILES_LOCATION="/tmp/hadoop/data/"
export DOWNLOAD_INFO_FILE_PATH="/tmp/hadoop/download_info.txt"

python3 ./data_acquisitor/data_acquisition.py
