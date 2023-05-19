# Data acquisitor

Set DATA_ACK_FILES_LOCATION env variable to customize downloaded files directory
Set DOWNLOAD_INFO_FILE_PATH env variable to customize download_info log path

Arguments:
1. If set to init, all files will be force downloaded [optional]

Example usage:  
`export DATA_ACK_FILES_LOCATION="/custom/path/to/downloaded/files/"`  
`export DOWNLOAD_INFO_FILE_PATH="/custom/path/to/download_info.txt"`  
`python3 data_acquisition.py`

# Logs preprocessor  
Set HADOOP_LOGS_FILE_LOCATION env variable to specify directory where csv will be created
Set HADOOP_LOGS_FILE_NAME env variable to customize csv log name

Arguments:
1. Path to the log to preprocess [required]
2. Map-Reduce process name [required]

Example usage:  
`export HADOOP_LOGS_FILE_LOCATION="/custom/path/to/csv/log/"`  
`export HADOOP_LOGS_FILE_NAME="map_reduce_log.csv"`  
`python3 log_preprocessor.py /path/to/log/file/to/preprocess.log`