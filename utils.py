import json
from datetime import datetime
import logging

def read_json(file_path):
    with open(file_path, "r") as f:
        return json.load(f)
    
def unix_ms_to_datetime(unix_ms):
    return datetime.utcfromtimestamp(unix_ms / 1000.0)

def datetime_to_unix_ms(dt):
    return int(dt.timestamp() * 1000)

def set_logger(name, log_file, log_level='ERROR'):
    log_levels = {'INFO':logging.INFO,
                  'WARNING':logging.WARNING,
                  'DEBUG':logging.DEBUG,
                  'ERROR':logging.ERROR,
                  'CRITICAL':logging.CRITICAL,
                  'FATAL':logging.FATAL}

    logging.basicConfig(
    filename=f'log/{log_file}',
    encoding='utf-8', 
    level=log_levels[log_level],
    format='%(asctime)s %(levelname)-8s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')

    logger = logging.getLogger(name)
    
    return logger
    
def convert_to_int_or_str(value):
        if isinstance(value, str) and value.isdigit():
            return int(value)
        elif isinstance(value, int):
            return value
        else:
            return str(value)
        
def convert_from_deribit_date(date, as_string=True):
    # try:
    if as_string:
        return datetime.strptime(date, '%d%b%y').strftime('%Y-%m-%d')
    else:
        return datetime.strptime(date, '%d%b%y')
    # except:
    #     return date