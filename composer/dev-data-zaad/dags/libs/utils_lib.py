import logging
import os
import shutil
from urllib.request import urlretrieve

import pandas as pd


def delete_folder(folder_name):
    shutil.rmtree(folder_name)


def donwload_file(file_name, url):
    os.makedirs(os.path.dirname(file_name), exist_ok=True)
    saved_file = urlretrieve(url, file_name)
    logging.info(saved_file)


def transform_dict_to_csv(dict_data, csv_file):
    os.makedirs(os.path.dirname(csv_file), exist_ok=True)
    data_frame = pd.json_normalize(dict_data)
    data_frame.columns = [c.lower() for c in data_frame.columns]
    data_frame.to_csv(csv_file, index=False)


def transform_xlsx_to_csv(xlsx_file, csv_file):
    os.makedirs(os.path.dirname(csv_file), exist_ok=True)
    data_frame = pd.read_excel(xlsx_file)
    data_frame["csv_file"] = os.path.basename(csv_file)
    data_frame.columns = [c.lower() for c in data_frame.columns]
    data_frame.to_csv(csv_file, index=False)
