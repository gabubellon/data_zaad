"""A compilation of Util functions to be used on other files
"""
import logging
import os
import shutil
from urllib.request import urlretrieve

import pandas as pd


def delete_folder(folder_name):
    """ Delete a folder and all files and folders inside

    Args:
        folder_name (str): Path to a folder to be deleted (/my/folder/path/exmaple)
    """
    shutil.rmtree(folder_name)


def donwload_file(file_name, url):
    """Download a url file to a local path

    Args:
        file_name (_type_): local path to file (/folder/file.txt)
        url (_type_): The url from file (http://www.filelin/file.txt)
    """
    os.makedirs(os.path.dirname(file_name), exist_ok=True)
    saved_file = urlretrieve(url, file_name)
    logging.info(saved_file)


def transform_dict_to_csv(dict_data, csv_file):
    """Read a dict to a pandas dataframe and save to a csv file

    Args:
        dict_data (dict): a dict with data to saved
        csv_file (_type_): csv files path (/folder/inside folder/csv.xlsx)
    """
    os.makedirs(os.path.dirname(csv_file), exist_ok=True)
    data_frame = pd.json_normalize(dict_data)
    data_frame.columns = [c.lower() for c in data_frame.columns]
    data_frame.to_csv(csv_file, index=False)


def transform_xlsx_to_csv(xlsx_file, csv_file):
    """Read a xlsx to a pandas dataframe and save to a csv file

    Args:
        xlsx_file (str): xlsx files path (/folder/inside folder/file.xlsx)
        csv_file (str): csv files path (/folder/inside folder/csv.xlsx)
    """
    os.makedirs(os.path.dirname(csv_file), exist_ok=True)
    data_frame = pd.read_excel(xlsx_file)
    data_frame["csv_file"] = os.path.basename(csv_file)
    data_frame.columns = [c.lower() for c in data_frame.columns]
    data_frame.to_csv(csv_file, index=False)
