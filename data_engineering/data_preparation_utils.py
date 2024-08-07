"""Data preparation methods that could be useful in this and other projects."""

import os
from zipfile import ZipFile
import json

def download_kaggle_competition_dataset(credentials_path, competition_name, target_path):
    """
    Authenthicate to the kaggle api, download the requested dataset if not downloaded, and extract it.
    credentials_path example: './.kaggle/kaggle.json'
    competition_name example: 'store-sales-time-series-forecasting'
    target_path example: './dataset'
    """
    with open(credentials_path) as credentials_file:
        credentials_dict = json.load(credentials_file)
        os.environ['KAGGLE_USERNAME'] = credentials_dict['username']
        os.environ['KAGGLE_KEY'] = credentials_dict['key']
        import kaggle

    kaggle.api.competition_download_files(competition=competition_name, path=target_path, force=False, quiet=True)
    with ZipFile(f'{target_path}/{competition_name}.zip') as dataset_zip:
        dataset_zip.extractall(target_path)