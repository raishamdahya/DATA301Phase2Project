import json
import logging
import os
import re
from enum import Enum

import pandas as pd

pd.set_option('display.max_columns', 500)


class Component(Enum):
    DEMOGRAPHICS = 'Demographics'
    DIETARY = 'Dietary'
    EXAMINATION = 'Examination'
    LABORATORY = 'Laboratory'
    QUESTIONAIRE = 'Questionnaire'


class ParseNHANES:
    def __init__(self, data_dir: str, out_dir: str = 'data'):
        self.data_dir = data_dir
        self.out_dir = out_dir
        self.logger = self.__create_logger()

    def __create_logger(self):
        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(logging.INFO)

        log_format = logging.Formatter("%(asctime)s [%(name)s] %(levelname)s: %(message)s", '%Y-%m-%d %H:%M:%S')
        ch = logging.StreamHandler()
        ch.setFormatter(log_format)
        logger.addHandler(ch)

        return logger

    def export_component(self, component: Component, expr=r'.*.XPT', columns=None):
        df, meta = self.get_component(component, expr, columns)
        self.__export(df, meta, self.out_dir, Component.DEMOGRAPHICS.value)

    def get_component(self, component: Component, expr=r'.*.XPT', columns=None):
        self.logger.info(f"Getting {component}")

        df = pd.DataFrame()
        meta = dict()
        for xpt_file, json_file in self.__get_files(component, expr):
            data = self.read_json(json_file)
            for key, value in [key for key in data.items() if key not in meta]:
                meta[key] = value

            df_this = pd.read_sas(xpt_file)
            df_this.columns = [column.upper() for column in df_this.columns]
            df_this['year'] = os.path.basename(os.path.split(xpt_file)[0])

            df = pd.concat([df, df_this])

        if columns:
            meta = {key: meta[key] for key in columns}
            columns_lower = [column.lower() for column in columns]

            if 'year' not in columns_lower:
                columns.append('year')
            if 'seqn' not in columns_lower:
                columns.append('SEQN')

            df = df[columns]

        df['SEQN'] = df['SEQN'].astype(int)
        return df, meta

    def __get_files(self, component: Component, expr):
        self.logger.debug(f"Getting files from {component}")

        json_files, xpt_files = [], []
        component_path = os.path.join(self.data_dir, component.value)
        for year in os.listdir(component_path):
            directory = os.path.join(component_path, year)
            for file in [file for file in os.listdir(directory) if re.match(expr, file)]:
                file_abs = os.path.join(directory, file)
                df_this = pd.read_sas(file_abs)
                if 'SEQN' in df_this.columns:
                    json_files.append(os.path.splitext(file_abs)[0] + '.JSON')
                    xpt_files.append(file_abs)
                else:
                    self.logger.debug(f"Column SEQN not in file:\n\t{file_abs}")

        return zip(xpt_files, json_files)

    def __export(self, df, meta, filepath: str, filename: str):
        os.makedirs(filepath, exist_ok=True)
        pkl_file = os.path.join(filepath, filename + '.pkl')
        json_file = os.path.join(filepath, filename + '.JSON')

        self.export_pkl(df, pkl_file)
        self.export_json(meta, json_file)

    @staticmethod
    def export_pkl(df: pd.DataFrame, file: str):
        df.to_pickle(file)

    @staticmethod
    def export_json(data: dict, file: str):
        with open(file, 'w') as fp:
            json.dump(data, fp, indent=4)

    @staticmethod
    def read_json(json_file: str):
        f = open(os.path.splitext(json_file)[0] + '.JSON')
        data = json.load(f)
        f.close()

        return data

    @staticmethod
    def read_pkl(pkl_file: str):
        return pd.read_pickle(pkl_file)


if __name__ == '__main__':
    nhanes = ParseNHANES('data')
    # nhanes.export_demographics()
    nhanes.get_component(Component.DEMOGRAPHICS)
