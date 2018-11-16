# coding=utf-8

import pandas as pd
from pandas import DataFrame

from core.source import DataSource


class CsvDataSource(DataSource):
    def __init__(self, file_dir: str, file_name: str, table: str) -> None:
        super().__init__(file_dir, file_name)
        self.__table_name: str = table
        self.__data_path: str = self.path()

    def data(self) -> DataFrame:
        if self.df is None:
            self.df = pd.read_csv(self.__data_path)
        return self.df

    def store(self) -> None:
        _df: DataFrame = self.df
        if _df is not None:
            _df.to_csv(self.__data_path)

    def extension(self) -> str:
        return ".csv"
