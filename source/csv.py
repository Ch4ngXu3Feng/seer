# coding=utf-8

from typing import List, Union

import pandas as pd
from pandas import DataFrame

from core.source import DataSource


class CsvDataSource(DataSource):
    def __init__(self, file_dir: str, file_name: str, table: str) -> None:
        super().__init__(file_dir, file_name)
        self.__table_name: str = table
        self.__data_path: str = self.path()

    def read(self, fields: List[str]=None, raw: bool=False) -> Union[DataFrame, List]:
        if self.data is None:
            self.data = pd.read_csv(self.__data_path)
        return self.data

    def write(self) -> None:
        _df: DataFrame = self.data
        if _df is not None:
            _df.to_csv(self.__data_path)

    def extension(self) -> str:
        return "csv"
