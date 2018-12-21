# coding=utf-8

from typing import List, Union

import pandas as pd
from pandas import DataFrame

from core.source import DataSource


class ExcelDataSource(DataSource):
    def __init__(self, file_dir: str, file_name: str, table: str, encoding="utf-8") -> None:
        super().__init__(file_dir, file_name, encoding=encoding)
        self.__table_name: str = table
        self.__data_path: str = self.path()

    def read(self, fields: List[str]=None, raw: bool=False) -> Union[DataFrame, List]:
        if self.data is None:
            self.data = pd.read_excel(self.__data_path, self.__table_name, encoding=self.encoding())
        print(self.data, flush=True)
        return self.data

    def write(self) -> None:
        df: DataFrame = self.data
        if df is not None:
            df.to_excel(self.__data_path, encoding=self.encoding())

    def extension(self) -> str:
        return "xlsx"
