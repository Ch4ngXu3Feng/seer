# coding=utf-8

import pandas as pd
from pandas import DataFrame

from core.source import DataSource


class ExcelDataSource(DataSource):
    def __init__(self, file_dir: str, file_name: str, table: str, encoding="utf-8") -> None:
        super().__init__(file_dir, file_name, encoding=encoding)
        self.__table_name: str = table
        self.__data_path: str = self.path()

    def data(self) -> DataFrame:
        if self.df is None:
            self.df = pd.read_excel(self.__data_path, self.__table_name, encoding=self.encoding())
        print(self.df)
        return self.df

    def store(self) -> None:
        df: DataFrame = self.df
        if df is not None:
            df.to_excel(self.__data_path, encoding=self.encoding())

    def extension(self) -> str:
        return ".xlsx"
