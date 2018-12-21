# coding=utf-8

from typing import List, Union

import time
import pandas as pd

from core.source import DataSource


class PickleDataSource(DataSource):
    def __init__(self, file_dir: str, file_name: str, table_name: str, encoding='utf-8') -> None:
        super().__init__(file_dir, file_name, encoding)
        self.__file_dir: str = file_dir
        self.__file_name: str = file_name
        self.__table_name: str = table_name
        self.__data_path: str = self.path()

    @property
    def table_name(self) -> str:
        return self.__table_name

    @table_name.setter
    def table_name(self, value: str) -> None:
        self.__table_name = value

    @property
    def data_path(self) -> str:
        return self.__data_path

    @data_path.setter
    def data_path(self, value: str) -> None:
        self.__data_path = value

    def path(self) -> str:
        file_dir: str = self.__file_dir
        file_name: str = self.__file_name
        table_name: str = self.__table_name
        extension: str = self.extension()
        return f"{file_dir}/{file_name}_{table_name}.{extension}"

    def read(self, fields: List[str]=None, raw: bool=False) -> Union[pd.DataFrame, List]:
        if self.data is None:
            start = int(time.time())
            self.data = pd.read_pickle(self.path())
            end = int(time.time())
            print("pickle read data time:", end - start)
        return self.data

    def write(self) -> None:
        if self.data is not None:
            start = int(time.time())
            self.data.to_pickle(self.path())
            end = int(time.time())
            print("pickle write data time:", end - start)

    def extension(self) -> str:
        return "pickle"
