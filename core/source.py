# coding=utf-8

from typing import List, Union
import numpy as np
from pandas import DataFrame
from pandas.api.types import is_string_dtype


class DataSource(object):
    def __init__(self, file_dir: str, file_name: str, encoding: str="utf-8") -> None:
        super().__init__()

        self.__data: Union[DataFrame, List] = None
        self.__file_dir: str = file_dir
        self.__file_name: str = file_name
        self.__encoding: str = encoding

    def encoding(self) -> str:
        return self.__encoding

    def clean(self) -> None:
        self.__data = None

    @property
    def data(self) -> Union[DataFrame, List]:
        return self.__data

    @data.setter
    def data(self, value: Union[DataFrame, List]) -> None:
        self.__data = value

    def read(self, fields: List[str]=None, raw: bool=False) -> Union[DataFrame, List]:
        raise NotImplementedError()

    def write(self) -> None:
        raise NotImplementedError()

    def columns(self) -> List[str]:
        if self.__data is None:
            raise NotImplementedError()
        return list(self.__data)

    def unique(self, name: str) -> List[Union[int, str]]:
        if self.__data is None:
            raise NotImplementedError()
        return list(self.__data[name].unique())

    def is_text_column(self, name: str) -> bool:
        if self.__data is None:
            raise NotImplementedError()
        return is_string_dtype(self.__data[name].dtype)

    def mock(self) -> None:
        data = {
            'timestamp': [
                1541692800, 1541692900, 1541693000, 1541693100,
                1541693200, 1541693300, 1541693400, 1541693500,
            ],
            'area': ['bj', 'sh', 'bj', 'zj', 'zj', 'bj', 'sh', 'sh'],
            'color': ['red', 'red', 'red', 'blue', 'red', 'red', 'blue', 'red'],
            'count': np.random.randn(8),
        }
        self.__data = DataFrame(data, index=list(range(0, 8)))

    def extension(self) -> str:
        raise NotImplementedError()

    def path(self) -> str:
        _dir: str = self.__file_dir
        _file: str = self.__file_name
        _extension: str = self.extension()
        return f"{_dir}/{_file}.{_extension}"
