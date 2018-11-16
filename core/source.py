# coding=utf-8

from typing import List, Optional, Union
from pandas import DataFrame
import numpy as np
from pandas.api.types import is_string_dtype


class DataSource:
    def __init__(self, file_dir: str, file_name: str, encoding: str="utf-8") -> None:
        super().__init__()
        self.__df: Optional[DataFrame] = None
        self.__file_dir: str = file_dir
        self.__file_name: str = file_name
        self.__encoding: str = encoding

    def encoding(self) -> str:
        return self.__encoding

    def clean(self) -> None:
        self.__df = None

    @property
    def df(self) -> Optional[DataFrame]:
        return self.__df

    @df.setter
    def df(self, value: DataFrame) -> None:
        self.__df = value

    def data(self) -> DataFrame:
        raise NotImplementedError

    def columns(self) -> List[str]:
        if self.__df is None:
            raise NotImplementedError
        return list(self.__df)

    def unique(self, name: str) -> List[Union[int, str]]:
        if self.__df is None:
            raise NotImplementedError
        return list(self.__df[name].unique())

    def is_text_column(self, name: str) -> bool:
        if self.__df is None:
            raise NotImplementedError
        return is_string_dtype(self.__df[name].dtype)

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
        self.__df = DataFrame(data, index=list(range(0, 8)))

    def store(self) -> None:
        raise NotImplementedError

    def extension(self) -> str:
        raise NotImplementedError

    def path(self) -> str:
        if not self.__file_name.endswith(self.extension()):
            self.__file_name += self.extension()
        return "%s/%s" % (self.__file_dir, self.__file_name)
