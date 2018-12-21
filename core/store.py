# coding=utf-8

from typing import Tuple, List
import pandas as pd


class DataStore(object):
    def __init__(self) -> None:
        super().__init__()
        self.__data: pd.DataFrame = None

    @property
    def data(self) -> pd.DataFrame:
        return self.__data

    @data.setter
    def data(self, value: pd.DataFrame) -> None:
        self.__data = value

    def key(self) -> str:
        raise NotImplementedError()

    def timestamp(self) -> str:
        raise NotImplementedError()

    def mappings(self) -> List[str]:
        raise NotImplementedError()

    def terms(self, name: str) -> List[str]:
        raise NotImplementedError()

    def columns(self) -> List[str]:
        raise NotImplementedError()

    def view_map(self) -> List[Tuple[str, int]]:
        raise NotImplementedError()
