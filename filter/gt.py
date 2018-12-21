# coding=utf-8

import pandas as pd

from core.filter import Filter


class GtFilter(Filter):
    def __init__(self) -> None:
        super().__init__()
        self.__name: str = ""
        self.__value: float = 0.0

    def set_value(self, name: str, value: float=-0.01) -> None:
        self.__name = name
        self.__value = value

    def filter(self, data: pd.DataFrame, series: pd.Series=None) -> pd.Series:
        current: pd.Series = data[self.__name] >= self.__value
        return super().filter(data, current if series is None else series & current)
