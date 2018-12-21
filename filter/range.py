# coding=utf-8

import pandas as pd
from core.filter import Filter


class RangeFilter(Filter):
    def __init__(self) -> None:
        super().__init__()
        self.__name: str = ""
        self.__start: int = -1
        self.__end: int = -1

    def set_range(self, name: str, start: int, end: int) -> None:
        self.__name = name
        self.__start = start
        self.__end = end

    def filter(self, data: pd.DataFrame, series: pd.Series=None) -> pd.Series:
        current: pd.Series = (data[self.__name] >= self.__start) & (data[self.__name] <= self.__end)
        return super().filter(data, current if series is None else series & current)
