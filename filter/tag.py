# coding=utf-8

from typing import List, Dict
import pandas as pd

from core.filter import Filter


class TagFilter(Filter):
    def __init__(self) -> None:
        super().__init__()
        self.__tags: Dict[str, int] = dict()

    def add_tag(self, name: str, value: int) -> None:
        self.__tags[name] = value

    def filter(self, data: pd.DataFrame, series: pd.Series=None) -> pd.Series:
        result: pd.Series = series
        for name, value in self.__tags.items():
            current = data[name] == value
            if result is None:
                result = current
            result &= current

        return super().filter(data, result)
