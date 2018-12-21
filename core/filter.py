# coding=utf-8

import pandas as pd


class Filter(object):
    def __init__(self) -> None:
        super().__init__()
        self.__next: Filter = None

    def filter(self, data: pd.DataFrame, series: pd.Series=None) -> pd.Series:
        if self.__next is not None:
            return self.__next.filter(data, series)
        else:
            return series

    def add_next(self, _filter: 'Filter') -> 'Filter':
        if _filter is not None:
            self.__next = _filter
        return self
