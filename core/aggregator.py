# coding=utf-8

from typing import List, Optional
import pandas as pd

from .filter import Filter
from .observer import AggregatorObserver


class Aggregator(object):
    def __init__(self, field: str, term: str) -> None:
        super().__init__()

        self.__filter: Optional[Filter] = None
        self.__terms: List[str] = [term] if term else list()
        self.__field: str = field
        self.__observers: List[AggregatorObserver] = list()

    def add_filter(self, _filter: Filter) -> None:
        if _filter is not None:
            self.__filter = _filter.add_next(self.__filter)

    def add_terms(self, terms: List[str]) -> None:
        if terms:
            self.__terms = terms + self.__terms

    def method(self, name: str, data: pd.DataFrame) -> None:
        raise NotImplementedError()

    def filter(self, data: pd.DataFrame) -> pd.DataFrame:
        return data[self.__filter.filter(data)] if self.__filter is not None else data

    def split(self, data: pd.DataFrame) -> pd.DataFrame:
        return data[self.__field]

    def aggregate(self, name: str, data: pd.DataFrame) -> None:
        if self.__terms:
            data = data.groupby(self.__terms)
        self.method(name, self.split(data))

    def attach(self, observer: AggregatorObserver) -> None:
        self.__observers.append(observer)

    def notify_data_frame(self, name: str, data: pd.DataFrame) -> None:
        for observer in self.__observers:
            observer.update_data_frame(name, data)

    def notify_series(self, name: str, series: pd.Series) -> None:
        for observer in self.__observers:
            observer.update_series(name, series)
