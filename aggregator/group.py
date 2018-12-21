# coding=utf-8

import pandas as pd

from core.aggregator import Aggregator


class GroupFieldAggregator(Aggregator):
    def __init__(self, aggregator: Aggregator, key: str, field: str, term: str) -> None:
        super().__init__(field, term)

        self.__aggregator: Aggregator = aggregator
        self.__key: str = key
        self.__field: str = field
        self.__term: str = term

    def method(self, name: str, data: pd.DataFrame) -> None:
        raise RuntimeError()

    def aggregate(self, name: str, data: pd.DataFrame) -> None:
        for name, group in data.groupby(self.__term)[self.__key, self.__field]:
            self.__aggregator.aggregate(name, group)
