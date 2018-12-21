# coding=utf-8

import pandas as pd

from core.aggregator import Aggregator
from core.observer import AggregatorObserver

from .mean import MeanFieldAggregator
from .group import GroupFieldAggregator


class BoxFieldAggregator(Aggregator):
    NAME: str = "box"

    def __init__(self, key: str, field: str, term: str) -> None:
        super().__init__(field, term)

        self.__aggregator: Aggregator = MeanFieldAggregator(field, key)
        self.__group = GroupFieldAggregator(
            self.__aggregator, key, field, term
        )

    def method(self, name: str, data: pd.DataFrame) -> None:
        raise NotImplementedError()

    def attach(self, observer: AggregatorObserver) -> None:
        self.__aggregator.attach(observer)

    def aggregate(self, name: str, data: pd.DataFrame) -> None:
        self.__group.aggregate(name, data)
