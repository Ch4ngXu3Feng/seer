# coding=utf-8

import pandas as pd

from core.aggregator import Aggregator


class SumFieldAggregator(Aggregator):
    NAME: str = "sum"

    def __init__(self, field: str, term: str) -> None:
        super().__init__(field, term)

    def method(self, name: str, data: pd.DataFrame) -> None:
        self.notify_data_frame(name, data.sum())
