# coding=utf-8

from typing import Optional
import pandas as pd

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .aggregator import Aggregator


class AggregatorObserver(object):
    def __init__(self) -> None:
        super().__init__()

    @property
    def aggregator(self) -> Optional['Aggregator']:
        raise NotImplementedError()

    @aggregator.setter
    def aggregator(self, value: 'Aggregator') -> None:
        raise NotImplementedError()

    def update_data_frame(self, name: str, data: pd.DataFrame) -> None:
        raise NotImplementedError()

    def update_series(self, name: str, series: pd.Series) -> None:
        raise NotImplementedError()
