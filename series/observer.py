# coding=utf-8

from typing import Optional

import pandas as pd

from core.aggregator import Aggregator
from core.observer import AggregatorObserver

from aggregator.mean import MeanFieldAggregator
from aggregator.sum import SumFieldAggregator
from aggregator.box import BoxFieldAggregator

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .data import SeriesData


class SdAggregatorObserver(AggregatorObserver):
    def __init__(self, sd: 'SeriesData') -> None:
        super().__init__()
        self.__sd: 'SeriesData' = sd

    @property
    def aggregator(self) -> Optional[Aggregator]:
        return self.__sd.aggregator

    @aggregator.setter
    def aggregator(self, value: Aggregator) -> None:
        self.__sd.aggregator = value

    def update_data_frame(self, name: str, data: pd.DataFrame) -> None:
        drawing = self.__sd.drawing
        aggregator = self.__sd.aggregator
        if isinstance(aggregator, (SumFieldAggregator, MeanFieldAggregator)):
            d1 = dict()
            for name, value in data.items():
                t, ts = name
                d2 = d1.get(t)
                if d2 is None:
                    d2 = dict()
                    d1[t] = d2
                d2[ts] = value

            for name, __data in d1.items():
                drawing.add_scatter(name, __data)

        elif isinstance(aggregator, BoxFieldAggregator):
            drawing.add_box(name, data.values)

        else:
            raise NotImplementedError()

    def update_series(self, _name: str, series: pd.Series) -> None:
        drawing = self.__sd.drawing

        d1 = dict()
        for name, value in series.to_dict().items():
            t, ts = name
            d2 = d1.get(t)
            if d2 is None:
                d2 = dict()
                d1[t] = d2
            d2[ts] = value

        for name, data in d1.items():
            drawing.add_scatter(name, data)
