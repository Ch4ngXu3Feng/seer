# coding=utf-8

from typing import Optional, Dict, Any

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
            lines = dict()
            if self.__split_dict(data.to_dict(), lines):
                for name, __data in lines.items():
                    drawing.add_scatter(name, __data)
            else:
                drawing.add_scatter(name, lines)

        elif isinstance(aggregator, BoxFieldAggregator):
            drawing.add_box(name, data.values)

        else:
            raise NotImplementedError()

    def update_series(self, name: str, series: pd.Series) -> None:
        drawing = self.__sd.drawing
        lines = dict()
        if self.__split_dict(series.to_dict(), lines):
            for _name, _data in lines.items():
                drawing.add_scatter(_name, _data)
        else:
            drawing.add_scatter(name, lines)

    @staticmethod
    def __split_dict(data: Any, result: Any) -> bool:
        term = False
        for _name, _value in data.items():
            if isinstance(_name, tuple):
                first, second = _name
                _data = result.get(first)
                if _data is None:
                    _data = dict()
                    result[first] = _data
                _data[second] = _value
                term = True
            else:
                result[_name] = _value
                term = False
        return term
