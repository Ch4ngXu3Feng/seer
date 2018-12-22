# coding=utf-8

from typing import Any, Type, Optional
import time
import pandas as pd
import logging

from core.builder import Builder
from core.store import DataStore
from core.drawing import Drawing
from core.adapter import Adapter
from core.aggregator import Aggregator
from core.observer import AggregatorObserver

from .observer import SdAggregatorObserver


class SeriesData(Adapter):
    def __init__(self, application: str, topic: str, builder: Builder) -> None:
        super().__init__()

        self.__builder: Builder = builder
        self.__store: DataStore = builder.create_store(application, topic)
        self.__drawing: Drawing = builder.create_drawing(application, topic)
        self.__aggregator: Optional[Aggregator] = None

    @property
    def drawing(self) -> Drawing:
        return self.__drawing

    @property
    def aggregator(self) -> Optional[Aggregator]:
        return self.__aggregator

    @aggregator.setter
    def aggregator(self, value: Aggregator) -> None:
        self.__aggregator = value

    def adapter(self, clazz: Type[Any]) -> Any:
        if clazz is AggregatorObserver:
            return SdAggregatorObserver(self)
        elif clazz is pd.DataFrame:
            return self.__store.data
        elif clazz is DataStore:
            return self.__store
        else:
            return None

    def render(self) -> str:
        html: str = ""
        start = int(time.time())
        if len(self.__drawing):
            html = self.__drawing.draw()
        end = int(time.time())
        logging.info("drawing scatter: %d", end - start)
        return html
