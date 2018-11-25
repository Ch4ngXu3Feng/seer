# coding=utf-8

from typing import List, Dict, Tuple

from core.builder import Builder
from core.schema import Schema
from core.drawing import Drawing


class SeriesContext:
    def __init__(self):
        self.range_name: str = ""
        self.range: Tuple[int, int] = tuple()
        self.tags: List[Tuple[str, str]] = list()
        self.field_name: str = ""
        self.field_aggregation: str = ""
        self.terms: List[str] = list()
        self.data: Dict[str, Dict[int, int]] = dict()


class SeriesData:
    def __init__(self, builder: Builder) -> None:
        super().__init__()

        self.__context: SeriesContext = SeriesContext()
        self.__builder: builder = builder
        self.__drawing: Drawing = builder.create_drawing()
        self.__schema: Schema = builder.create_schema()

    def context(self) -> SeriesContext:
        return self.__context

    def render(self) -> str:
        self.__schema.process_series(self.__context)
        for name, data in self.__context.data.items():
            self.__drawing.add_trace(name, data)
        return self.__drawing.draw()
