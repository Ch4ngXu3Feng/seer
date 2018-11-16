# coding=utf-8

from typing import List, Dict, Tuple

from core.source import DataSource
from core.drawing import Drawing


class SeriesContext:
    def __init__(self):
        self.range_name: str = ""
        self.range: Tuple[int, int] = tuple()
        self.tags: List[Tuple[str, str]] = list()
        self.field_name: str = ""
        self.terms: List[str] = list()
        self.term_name: str = ""
        self.data: Dict[int, int] = dict()


class SeriesData:
    def __init__(self, source: DataSource, drawing: Drawing) -> None:
        super().__init__()

        self.__context = SeriesContext()
        self.__drawing: Drawing = drawing
        self.__source: DataSource = source

    def context(self) -> SeriesContext:
        return self.__context

    def __filter(self, data) -> bool:
        if "filter range":
            if self.__context.range_name:
                _from, _to = self.__context.range
                value = data[self.__context.range_name]
                if not _from <= value < _to:
                    return False

        if "filter tag":
            for tag in self.__context.tags:
                value1 = tag[1]
                value2 = data[tag[0]]
                if value1 != value2:
                    return False

        return True

    def __add_value(self, name: int, value: int) -> None:
        _value = self.__context.data.get(name, 0)
        _value += value
        self.__context.data[name] = _value

    def __process(self, data):
        if self.__filter(data):
            point: int = data[self.__context.term_name]
            value: int = data[self.__context.field_name]
            self.__add_value(point, value)
        return None

    def __name(self) -> str:
        return "-".join([x[1] for x in self.__context.tags])

    def render(self) -> None:
        self.__source.data().apply(self.__process, axis=1)
        self.__drawing.add_trace(self.__name(), self.__context.data)
        self.__drawing.draw()
