# coding=utf-8

from typing import Tuple
from core.query import DataQuery
from .visitor import DataVisitor


class RangeQuery(DataQuery):
    def __init__(self, name: str, value: Tuple[int, int]) -> None:
        super().__init__(name)
        self.__range: Tuple[int, int] = value

    def accept(self, visitor: DataVisitor) -> None:
        visitor.visit_range(self)
        super().accept(visitor)

    @property
    def range(self) -> Tuple[int, int]:
        return self.__range

    @range.setter
    def range(self, value: Tuple[int, int]) -> None:
        self.__range = value
