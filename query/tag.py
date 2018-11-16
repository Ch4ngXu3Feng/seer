# coding=utf-8

from typing import Union

from core.query import DataQuery
from .visitor import DataVisitor


class TagQuery(DataQuery):
    def __init__(self, name: str, value: Union[str, int, float]) -> None:
        super().__init__(name)
        self.__value: Union[str, int, float] = value

    def accept(self, visitor: DataVisitor) -> None:
        visitor.visit_tag(self)
        super().accept(visitor)

    @property
    def value(self) -> Union[str, int, float]:
        return self.__value

    @value.setter
    def value(self, value: Union[str, int, float]) -> None:
        self.__value = value
