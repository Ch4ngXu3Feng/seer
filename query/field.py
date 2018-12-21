# coding=utf-8

from core.query import DataQuery
from core.visitor import DataVisitor


class FieldQuery(DataQuery):
    def __init__(self, name: str, aggregation: str) -> None:
        super().__init__(name)
        self.__aggregation: str = aggregation

    def accept(self, visitor: DataVisitor) -> None:
        visitor.visit_field(self)
        super().accept(visitor)

    @property
    def aggregation(self) -> str:
        return self.__aggregation

    @aggregation.setter
    def aggregation(self, value: str)-> None:
        self.__aggregation = value
