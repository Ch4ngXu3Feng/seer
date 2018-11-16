# coding=utf-8

from core.query import DataQuery
from .visitor import DataVisitor


class FieldQuery(DataQuery):
    def __init__(self, name: str) -> None:
        super().__init__(name)

    def accept(self, visitor: DataVisitor) -> None:
        visitor.visit_field(self)
        super().accept(visitor)
