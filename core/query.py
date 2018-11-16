# coding=utf-8

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from query.visitor import DataVisitor


class DataQuery:
    def __init__(self, name: str) -> None:
        self.__next: DataQuery = None
        self.__name: str = name
        self.__text: str = ""

    def accept(self, visitor: 'DataVisitor') -> None:
        if self.__next:
            self.__next.accept(visitor)

    def add_query(self, query: 'DataQuery') -> 'DataQuery':
        if query is not None:
            self.__next = query
        return self

    @property
    def name(self) -> str:
        return self.__name

    @name.setter
    def name(self, value: str) -> None:
        self.__name = value

    @property
    def text(self) -> str:
        return self.__text

    @text.setter
    def text(self, value: str) -> None:
        self.__text = value
