# coding=utf-8

from query.tag import TagQuery
from query.field import FieldQuery
from query.range import RangeQuery
from query.term import TermQuery
from query.visitor import DataVisitor
from .data import SeriesContext


class SeriesDataVisitor(DataVisitor):
    def __init__(self, context: SeriesContext) -> None:
        super().__init__()
        self.__context: SeriesContext = context

    def visit_tag(self, query: TagQuery) -> None:
        self.__context.tags.append((query.name, query.value))

    def visit_field(self, query: FieldQuery) -> None:
        self.__context.field_name = query.name
        self.__context.field_aggregation = query.aggregation

    def visit_range(self, query: RangeQuery) -> None:
        self.__context.range_name = query.name
        self.__context.range = query.range

    def visit_term(self, query: TermQuery) -> None:
        self.__context.terms.append(query.name)
