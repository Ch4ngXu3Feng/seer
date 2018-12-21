# coding=utf-8

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from query.tag import TagQuery
    from query.field import FieldQuery
    from query.range import RangeQuery
    from query.term import TermQuery


class DataVisitor(object):
    def __init__(self) -> None:
        super().__init__()

    def visit_tag(self, query: 'TagQuery') -> None:
        raise NotImplementedError()

    def visit_field(self, query: 'FieldQuery') -> None:
        raise NotImplementedError()

    def visit_range(self, query: 'RangeQuery') -> None:
        raise NotImplementedError()

    def visit_term(self, query: 'TermQuery') -> None:
        raise NotImplementedError()

    def finish(self) -> None:
        raise NotImplementedError()
