# coding=utf-8

from typing import List, Optional

import pandas as pd

from core.adapter import Adapter
from core.visitor import DataVisitor
from core.aggregator import Aggregator
from core.observer import AggregatorObserver
from core.store import DataStore

from query.tag import TagQuery
from query.field import FieldQuery
from query.range import RangeQuery
from query.term import TermQuery

from filter.tag import TagFilter
from filter.range import RangeFilter
from filter.gt import GtFilter

from aggregator.count import CountFieldAggregator
from aggregator.sum import SumFieldAggregator
from aggregator.mean import MeanFieldAggregator
from aggregator.box import BoxFieldAggregator


class SeriesDataVisitor(DataVisitor):
    def __init__(self, context: Adapter) -> None:
        super().__init__()

        self.__context: Adapter = context
        self.__tag_filter: Optional[TagFilter] = None
        self.__range_filter: Optional[RangeFilter] = None
        self.__value_filter: Optional[GtFilter] = None
        self.__terms: List[str] = list()
        self.__aggregator: Aggregator = None

    def visit_tag(self, query: TagQuery) -> None:
        if self.__tag_filter is None:
            self.__tag_filter = TagFilter()
        self.__tag_filter.add_tag(query.name, query.value)

    def visit_range(self, query: RangeQuery) -> None:
        if self.__range_filter is None:
            self.__range_filter = RangeFilter()
        self.__range_filter.set_range(query.name, query.range[0], query.range[1])

    def visit_field(self, query: FieldQuery) -> None:
        agg: str = query.aggregation.lower()
        ao: AggregatorObserver = self.__context.adapter(AggregatorObserver)
        store: DataStore = self.__context.adapter(DataStore)

        if query.name:
            self.__value_filter = GtFilter()
            self.__value_filter.set_value(query.name)

        if agg == CountFieldAggregator.NAME:
            self.__aggregator = CountFieldAggregator(store.key(), store.timestamp())
            self.__aggregator.attach(ao)
            ao.aggregator = self.__aggregator

        elif agg == SumFieldAggregator.NAME:
            self.__aggregator = SumFieldAggregator(query.name, store.timestamp())
            self.__aggregator.attach(ao)
            ao.aggregator = self.__aggregator

        elif agg == MeanFieldAggregator.NAME:
            self.__aggregator = MeanFieldAggregator(query.name, store.timestamp())
            self.__aggregator.attach(ao)
            ao.aggregator = self.__aggregator

        elif agg == BoxFieldAggregator.NAME:
            self.__aggregator = BoxFieldAggregator(store.key(), query.name, store.timestamp())
            self.__aggregator.attach(ao)
            ao.aggregator = self.__aggregator

        else:
            raise NotImplementedError()

    def visit_term(self, query: TermQuery) -> None:
        self.__terms.append(query.name)

    def finish(self) -> None:
        if self.__aggregator is not None:
            self.__aggregator.add_filter(self.__value_filter)
            self.__aggregator.add_filter(self.__range_filter)
            self.__aggregator.add_filter(self.__tag_filter)

            self.__aggregator.add_terms(self.__terms)

            data = self.__context.adapter(pd.DataFrame)
            data = self.__aggregator.filter(data)
            self.__aggregator.aggregate("", data)
