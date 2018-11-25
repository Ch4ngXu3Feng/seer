# coding=utf-8

from typing import Tuple
import scrapy

from .source import DataSource

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from series.data import SeriesContext


class Schema:
    def __init__(self) -> None:
        super().__init__()
        self.source: DataSource = None

    def range_filter(self, item: scrapy.Item, range_name: str, range_value: Tuple[int, int]) -> bool:
        raise NotImplementedError

    def process_series(self, context: 'SeriesContext') -> None:
        raise NotImplementedError
