# coding=utf-8

from pandas import DataFrame

from core.source import DataSource
from core.store import DataStore


class Transformer(object):
    def __init__(self) -> None:
        super().__init__()

    def transform(self, src: DataSource, dst: DataSource, store: DataStore) -> DataFrame:
        raise NotImplementedError()
