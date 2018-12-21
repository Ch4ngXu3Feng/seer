# coding=utf-8

from .drawing import Drawing
from .store import DataStore
from .source import DataSource


class Builder(object):
    def __init__(self) -> None:
        super().__init__()

    def load(self) -> None:
        raise NotImplementedError()

    def create_source(self, application: str, topic: str) -> DataSource:
        raise NotImplementedError()

    def create_store(self, application: str, topic: str) -> DataStore:
        raise NotImplementedError()

    def create_drawing(self, application: str, topic: str) -> Drawing:
        raise NotImplementedError()
