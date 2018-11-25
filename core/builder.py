# coding=utf-8

from .drawing import Drawing
from .schema import Schema


class Builder:
    def __init__(self):
        super().__init__()

    def create_schema(self) -> Schema:
        raise NotImplementedError

    def create_drawing(self) -> Drawing:
        raise NotImplementedError
