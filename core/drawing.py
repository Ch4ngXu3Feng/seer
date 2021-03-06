# coding=utf-8

from typing import Dict, Type
from numpy import ndarray


class Drawing(object):
    def __init__(self) -> None:
        super().__init__()

    def __len__(self) -> int:
        raise NotImplementedError()

    def add_scatter(self, name: str, data: Dict[int, int]) -> None:
        raise NotImplementedError()

    def add_box(self, name: str, data: Type[ndarray]) -> None:
        raise NotImplementedError()

    def draw(self) -> str:
        raise NotImplementedError()
