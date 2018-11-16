# coding=utf-8

from typing import Dict


class Drawing:
    def __init__(self) -> None:
        super().__init__()

    def add_trace(self, name: str, data: Dict[int, int]) -> None:
        raise NotImplementedError

    def draw(self):
        raise NotImplementedError
