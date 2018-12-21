# coding=utf-8

from typing import Type, Any


class Adapter(object):
    def __init__(self) -> None:
        super().__init__()

    def adapter(self, clazz: Type[Any]) -> Any:
        raise NotImplementedError()
