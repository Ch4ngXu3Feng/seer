# coding=utf-8

from typing import List, Tuple, Dict, Callable
import time

from pandas import DataFrame

from core.transformer import Transformer
from core.source import DataSource
from core.store import DataStore

from spider.douban_movie.subject_pipeline import Item


class MovieSubjectTransformer(Transformer):
    def __init__(self):
        super().__init__()

        self.__parsers = {
            Item.MOVIE_ID_NAME: self.__parse_movie_id,
            Item.RELEASE_NAME: self.__parse_release,
            Item.DIRECTOR_NAME: self.__parse_director,
            Item.AUTHOR_NAME: self.__parse_author,
            Item.ACTOR_NAME: self.__parse_actor,
            Item.REGION_NAME: self.__parse_region,
            Item.LANG_NAME: self.__parse_lang,
            Item.GENRE_NAME: self.__parse_genre,
            Item.AVERAGE_NAME: self.__parse_average,
            Item.VOTES_NAME: self.__parse_votes,
        }

        self.__column_data: List[int, int, Callable] = None
        self.__view_data: List[Tuple[str, List]] = None
        self.__values: List = None

    def transform(self, src: DataSource, dst: DataSource, store: DataStore) -> None:
        self.__values = list()

        views: Dict[str, Tuple[str, int]] = store.view_map()
        columns: List[str] = store.columns()

        self.__column_data = [
            (index, views[name][1], self.__parsers[name])
            for index, name in enumerate(columns)
        ]

        self.__view_data: List[Tuple[str, List]] = [None] * len(views)

        for _, view in views.items():
            name: str = view[0]
            index: int = view[1]
            self.__view_data[index] = (name, list())

        _data = src.read(columns, True)

        start = int(time.time())
        for record in _data:
            self.__transform_record(record)
        end = int(time.time())
        print("load data time: ", end - start, flush=True)

        for vd in self.__view_data:
            print(vd[0], len(vd[-1]))

        start = int(time.time())
        df = DataFrame({item[0]: item[-1] for item in self.__view_data})
        end = int(time.time())
        print("DataFrame time: ", end - start, len(self.__view_data[0]),
              df[store.key()].count(), flush=True)

        dst.data = df
        dst.write()

    def __insert_all_value(self):
        for __value in self.__values:
            self.__view_data[__value[0]][-1].append(__value[1])

    def __insert_value(self, record, index, value):
        self.__values.append(value)
        self.__transform_record(record, index)
        self.__values.pop()

    def __transform_record(self, record: Tuple, column_index: int=0) -> None:
        if column_index < len(self.__column_data):
            index, view_index, parse = self.__column_data[column_index]
            value = parse(record[index])

            if isinstance(value, list):
                for _value in value:
                    self.__insert_value(record, column_index + 1, (view_index, _value))

            else:
                self.__insert_value(record, column_index + 1, (view_index, value))

        else:
            self.__insert_all_value()

    @staticmethod
    def __parse_movie_id(item) -> int:
        if item:
            movie_id = int(item)
        else:
            movie_id = -1
        return movie_id

    @staticmethod
    def __parse_release(item: str) -> int:
        value = item.split("-")[0].split("(")[0]
        if value:
            date = int(value.strip())
        else:
            date = -1
        return date

    @staticmethod
    def __parse_director(item: str) -> List[str]:
        return [value.strip() for value in item.split("/")]

    @staticmethod
    def __parse_author(item: str) -> List[str]:
        return [value.strip() for value in item.split("/")]

    @staticmethod
    def __parse_actor(item: str) -> List[str]:
        return [value.strip() for value in item.split("/")]

    @staticmethod
    def __parse_region(item: str) -> List[str]:
        return [value.strip() for value in item.split("/")]

    @staticmethod
    def __parse_lang(item: str) -> List[str]:
        return [value.strip() for value in item.split("/")]

    @staticmethod
    def __parse_genre(item: str) -> List[str]:
        return [value.strip() for value in item.split("/")]

    @staticmethod
    def __parse_average(item: str) -> float:
        if item:
            average = float(item.strip())
        else:
            average = -1.0

        return average

    @staticmethod
    def __parse_votes(item: str) -> List[str]:
        return [value.strip() for value in item.split("/")]
