# coding=utf-8

from typing import Dict, List, Tuple
import scrapy

from core.store import DataStore
from spider.douban_movie.subject_pipeline import Item


class MovieSubjectViewItem(scrapy.Item):
    MOVIE_ID_NAME: str = "movie_id"
    MOVIE_ID_INDEX: int = 0
    movie_id: scrapy.Field = scrapy.Field()

    DATE_NAME: str = "date"
    DATE_INDEX: int = 1
    date: scrapy.Field = scrapy.Field()

    DIRECTOR_NAME: str = "director"
    DIRECTOR_INDEX: int = 2
    director: scrapy.Field = scrapy.Field()

    AUTHOR_NAME: str = "author"
    AUTHOR_INDEX: int = 3
    author: scrapy.Field = scrapy.Field()

    ACTOR_NAME: str = "actor"
    ACTOR_INDEX: int = 4
    actor: scrapy.Field = scrapy.Field()

    REGION_NAME: str = "region"
    REGION_INDEX: int = 5
    region: scrapy.Field = scrapy.Field()

    LANG_NAME: str = "lang"
    LANG_INDEX: int = 6
    lang: scrapy.Field = scrapy.Field()

    GENRE_NAME: str = "genre"
    GENRE_INDEX: int = 7
    genre: scrapy.Field = scrapy.Field()

    EPISODE_NAME: str = "episode"
    episode: scrapy.Field = scrapy.Field()

    DURATION_NAME: str = "duration"
    duration: scrapy.Field = scrapy.Field()

    VOTES_NAME: str = "votes"
    VOTES_INDEX: int = 8
    votes: scrapy.Field = scrapy.Field()

    AVERAGE_NAME: str = "average"
    AVERAGE_INDEX: int = 9
    average: scrapy.Field = scrapy.Field()

    KEY_NAME: str = MOVIE_ID_NAME
    KEY_INDEX: int = MOVIE_ID_INDEX


class MovieSubjectDataStore(DataStore):
    def __init__(self):
        super().__init__()

        self.__columns: List[str] = [
            Item.MOVIE_ID_NAME,
            Item.DIRECTOR_NAME,
            Item.AUTHOR_NAME,
            Item.ACTOR_NAME,
            Item.REGION_NAME,
            Item.LANG_NAME,
            Item.GENRE_NAME,
            Item.RELEASE_NAME,
            Item.AVERAGE_NAME,
            Item.VOTES_NAME,
        ]

        self.__view_map: Dict[str, [Tuple[str, int]]] = {
            Item.MOVIE_ID_NAME: (MovieSubjectViewItem.MOVIE_ID_NAME, MovieSubjectViewItem.MOVIE_ID_INDEX),
            Item.RELEASE_NAME: (MovieSubjectViewItem.DATE_NAME, MovieSubjectViewItem.DATE_INDEX),
            Item.DIRECTOR_NAME: (MovieSubjectViewItem.DIRECTOR_NAME, MovieSubjectViewItem.DIRECTOR_INDEX),
            Item.AUTHOR_NAME: (MovieSubjectViewItem.AUTHOR_NAME, MovieSubjectViewItem.AUTHOR_INDEX),
            Item.ACTOR_NAME: (MovieSubjectViewItem.ACTOR_NAME, MovieSubjectViewItem.ACTOR_INDEX),
            Item.REGION_NAME: (MovieSubjectViewItem.REGION_NAME, MovieSubjectViewItem.REGION_INDEX),
            Item.LANG_NAME: (MovieSubjectViewItem.LANG_NAME, MovieSubjectViewItem.LANG_INDEX),
            Item.GENRE_NAME: (MovieSubjectViewItem.GENRE_NAME, MovieSubjectViewItem.GENRE_INDEX),
            Item.VOTES_NAME: (MovieSubjectViewItem.VOTES_NAME, MovieSubjectViewItem.VOTES_INDEX),
            Item.AVERAGE_NAME: (MovieSubjectViewItem.AVERAGE_NAME, MovieSubjectViewItem.AVERAGE_INDEX)
        }

        self.__tags: List[str] = [
            MovieSubjectViewItem.REGION_NAME,
            MovieSubjectViewItem.LANG_NAME,
            MovieSubjectViewItem.GENRE_NAME,
            MovieSubjectViewItem.DIRECTOR_NAME,
            MovieSubjectViewItem.AUTHOR_NAME,
            MovieSubjectViewItem.ACTOR_NAME,
        ]

        self.__fields: List[str] = [
            MovieSubjectViewItem.VOTES_NAME,
            MovieSubjectViewItem.AVERAGE_NAME,
        ]

        from aggregator.count import CountFieldAggregator
        from aggregator.sum import SumFieldAggregator
        from aggregator.mean import MeanFieldAggregator
        from aggregator.box import BoxFieldAggregator

        self.__aggregations: List[str] = [
            CountFieldAggregator.NAME,
            SumFieldAggregator.NAME,
            MeanFieldAggregator.NAME,
            BoxFieldAggregator.NAME,
        ]

    def key(self) -> str:
        return MovieSubjectViewItem.KEY_NAME

    def timestamp(self) -> str:
        return MovieSubjectViewItem.DATE_NAME

    def mappings(self) -> Dict[str, List[str]]:
        return {
            "fields": self.__fields,
            "tags": self.__tags,
            "aggregations": self.__aggregations,
        }

    def terms(self, name: str) -> List[str]:
        data: Dict = self.data[name].value_counts().to_dict()
        x = sorted(data, key=data.get, reverse=True)
        """
        x = sorted(data.keys())
        x = [value for value in sorted(data, key=data.get, reverse=False)]
        x = sorted(result.values())
        x = sorted(data.keys())
        y = [data[k] for k in x]
        """
        return x

    def columns(self) -> List[str]:
        return self.__columns

    def view_map(self) -> Dict[str, Tuple[str, int]]:
        return self.__view_map
