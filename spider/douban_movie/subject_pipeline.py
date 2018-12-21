# coding=utf-8

from typing import Tuple
import logging
import scrapy
import core.pipeline as cp


class Item(scrapy.Item):
    MOVIE_ID_NAME = "movie_id"
    MOVIE_ID_INDEX = 0
    movie_id = scrapy.Field()

    TITLE_NAME = "title"
    TITLE_INDEX = 1
    title = scrapy.Field()

    DIRECTOR_NAME = "director"
    DIRECTOR_INDEX = 2
    director = scrapy.Field()

    AUTHOR_NAME = "author"
    AUTHOR_INDEX = 3
    author = scrapy.Field()

    ACTOR_NAME = "actor"
    ACTOR_INDEX = 4
    actor = scrapy.Field()

    REGION_NAME = "region"
    REGION_INDEX = 5
    region = scrapy.Field()

    LANG_NAME = "lang"
    LANG_INDEX = 6
    lang = scrapy.Field()

    GENRE_NAME = "genre"
    GENRE_INDEX = 7
    genre = scrapy.Field()

    RELEASE_NAME = "release"
    RELEASE_INDEX = 8
    release = scrapy.Field()

    EPISODE_NAME = "episode"
    EPISODE_INDEX = 9
    episode = scrapy.Field()

    DURATION_NAME = "duration"
    DURATION_INDEX = 10
    duration = scrapy.Field()

    RUNTIME_NAME = "runtime"
    RUNTIME_INDEX = 11
    runtime = scrapy.Field()

    AVERAGE_NAME = "average"
    AVERAGE_INDEX = 12
    average = scrapy.Field()

    VOTES_NAME = "votes"
    VOTES_INDEX = 13
    votes = scrapy.Field()


class Pipeline(cp.Pipeline):
    def __init__(self):
        super().__init__()

    def path(self, spider):
        data_path: str = spider.data_path
        application: str = spider.application

        return f"{data_path}/source/{application}.db"

    def table(self, spider):
        topic: str = spider.topic
        return f"{topic}"

    def create_sql_text(self, table: str) -> str:
        return (
            f"CREATE TABLE IF NOT EXISTS {table}("
            f"{Item.MOVIE_ID_NAME} INTEGER PRIMARY KEY, "
            f"{Item.TITLE_NAME} TEXT, "
            f"{Item.DIRECTOR_NAME} TEXT, "
            f"{Item.AUTHOR_NAME} TEXT, "
            f"{Item.ACTOR_NAME} TEXT, "
            f"{Item.REGION_NAME} TEXT, "
            f"{Item.LANG_NAME} TEXT, "
            f"{Item.GENRE_NAME} TEXT, "
            f"{Item.RELEASE_NAME} TEXT, "
            f"{Item.EPISODE_NAME} TEXT, "
            f"{Item.DURATION_NAME} TEXT, "
            f"{Item.RUNTIME_NAME} TEXT, "
            f"{Item.AVERAGE_NAME} TEXT, "
            f"{Item.VOTES_NAME} TEXT)"
        )

    def insert_sql_values(self, table: str, item: scrapy.Item) -> Tuple[str, Tuple]:
        movie_id = item.get(Item.MOVIE_ID_NAME, 0)
        title = item.get(Item.TITLE_NAME, "")
        director = item.get(Item.DIRECTOR_NAME, "")
        author = item.get(Item.AUTHOR_NAME, "")
        actor = item.get(Item.ACTOR_NAME, "")
        region = item.get(Item.REGION_NAME, "")
        lang = item.get(Item.LANG_NAME, "")
        genre = item.get(Item.GENRE_NAME, "")
        release = item.get(Item.RELEASE_NAME, "")
        episode = item.get(Item.EPISODE_NAME, "")
        duration = item.get(Item.DURATION_NAME, "")
        runtime = item.get(Item.RUNTIME_NAME, "")
        average = item.get(Item.AVERAGE_NAME, "")
        votes = item.get(Item.VOTES_NAME, "")

        log: str = (
            f"INSERT OR IGNORE INTO {table} VALUES("
            f"{movie_id}, '{title}', '{director}', '{author}', '{actor}', '{region}', '{lang}', "
            f"'{genre}', '{release}', '{episode}', '{duration}', '{runtime}', '{average}', '{votes}')"
        )
        logging.info(log)
        print(log, flush=True)

        return (
            f"INSERT OR IGNORE INTO {table} VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                movie_id, title, director, author, actor, region, lang,
                genre, release, episode, duration, runtime, average, votes
            )
        )
