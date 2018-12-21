# coding=utf-8

from typing import Tuple
import logging
import scrapy
import core.pipeline as cp


# FIXME
class Item(scrapy.Item):
    MUSIC_ID_NAME = "music_id"
    music_id = scrapy.Field()


class Pipeline(cp.Pipeline):
    def __init__(self):
        super().__init__()

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
