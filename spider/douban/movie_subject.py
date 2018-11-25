# coding=utf-8

import logging
import sqlite3 as sql
import scrapy


class Item(scrapy.Item):
    movie_id = scrapy.Field()
    title = scrapy.Field()
    director = scrapy.Field()
    author = scrapy.Field()
    actor = scrapy.Field()
    region = scrapy.Field()
    lang = scrapy.Field()
    genre = scrapy.Field()
    release = scrapy.Field()
    episode = scrapy.Field()
    duration = scrapy.Field()
    runtime = scrapy.Field()
    average = scrapy.Field()
    votes = scrapy.Field()


class Pipeline:
    def __init__(self):
        self.con = None
        self.cur = None

    def open_spider(self, spider):
        path = spider.path
        table = spider.table

        self.con = sql.connect(path)
        self.cur = self.con.cursor()
        self.cur.execute(
            (
                f"CREATE TABLE IF NOT EXISTS {table}("
                f"movie_id INTEGER PRIMARY KEY, "
                f"title TEXT, "
                f"director TEXT, "
                f"author TEXT, "
                f"actor TEXT, "
                f"region TEXT, "
                f"lang TEXT, "
                f"genre TEXT, "
                f"release TEXT, "
                f"episode TEXT, "
                f"duration TEXT, "
                f"runtime TEXT, "
                f"average TEXT, "
                f"votes TEXT)"
            )
        )

    def process_item(self, item, spider):
        table = spider.table

        movie_id = item.get('movie_id', 0)
        title = item.get('title', "")
        director = item.get('director', "")
        author = item.get('author', "")
        actor = item.get('actor', "")
        region = item.get('region', "")
        lang = item.get('lang', "")
        genre = item.get('genre', "")
        release = item.get('release', "")
        episode = item.get('episode', "")
        duration = item.get('duration', "")
        runtime = item.get('runtime', "")
        average = item.get('average', "")
        votes = item.get('votes', "")

        log: str = (
            f"INSERT OR IGNORE INTO {table} VALUES("
            f"{movie_id}, '{title}', '{director}', '{author}', '{actor}', '{region}', '{lang}', "
            f"'{genre}', '{release}', '{episode}', '{duration}', '{runtime}', '{average}', '{votes}')"
        )
        logging.info(log)
        print(log)

        self.cur.execute(
            f"INSERT OR IGNORE INTO {table} VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                movie_id, title, director, author, actor, region, lang,
                genre, release, episode, duration, runtime, average, votes
            )
        )
        self.con.commit()

        return item

    def close_spider(self, spider):
        _ = spider
        if self.con is not None:
            self.con.close()
