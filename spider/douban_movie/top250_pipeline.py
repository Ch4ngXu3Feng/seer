# coding=utf-8

import sqlite3 as sql
import scrapy


class Item(scrapy.Item):
    ranking = scrapy.Field()
    movie_name = scrapy.Field()
    score = scrapy.Field()
    score_num = scrapy.Field()


# FIXME
class Pipeline(object):
    def __init__(self):
        super().__init__()

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
                f"id INTEGER PRIMARY KEY, "
                f"movie_name TEXT, "
                f"ranking INT, "
                f"score FLOAT, "
                f"score_num INTERGER)"
            )
        )

    def process_item(self, item, spider):
        table = spider.table

        movie_name = item['movie_name']
        ranking = item['ranking']
        score = item['score']
        score_num = item['score_num']

        print(
            (
                f"INSERT OR IGNORE INTO {table} "
                f"VALUES(NULL, '{movie_name}', {ranking}, {score}, {score_num})"
            ),
            flush=True
        )

        self.cur.execute(
            f"INSERT OR IGNORE INTO {table} VALUES(NULL, ?, ?, ?, ?)",
            (movie_name, ranking, score, score_num)
        )
        self.con.commit()
        return item

    def close_spider(self, spider):
        _ = spider
        if self.con is not None:
            self.con.close()
