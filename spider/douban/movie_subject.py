# coding=utf-8

import sqlite3 as sql
import scrapy


class Item(scrapy.Item):
    douban_id = scrapy.Field()
    name = scrapy.Field()


class Pipeline:
    def __init__(self):
        self.con = None
        self.cur = None

    def open_spider(self, spider):
        return
        self.con = sql.connect('data/subject.db')
        self.cur = self.con.cursor()
        self.cur.execute(
            (
                "CREATE TABLE IF NOT EXISTS subject("
                "id INTEGER PRIMARY KEY, "
                "movie_name TEXT, "
                "ranking INT, "
                "score FLOAT, "
                "score_num INTERGER)"
            )
        )

    def process_item(self, item, spider):
        return item
        movie_name = item['movie_name']
        ranking = item['ranking']
        score = item['score']
        score_num = item['score_num']

        sql_text = (
            f"INSERT INTO subject VALUES(NULL, '{movie_name}', {ranking}, {score}, {score_num})"
        )

        self.cur.execute(sql_text)
        self.con.commit()
        return item

    def close_spider(self, spider):
        if self.con is not None:
            self.con.close()
