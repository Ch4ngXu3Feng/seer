# coding=utf-8

from typing import Tuple, Any

import sqlite3 as sql
import scrapy


class Pipeline(object):
    def __init__(self):
        super().__init__()

        self.connect = None
        self.cursor = None

    def create_sql_text(self, table: str) -> str:
        raise NotImplementedError()

    def insert_sql_values(self, table: str, item: scrapy.Item) -> Tuple[str, Tuple]:
        raise NotImplementedError()

    def path(self, spider) -> str:
        raise NotImplementedError()

    def table(self, spider) -> str:
        raise NotImplementedError()

    def open_spider(self, spider) -> None:
        self.connect = sql.connect(self.path(spider))
        self.cursor = self.connect.cursor()
        self.cursor.execute(self.create_sql_text(self.table(spider)))

    def process_item(self, item, spider) -> Any:
        text, values = self.insert_sql_values(self.table(spider), item)
        self.cursor.execute(text, values)
        self.connect.commit()
        return item

    def close_spider(self, spider) -> None:
        _ = spider
        if self.connect is not None:
            self.connect.close()
