# coding=utf-8

from typing import List

import pandas as pd
from pandas import DataFrame
import sqlite3 as sql

from core.source import DataSource


class SqliteDataSource(DataSource):
    def __init__(self, file_dir: str, file_name: str, table: str, encoding='utf-8') -> None:
        super().__init__(file_dir, file_name, encoding)

        self.__table_name: str = table
        self.__data_path: str = self.path()
        self.__columns: List[str] = ["timestamp", "area", "color", "count"]
        self.__conn = sql.connect(self.__data_path)

    @property
    def table_name(self) -> str:
        return self.__table_name

    @table_name.setter
    def table_name(self, value: str) -> None:
        self.__table_name = value

    @property
    def data_path(self) -> str:
        return self.__data_path

    @data_path.setter
    def data_path(self, value: str) -> None:
        self.__data_path = value

    def data(self) -> DataFrame:
        if self.df is None:
            """
            query = "SELECT %s FROM %s;" % (
                ", ".join(self.__columns),
                self.__table_name
            )
            """
            query = "SELECT * FROM %s;" % self.__table_name
            self.df = pd.read_sql_query(query, self.__conn)
        return self.df

    """
    def columns(self) -> List[str]:
        return self.__columns
    """

    def mock(self) -> None:
        table_name = self.__table_name
        cur = self.__conn.cursor()
        cur.execute(
            (
                "CREATE TABLE IF NOT EXISTS %s("
                "id INTEGER PRIMARY KEY, "
                "timestamp INTERGER, "
                "area TEXT, "
                "color TEXT, "
                "count INT)"
            ) % self.__table_name
        )
        cur.execute("INSERT INTO %s VALUES(NULL, %d, '%s', '%s', %d)" % (table_name, 1541692800, "bj", "red", 1))
        cur.execute("INSERT INTO %s VALUES(NULL, %d, '%s', '%s', %d)" % (table_name, 1541692900, "sh", "red", 2))
        cur.execute("INSERT INTO %s VALUES(NULL, %d, '%s', '%s', %d)" % (table_name, 1541693000, "bj", "red", 4))
        cur.execute("INSERT INTO %s VALUES(NULL, %d, '%s', '%s', %d)" % (table_name, 1541693100, "zj", "blue", 4))
        cur.execute("INSERT INTO %s VALUES(NULL, %d, '%s', '%s', %d)" % (table_name, 1541693200, "zj", "red", 8))
        cur.execute("INSERT INTO %s VALUES(NULL, %d, '%s', '%s', %d)" % (table_name, 1541693300, "bj", "red", 19))
        cur.execute("INSERT INTO %s VALUES(NULL, %d, '%s', '%s', %d)" % (table_name, 1541693400, "sh", "blue", 5))
        cur.execute("INSERT INTO %s VALUES(NULL, %d, '%s', '%s', %d)" % (table_name, 1541693500, "sh", "red", 7))
        self.__conn.commit()

    def extension(self) -> str:
        return ".db"

    def store(self) -> None:
        return
