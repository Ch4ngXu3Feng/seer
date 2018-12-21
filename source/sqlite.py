# coding=utf-8

from typing import List, Union

import time
import pandas as pd
import sqlite3 as sql

from core.source import DataSource


class SqliteDataSource(DataSource):
    def __init__(self, file_dir: str, file_name: str, table_name: str, encoding='utf-8') -> None:
        super().__init__(file_dir, file_name, encoding)

        self.__table_name: str = table_name
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

    def read(self, fields: List[str] = None, raw: bool=False) -> Union[pd.DataFrame, List]:
        if self.data is None:
            if fields:
                _fields = ", ".join(fields)
            else:
                _fields = "*"

            start = int(time.time())
            query = "SELECT %s FROM %s;" % (_fields, self.__table_name)
            if raw:
                cursor = self.__conn.cursor()
                cursor.execute(query)
                self.data = cursor.fetchall()
            else:
                self.data = pd.read_sql_query(query, self.__conn)
            end = int(time.time())
            print("sqlite read data time: ", end - start, len(self.data), flush=True)

        return self.data

    def write(self) -> None:
        self.data.to_sql(self.__table_name, self.__conn, if_exists="replace", index=False)

    def extension(self) -> str:
        return "sqlite"

    def mock(self) -> None:
        table = self.__table_name
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

        cur.executemany(
            f"INSERT INTO {table} VALUES(NULL, ?, ?, ?, ?)",
            [
                (1541692800, "bj", "red", 1),
                (1541692900, "sh", "red", 2),
                (1541693000, "bj", "red", 4),
                (1541693100, "zj", "blue", 4),
                (1541693200, "zj", "red", 8),
                (1541693300, "bj", "red", 19),
                (1541693400, "sh", "blue", 5),
                (1541693500, "sh", "red", 7),
            ]
        )

        self.__conn.commit()
