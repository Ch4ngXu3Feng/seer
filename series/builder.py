# coding=utf-8

from core.schema import Schema
from core.drawing import Drawing
from core.builder import Builder


class SeriesBuilder(Builder):
    def __init__(self, dir: str, file_name: str, table_name: str) -> None:
        super().__init__()
        self.__dir: str = dir
        self.__file_name: str = file_name
        self.__table_name: str = table_name
        self.__schema: Schema = None
        self.__drawing: Drawing = None

    def create_schema(self) -> Schema:
        if self.__schema is None:
            if self.__file_name.find("douban") != -1:
                from schema.douban import MovieSubjectSchema
                self.__schema = MovieSubjectSchema(
                    self.__file_name, self.__table_name
                )

                from source.sqlite3 import SqliteDataSource
                self.__schema.source = SqliteDataSource(
                    "data", "douban_movie_tag", "subject"
                )

        return self.__schema

    def create_drawing(self) -> Drawing:
        if self.__drawing is None:
            from drawing.plotly import PlotlyDrawing
            self.__drawing = PlotlyDrawing()
        return self.__drawing
