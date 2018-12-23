# coding=utf-8

from core.store import DataStore
from core.source import DataSource
from core.drawing import Drawing
from core.builder import Builder
from core.transformer import Transformer


class SeriesBuilder(Builder):
    """
    source load data time: 9(load by DataFrame)
    source load data time: 0(load by sqlite3)
    load data time: 4
    DataFrame time: 2 1770910
    load store: 1770910 14
    """

    def __init__(self, _dir: str) -> None:
        super().__init__()

        self.__dir: str = _dir
        self.__store: DataStore = None
        self.__drawing: Drawing = None
        self.__raw: DataSource = None
        self.__source: DataSource = None
        self.__transformer: Transformer = None

    def __load_douban_movie_raw(self) -> None:
        if self.__raw is None:
            _dir: str = self.__dir
            from source.sqlite import SqliteDataSource
            self.__raw = SqliteDataSource(
                f"{_dir}/raw", "douban_movie", "subject"
            )

    def __load_douban_movie_source(self) -> None:
        if self.__source is None:
            _dir: str = self.__dir
            from source.pickle import PickleDataSource
            self.__source = PickleDataSource(
                f"{_dir}/store", "douban_movie", "subject"
            )

    def __load_douban_movie_store(self) -> None:
        if self.__store is None:
            from douban_movie.subject_store import MovieSubjectDataStore
            self.__store = MovieSubjectDataStore()

    def load(self):
        self.__load_douban_movie_source()
        self.__load_douban_movie_store()
        self.__store.data = self.__source.read()

    def create_source(self, application: str, topic: str, raw: bool=False):
        # _source: DataSource = None

        if (application.find("douban_movie") != -1 and
                topic.find("subject") != -1):
            if raw:
                self.__load_douban_movie_raw()
                _source = self.__raw
            else:
                self.__load_douban_movie_source()
                _source = self.__source

        else:
            raise NotImplementedError()

        return _source

    def create_store(self, application: str, topic: str) -> DataStore:
        # store: DataStore = None
        file_name = application
        table_name = topic

        if (file_name.find("douban_movie") != -1 and
                table_name.find("subject") != -1):
            self.__load_douban_movie_store()
            store = self.__store

        else:
            raise NotImplementedError()

        return store

    def create_drawing(self, application: str, topic: str) -> Drawing:
        from drawing.plotly import PlotlyDrawing
        self.__drawing = PlotlyDrawing()
        return self.__drawing

    def create_transformer(self, application: str, topic: str) -> Transformer:
        if (application.find("douban_movie") != -1 and
                topic.find("subject") != -1):
            from douban_movie.subject_transformer import MovieSubjectTransformer
            self.__transformer = MovieSubjectTransformer()
        else:
            raise NotImplementedError()
        return self.__transformer
