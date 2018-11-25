# coding=utf-8

from typing import List
import tornado.web

from core.query import DataQuery
from series.data import SeriesData
from series.visitor import SeriesDataVisitor


class View:
    def __init__(self):
        super().__init__()

    def test(self):
        from query.tag import TagQuery
        from query.field import FieldQuery
        from query.range import RangeQuery
        from query.term import TermQuery

        query: DataQuery = TagQuery("region", "中国").add_query(
            RangeQuery("date", (1990, 2019)).add_query(
                FieldQuery("score", "sum").add_query(
                    TermQuery("genre").add_query(
                        TermQuery("lang")
                    )
                )
            )
        )

        path = "douban_movie_tag.subject"
        schema = ""

        self.get(path, schema, query)
        #
        # for c in ds.columns():
        #    print(c, ds.unique(c))

    def get(self, path: str, schema_name: str, query: DataQuery) -> str:
        _path = path.split(".")
        file_name: str = _path[0]
        table_name: str = file_name
        if len(_path) > 1:
            table_name = _path[1]

        from series.builder import SeriesBuilder
        builder = SeriesBuilder("data", file_name, table_name)
        sd: SeriesData = SeriesData(builder)
        pdv: SeriesDataVisitor = SeriesDataVisitor(sd.context())
        query.accept(pdv)
        return sd.render()


class ViewHandler(tornado.web.RequestHandler):
    def data_received(self, chunk):
        pass

    def get(self, path: str, schema: str) -> None:
        from query.tag import TagQuery
        from query.field import FieldQuery
        from query.range import RangeQuery
        from query.term import TermQuery

        query: DataQuery = None
        view = View()

        _range: str = self.get_argument("range", "")
        if _range:
            __range: List[str] = _range.split(":")
            if len(__range) != 2:
                self.set_status(400)
                return

            range_name: str = __range[0]
            range_value: str = __range[1]
            _range_value = range_value.split(",")
            if len(_range_value) != 2:
                self.set_status(400)
                return

            # FIXME
            start: int = int(_range_value[0])
            end: int = int(_range_value[1])

            query = RangeQuery(range_name, (start, end))

        _tag: str = self.get_argument("tag", "")
        if _tag:
            __tag: List[str] = _tag.split(",")
            for item in __tag:
                _item = item.split(":")
                if len(_item) == 2:
                    name: str = _item[0]
                    value: str = _item[1]
                    query = TagQuery(name, value).add_query(query)

        _term: str = self.get_argument("term", "")
        if _term:
            __term: List[str] = _term.split(",")
            for item in __term:
                _item: str = item.split(",")
                for __item in _item:
                    query = TermQuery(__item).add_query(query)

        _field: str = self.get_argument("field", "")
        _agg: str = self.get_argument("agg", "")
        if not (_field or _agg):
            self.set_status(400)
            return

        query = FieldQuery(_field, _agg).add_query(query)
        body = view.get(path, schema, query)

        self.write(f"""\
<html>
<head>
  <script src="/static/js/plotly-latest.min.js"></script>
</head>
<body>
  {body}
</body>
</html>""")

        self.set_status(200)


class TermHandler(tornado.web.RequestHandler):
    def data_received(self, chunk):
        pass

    def get(self, path: str, schema: str) -> None:
        self.write()
        self.set_status(200)

