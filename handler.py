# coding=utf-8

from typing import List, Optional, Any
import json
import tornado.web

from core.query import DataQuery
from core.builder import Builder
from core.store import DataStore

from series.data import SeriesData
from series.visitor import SeriesDataVisitor


class Series(object):
    def __init__(self, application: Any):
        super().__init__()
        self.application: Any = application

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
        store = ""

        self.get(path, store, query)

    def get(self, application: str, topic: str, query: DataQuery) -> str:
        sd: SeriesData = SeriesData(application, topic, self.application.series_builder)
        pdv: SeriesDataVisitor = SeriesDataVisitor(sd)
        query.accept(pdv)
        return sd.render()


class BaseHandler(tornado.web.RequestHandler):
    def data_received(self, chunk):
        pass

    def options(self, *args, **kwargs):
        self.set_status(204)

    def allow_cross(self):
        self.set_header('Access-Control-Allow-Origin', '*')
        self.set_header('Access-Control-Allow-Credentials', "true")
        self.set_header('Access-Control-Allow-Methods', 'POST, OPTIONS, GET, PUT, DELETE')
        self.set_header('Access-Control-Allow-Headers', (
                "Content-Type, Content-Length, Accept-Encoding, "
                "X-CSRF-Token, Authorization, accept, origin, "
                "Cache-Control, X-Requested-With"
            )
        )


class IndexHandler(BaseHandler):
    def get(self) -> None:
        self.allow_cross()
        self.render("index.html")


class TemplateHandler(BaseHandler):
    def get(self, file_name: str) -> None:
        self.allow_cross()
        if not file_name:
            file_name = "index.html"
        self.render(file_name)


class MappingsHandler(BaseHandler):
    def get(self, application: str, topic: str) -> None:
        self.allow_cross()

        builder: Builder = self.application.series_builder
        store: DataStore = builder.create_store(application, topic)

        result = dict()
        result["data"] = store.mappings()
        result["code"] = 200
        result["message"] = "success"

        context: str = json.dumps(result)

        self.write(context)
        self.set_status(200)


class TermsHandler(BaseHandler):
    def get(self, application: str, topic: str) -> None:
        self.allow_cross()

        term_name: str = self.get_argument("name", "")

        builder: Builder = self.application.series_builder
        store: DataStore = builder.create_store(application, topic)

        terms = store.terms(term_name)
        if len(terms) > 1024:
            terms = terms[:1024]

        result = dict()
        result["data"] = terms
        result["code"] = 200
        result["message"] = "success"

        self.write(json.dumps(result))
        self.set_status(200)


class SeriesHandler(BaseHandler):
    def parse_query(self) -> Optional[DataQuery]:
        from query.tag import TagQuery
        from query.field import FieldQuery
        from query.range import RangeQuery
        from query.term import TermQuery

        query: DataQuery = None
        status: int = -1
        _field: str = None
        _agg: str = None

        while True:
            _range: str = self.get_argument("range", "")
            if _range:
                __range: List[str] = _range.split(":")
                if len(__range) != 2:
                    status = 400
                    break

                range_name: str = __range[0]
                range_value: str = __range[1]
                _range_value = range_value.split(",")
                if len(_range_value) != 2:
                    status = 400
                    break

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

            _field = self.get_argument("field", "")
            _agg = self.get_argument("agg", "")
            if not (_field or _agg):
                status = 400
                break

            break

        if status != -1:
            self.set_status(status)
        else:
            query = FieldQuery(_field, _agg).add_query(query)

        return query

    def get(self, application: str, topic: str, endpoint_type: str) -> None:
        self.allow_cross()

        status: int = -1
        context: str = ""

        while True:
            query: DataQuery = self.parse_query()
            if query is None:
                break

            series = Series(self.application)
            context = series.get(application, topic, query)
            if not context:
                status = 500
                break

            if endpoint_type:
                context = f"""\
<html>
<head>
    <script src="/static/js/plotly-latest.min.js"></script>
</head>
<body>
    {context}
</body>
</html>"""

            status = 200
            break

        if status != -1:
            self.set_status(status)

            if status == 200:
                self.write(context)
