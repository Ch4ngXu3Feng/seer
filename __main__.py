# coding=utf-8

import tornado.ioloop

from series.data import SeriesData
from handler import DoubanApp


def main() -> None:
    app = DoubanApp()
    app.listen(9090)
    tornado.ioloop.IOLoop.instance().start()


def test() -> None:
    from source.sqlite3 import SqliteDataSource
    from source.csv import CsvDataSource
    from source.excel import ExcelDataSource
    from drawing.plotly import PlotlyDrawing
    from series.visitor import SeriesDataVisitor
    from core.query import DataQuery
    from core.source import DataSource

    from query.tag import TagQuery
    from query.field import FieldQuery
    from query.range import RangeQuery
    from query.term import TermQuery

    # ds: DataSource = ExcelDataSource("data", "test", "test", "gb2312")
    ds: DataSource = SqliteDataSource("data", "test", "test", "gb2312")

    """
    ds.mock()
    ds.store()
    ds.clean()
    """

    drawing = PlotlyDrawing()

    sd = SeriesData(ds, drawing)
    pdv = SeriesDataVisitor(sd.context())

    query: DataQuery = TagQuery("color", "red").add_query(
        TagQuery("area", "bj").add_query(
            RangeQuery("timestamp", (1541692800, 1541693400)).add_query(
                FieldQuery("count").add_query(
                    TermQuery("timestamp")
                )
            )
        )
    )

    query.accept(pdv)

    sd.render()
    for c in ds.columns():
        print(c, ds.unique(c))


if __name__ == "__main__":
    test()
    #main()
