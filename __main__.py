# coding=utf-8


def main() -> None:
    import os
    import tornado.ioloop
    from tornado.web import Application
    from handler import ViewHandler, TermHandler

    handlers = [
        (r'/(.*)/(.*)/view', ViewHandler),
        (r'/(.*)/(.*)/term', TermHandler),
    ]

    settings = dict(
        static_path=os.path.join(os.path.dirname(__file__), "static"),
        debug=True,
    )

    Application(handlers, **settings).listen(9090)

    try:
        tornado.ioloop.IOLoop.instance().start()
    except KeyboardInterrupt:
        tornado.ioloop.IOLoop.instance().stop()


def test() -> None:
    from source.sqlite3 import SqliteDataSource
    source = SqliteDataSource("data", "test", "test")
    source.mock()
    source.store()
    return

    from handler import View
    view = View()
    view.test()


if __name__ == "__main__":
    # test()
    main()
