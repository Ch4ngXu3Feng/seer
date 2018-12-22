# coding=utf-8

import os
import time
from tornado.options import options


def server_runner() -> None:
    if "init application":
        from tornado.web import Application
        from handler import IndexHandler, TemplateHandler, MappingsHandler, TermsHandler, SeriesHandler

        handlers = [
            (r"/",                      IndexHandler),
            (r"/(.*.html)",             TemplateHandler),
            (r'/(.*)/(.*)/mappings',    MappingsHandler),
            (r'/(.*)/(.*)/terms',       TermsHandler),
            (r'/(.*)/(.*)/series(.*)',  SeriesHandler),
        ]

        settings = dict(
            static_path=os.path.join(os.path.dirname(__file__), "static"),
            template_path=os.path.join(os.path.dirname(__file__), "template"),
            debug=options.debug,
        )

        application = Application(handlers, **settings)

    if "init log":
        import logging
        import sys

        # logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)

        formatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
        root = logging.getLogger()

        file_handler = logging.FileHandler("data/test.log")
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.INFO)
        root.addHandler(file_handler)

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        console_handler.setLevel(logging.DEBUG)
        # root.addHandler(console_handler)
        for _handler in root.handlers:
            logging.error("handler: %s, type: %s", type(_handler), _handler)

    if "init builder":
        from series.builder import SeriesBuilder
        builder: SeriesBuilder = SeriesBuilder(options.data_path)
        builder.load()
        application.series_builder = builder
        application.listen(options.port)

    if "loop":
        import tornado.ioloop
        try:
            tornado.ioloop.IOLoop.instance().start()
        except KeyboardInterrupt:
            tornado.ioloop.IOLoop.instance().stop()


def scrapy_runner() -> None:
    if options.start_year < options.end_year:
        start_year = options.start_year
        end_year = options.end_year
    else:
        start_year = options.end_year
        end_year = options.start_year

    data_path = options.data_path
    application = options.application
    topic = options.topic

    url: str = ""
    if application == "douban_movie":
        url = "https://movie.douban.com/tag/"
    elif application == "douban_music":
        url = "https://music.douban.com/tag/"

    spider: str = f"{application}_{topic}_tag"

    for year in list(range(end_year, start_year, -1)):
        cmd: str = (
            f'scrapy crawl {spider} '
            f'-a start_url={url}{year} '
            f'-a data_path={data_path} '
            f'-a application={application} '
            f'-a topic={topic} '
            f'-s LOG_FILE={data_path}/log/{application}_{topic}_{year}.log'
        )
        os.system(cmd)
        time.sleep(4)


def transformer_runner() -> None:
    from core.transformer import Transformer
    from core.store import DataStore
    from core.source import DataSource
    from series.builder import SeriesBuilder

    application = options.application
    topic = options.topic

    builder: SeriesBuilder = SeriesBuilder(options.data_path)
    store: DataStore = builder.create_store(application, topic)
    src: DataSource = builder.create_source(application, topic, True)
    dst: DataSource = builder.create_source(application, topic, False)

    transformer: Transformer = builder.create_transformer(application, topic)
    transformer.transform(src, dst, store)
