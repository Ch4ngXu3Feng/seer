# coding=utf-8

from tornado.options import define, parse_command_line


def define_options() -> None:
    if "common options":
        define("runner", default="server", help="run scrapy or http server or transform", type=str)
        define("debug", default=True, help="", type=bool)
        define("log_level", default="debug", help="set debug log level", type=str)
        define("data_path", default="./data", help="data path", type=str)

    if "http server options":
        define("port", default="9914", help="set listen port", type=int)

    if "scrapy options":
        define("application", default="douban_movie", help="application", type=str)
        define("topic", default="subject", help="topic", type=str)

        define("start_year", default=1950, help="start year", type=int)
        define("end_year", default=2018, help="end year", type=int)

    parse_command_line()
