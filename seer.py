# coding=utf-8


if __name__ == "__main__":
    from options import define_options
    define_options()

    from tornado.options import options

    if options.runner == "server":
        from runner import server_runner
        server_runner()

    elif options.runner == "scrapy":
        from runner import scrapy_runner
        scrapy_runner()

    elif options.runner == "transformer":
        from runner import transformer_runner
        transformer_runner()

    else:
        raise NotImplementedError()
