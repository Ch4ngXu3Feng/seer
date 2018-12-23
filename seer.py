# coding=utf-8


if __name__ == "__main__":
    from options import define_options
    define_options()

    from tornado.options import options

    if "make dir":
        import os
        dirs = ["log", "raw", "store"]
        for _dir in dirs:
            path = os.path.join(options.data_path, _dir)
            if not os.path.exists(_dir):
                os.makedirs(_dir)

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
