# coding=utf-8

import time
from scrapy import cmdline

if __name__ == "__main__":
    for year in list(range(1980, 2019)):
        cmd: str = (
            f'scrapy crawl douban_movie_tag '
            f'-a url=https://movie.douban.com/tag/{year} '
            f'-a path=data/douban_movie_tag.db '
            f'-a table=subject '
            f'-s LOG_FILE=data/douban_movie_subject_{year}.log'
        )
        cmdline.execute(cmd.split())
        time.sleep(30)
