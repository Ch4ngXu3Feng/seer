# coding=utf-8

import time, os

if __name__ == "__main__":
    for year in list(range(1980, 2019)):
        cmd: str = (
            f'scrapy crawl douban_movie_tag '
            f'-a url=https://movie.douban.com/tag/{year} '
            f'-a path=data/douban_movie_tag.db '
            f'-a table=subject '
            f'-s LOG_FILE=data/douban_movie_subject_{year}.log'
        )
        os.system(cmd)
        time.sleep(30)
