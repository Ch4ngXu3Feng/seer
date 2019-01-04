# coding=utf-8

import re
import json

from scrapy.spiders import Request, Spider

import spider.douban_movie.top250_pipeline as dtm


class Top250MoviePageSpider(Spider):
    name = 'douban_movie_top250_page'
    custom_settings = {
        'ITEM_PIPELINES': {
            'spider.douban_movie.top250.Pipeline': 0,
        }
    }

    def start_requests(self):
        url = 'https://movie.douban.com/top250'
        yield Request(url)

    def parse(self, response):
        item = dtm.Item()
        movies = response.xpath('//ol[@class="grid_view"]/li')
        for movie in movies:
            item['ranking'] = movie.xpath(
                './/div[@class="pic"]/em/text()'
            ).extract()[0]

            item['movie_name'] = movie.xpath(
                './/div[@class="hd"]/a/span[1]/text()'
            ).extract()[0]

            item['score'] = movie.xpath(
                './/div[@class="star"]/span[@class="rating_num"]/text()'
            ).extract()[0]

            item['score_num'] = movie.xpath(
                './/div[@class="star"]/span/text()'
            ).re(r'(\d+)人评价')[0]

            yield item

        next_url = response.xpath('//span[@class="next"]/a/@href').extract()
        if next_url:
            next_url = 'https://movie.douban.com/top250' + next_url[0]
            yield Request(next_url)


class Top250MovieAjaxSpider(Spider):
    name = 'douban_movie_top250_ajax'
    custom_settings = {
        'ITEM_PIPELINES': {
            'spider.douban_movie.top250.Pipeline': 0,
        }
    }

    def start_requests(self):
        url = 'https://movie.douban.com/j/chart/top_list?type=5&interval_id=100%3A90&action=&start=0&limit=20'
        yield Request(url)

    def parse(self, response):
        datas = json.loads(response.body)
        item = dtm.Item()
        if datas:
            for data in datas:
                item['ranking'] = data['rank']
                item['movie_name'] = data['title']
                item['score'] = data['score']
                item['score_num'] = data['vote_count']
                yield item

            page_num = re.search(r'start=(\d+)', response.url).group(1)
            page_num = 'start=' + str(int(page_num)+20)
            next_url = re.sub(r'start=\d+', page_num, response.url)
            yield Request(next_url)
