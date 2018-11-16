# coding=utf-8

import re
import json
import random
import string

from scrapy.spiders import CrawlSpider, Request, Rule, Spider
from scrapy.linkextractors import LinkExtractor

import spider.douban.top250_movie as dtm
import spider.douban.movie_subject as dms


class Top250MoviePageSpider(Spider):
    name = 'douban_top250_movie_page'

    custom_settings = {
        'ITEM_PIPELINES': {
            'spider.douban.top250_movie.Pipeline': 0,
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
                './/div[@class="pic"]/em/text()').extract()[0]
            item['movie_name'] = movie.xpath(
                './/div[@class="hd"]/a/span[1]/text()').extract()[0]
            item['score'] = movie.xpath(
                './/div[@class="star"]/span[@class="rating_num"]/text()'
            ).extract()[0]
            item['score_num'] = movie.xpath(
                './/div[@class="star"]/span/text()').re(r'(\d+)人评价')[0]
            yield item

        next_url = response.xpath('//span[@class="next"]/a/@href').extract()
        if next_url:
            next_url = 'https://movie.douban.com/top250' + next_url[0]
            yield Request(next_url)


class Top250MovieAjaxSpider(Spider):
    name = 'douban_top250_movie_ajax'

    custom_settings = {
        'ITEM_PIPELINES': {
            'spider.douban.top250_movie.Pipeline': 0,
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


class MovieSubjectSpider(CrawlSpider):
    name = 'douban_movie'
    allowed_domains = ['movie.douban.com']
    start_urls = ['https://movie.douban.com/subject/1292052/']

    custom_settings = {
        'ITEM_PIPELINES': {
            'spider.douban.movie_subject.Pipeline': 0,
        }
    }

    rules = {
        # Rule(LinkExtractor(allow=('subject/(\d).*rec$')),
        #    callback='parse_item', follow=True, process_request='cookie'),
        Rule(LinkExtractor(
            allow='https://movie.douban.com/subject/(\d)/'),
            callback='parse_item',
            follow=True,
            process_request='cookie',
        ),
    }

    def cookie(self, request):
        bid = ''.join(random.choice(string.ascii_letters + string.digits)
                      for x in range(11))
        request.cookies['bid'] = bid
        return request

    def start_requests(self):
        for url in self.start_urls:
            bid = ''.join(random.choice(string.ascii_letters + string.digits) for x in range(11))
            yield Request(url, cookies={'bid': bid})

    def get_douban_id(self, subject, response):
        subject['douban_id'] = response.url[35:-10]
        return subject

    def parse_item(self, response):
        subject = dms.Item()
        self.get_douban_id(subject, response)
        regx = '//title/text()'
        data = response.xpath(regx).extract()
        if data:
            subject['name'] = data[0][:-5].strip()
        return subject
