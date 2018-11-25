# coding=utf-8

import re
import json
import random
import string
import logging

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

    """
    Rule(LinkExtractor(
        allow='https://movie.douban.com/subject/(\d)/'),
        callback='parse_item',
        follow=True,
        process_request='cookie',
    ),
    """
    rules = {
        Rule(
            LinkExtractor(allow='subject/\d+/\?from=subject-page$'),
            callback='parse_item',
            follow=True,
            process_request='cookie'),
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

    def parse_item(self, response):
        subject = dms.Item()

        if "movie_id":
            movie_id = response.url.split("subject/")[-1].split("/")[0]
            subject['movie_id'] = movie_id

        if "title":
            query = '//title/text()'
            data = response.xpath(query).extract()
            if data:
                subject['title'] = data[0][:-5].strip()

        if "region":
            query = (
                '//text()[preceding-sibling::span[text()="制片国家/地区:"]]'
                '[following-sibling::br]'
            )
            data = response.xpath(query).extract()
            if data:
                subject['region'] = data[0].strip()

        if "lang":
            query = (
                '//text()[preceding-sibling::span[text()="语言:"]]'
                '[following-sibling::br]'
            )
            data = response.xpath(query).extract()
            if data:
                subject['lang'] = data[0].strip()

        if "genre":
            query = '//span[@property="v:genre"]/text()'
            genres = response.xpath(query).extract()
            subject['genre'] = '/'.join(genres)

        if "release":
            query = '//span[@property="v:initialReleaseDate"]/@content'
            data = response.xpath(query).extract()
            if data:
                subject['release'] = data[0].strip()

        if "score":
            query = '//*[@id="interest_sectl"]/div[1]/div[2]/strong/text()'
            data = response.xpath(query).extract()
            if data:
                subject['score'] = data[0].strip()

        if "rating_sum":
            query = '//*[@id="interest_sectl"]/div[1]/div[2]/div/div[2]/a/span/text()'
            data = response.xpath(query).extract()
            if data:
                subject['rating_number'] = data[0].strip()

        if "clazz":
            query = '//text()[preceding-sibling::span[text()="集数:"]][following-sibling::br]'
            data = response.xpath(query).extract()
            if data:
                subject['clazz'] = "tv"
            else:
                subject['clazz'] = "movie"

        print("subject", subject)
        return subject


class MovieSubjectTagSpider(Spider):
    name = 'douban_movie_tag'

    allowed_domains = ['movie.douban.com']

    custom_settings = {
        'ITEM_PIPELINES': {
            'spider.douban.movie_subject.Pipeline': 0,
        }
    }

    def check_code(self, response):
        from scrapy.extensions.closespider import CloseSpider
        if response.status == 403:
            raise CloseSpider('check code 403')

    def cookie(self, request):
        bid = ''.join(random.choice(string.ascii_letters + string.digits) for x in range(11))
        request.cookies['bid'] = bid
        return request

    def start_requests(self):
        bid = ''.join(random.choice(string.ascii_letters + string.digits) for x in range(11))
        yield Request(self.url, cookies={'bid': bid})

    def parse_subject(self, response):
        self.check_code(response)

        subject = dms.Item()

        try:
            if "movie_id":
                movie_id = response.url.split("subject/")[-1].split("/")[0]
                subject['movie_id'] = int(movie_id)

            if "title":
                subject['title'] = ""
                query = '//title/text()'
                data = response.xpath(query).extract()
                if data:
                    subject['title'] = data[0][:-5].strip()

            if "director":
                subject['director'] = ""
                query = '//a[@rel="v:directedBy"]/text()'
                data = response.xpath(query).extract()
                if data:
                    subject['director'] = "/".join(data)

            if "author":
                subject['author'] = ""
                query = '//span[text()="编剧"]/following-sibling::*/a/text()'
                data = response.xpath(query).extract()
                if data:
                    subject['author'] = data[0].strip()

            if "actor":
                subject['actor'] = ""
                query = '//a[@rel="v:starring"]/text()'
                data = response.xpath(query).extract()
                if data:
                    subject['actor'] = "/".join(data)

            if "region":
                subject['region'] = ""
                query = '//text()[preceding-sibling::span[text()="制片国家/地区:"]][following-sibling::br]'
                data = response.xpath(query).extract()
                if data:
                    subject['region'] = data[0].strip()

            if "lang":
                subject['lang'] = ""
                query = '//text()[preceding-sibling::span[text()="语言:"]][following-sibling::br]'
                data = response.xpath(query).extract()
                if data:
                    subject['lang'] = data[0].strip()

            if "genre":
                subject['genre'] = ""
                query = '//span[@property="v:genre"]/text()'
                data = response.xpath(query).extract()
                if data:
                    subject['genre'] = '/'.join(data)

            if "release":
                subject['release'] = ""
                query = '//span[@property="v:initialReleaseDate"]/@content'
                data = response.xpath(query).extract()
                if data:
                    subject['release'] = data[0].strip()

            if "episode":
                subject['episode'] = ""
                # query = '//text()[preceding-sibling::span[text()="集数:"]][following-sibling::br]'
                query = '//text()[preceding-sibling::span[text()="集数:"]][following-sibling::br]'
                data = response.xpath(query).extract()
                if data:
                    subject['episode'] = data[0].strip()

            if "duration":
                subject['duration'] = ""
                query = '//text()[preceding-sibling::span[text()="单集片长:"]][following-sibling::br]'
                data = response.xpath(query).extract()
                if data:
                    subject['duration'] = data[0].strip()

            if "runtime":
                subject['runtime'] = ""
                query = '//span[@property="v:runtime"]/text()'
                data = response.xpath(query).extract()
                if data:
                    subject['runtime'] = data[0].strip()

            if "average":
                subject['average'] = ""
                query = '//strong[@property="v:average"]/text()'
                data = response.xpath(query).extract()
                if data:
                    subject['average'] = data[0].strip()

            if "votes":
                subject['votes'] = ""
                query = '//span[@property="v:votes"]/text()'
                data = response.xpath(query).extract()
                if data:
                    subject['votes'] = data[0].strip()

        except Exception as e:
            logging.error("Exception: %s", str(e))
            print(e)

        return subject

    def parse(self, response):
        self.check_code(response)

        movies_url = response.xpath('//a[@class="nbg"]/@href').extract()
        for movie_url in movies_url:
            logging.info("movie_url: %s", movie_url)
            print("movie_url: ", movie_url)
            yield Request(movie_url, callback=self.parse_subject)

        next_url = response.xpath('//span[@class="next"]/a/@href').extract()
        if next_url:
            logging.info("tag: %s", next_url[0])
            print("tag: ", next_url[0])
            yield Request(next_url[0])
