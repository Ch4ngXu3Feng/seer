# coding=utf-8

import re
import json
import random
import string
import logging

from scrapy.spiders import CrawlSpider, Request, Rule, Spider
from scrapy.linkextractors import LinkExtractor

import spider.douban_music.subject_pipeline as dms


# FIXME
class MusicSubjectSpider(CrawlSpider):
    name = 'douban_music_subject_page'
    allowed_domains = ['music.douban.com']
    start_urls = ['https://music.douban.com/subject/1292052/']

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

        logging.info("subject: %s", subject)
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
        yield Request(self.start_url, cookies={'bid': bid})

    def parse_subject(self, response):
        self.check_code(response)

        subject = dms.Item()

        try:
            if dms.Item.MOVIE_ID_NAME:
                movie_id = response.url.split("subject/")[-1].split("/")[0]
                subject[dms.Item.MOVIE_ID_NAME] = int(movie_id)

            if dms.Item.TITLE_NAME:
                name: str = dms.Item.TITLE_NAME
                value: str = ""
                query = '//title/text()'
                data = response.xpath(query).extract()
                if data:
                    value = data[0][:-5].strip()
                subject[name] = value

            if dms.Item.DIRECTOR_NAME:
                name: str = dms.Item.DIRECTOR_NAME
                value: str = ""
                query = '//a[@rel="v:directedBy"]/text()'
                data = response.xpath(query).extract()
                if data:
                    value = "/".join(data)
                subject[name] = value

            if dms.Item.AUTHOR_NAME:
                name: str = dms.Item.AUTHOR_NAME
                value: str = ""
                query = '//span[text()="编剧"]/following-sibling::*/a/text()'
                data = response.xpath(query).extract()
                if data:
                    value = data[0].strip()
                subject[name] = value

            if dms.Item.ACTOR_NAME:
                name: str = dms.Item.ACTOR_NAME
                value: str = ""
                query = '//a[@rel="v:starring"]/text()'
                data = response.xpath(query).extract()
                if data:
                    value = "/".join([_data.strip() for _data in data])
                subject[name] = value

            if dms.Item.REGION_NAME:
                name: str = dms.Item.REGION_NAME
                value: str = ""
                query = '//text()[preceding-sibling::span[text()="制片国家/地区:"]][following-sibling::br]'
                data = response.xpath(query).extract()
                if data:
                    value = data[0].strip()
                subject[name] = value

            if dms.Item.LANG_NAME:
                name: str = dms.Item.LANG_NAME
                value: str = ""
                query = '//text()[preceding-sibling::span[text()="语言:"]][following-sibling::br]'
                data = response.xpath(query).extract()
                if data:
                    value = data[0].strip()
                subject[name] = value

            if dms.Item.GENRE_NAME:
                name: str = dms.Item.GENRE_NAME
                value: str = ""
                query = '//span[@property="v:genre"]/text()'
                data = response.xpath(query).extract()
                if data:
                    value = '/'.join([_data.strip() for _data in data])
                subject[name] = value

            if dms.Item.RELEASE_NAME:
                name: str = dms.Item.RELEASE_NAME
                value: str = ""
                query = '//span[@property="v:initialReleaseDate"]/@content'
                data = response.xpath(query).extract()
                if data:
                    value = data[0].strip()
                subject[name] = value

            if dms.Item.EPISODE_NAME:
                name: str = dms.Item.EPISODE_NAME
                value: str = ""
                query = '//text()[preceding-sibling::span[text()="集数:"]][following-sibling::br]'
                data = response.xpath(query).extract()
                if data:
                    value = data[0].strip()
                subject[name] = value

            if dms.Item.DURATION_NAME:
                name: str = dms.Item.DURATION_NAME
                value: str = ""
                query = '//text()[preceding-sibling::span[text()="单集片长:"]][following-sibling::br]'
                data = response.xpath(query).extract()
                if data:
                    value = data[0].strip()
                subject[name] = value

            if dms.Item.RUNTIME_NAME:
                name: str = dms.Item.RUNTIME_NAME
                value: str = ""
                query = '//span[@property="v:runtime"]/text()'
                data = response.xpath(query).extract()
                if data:
                    value = data[0].strip()
                subject[name] = value

            if dms.Item.AVERAGE_NAME:
                name: str = dms.Item.AVERAGE_NAME
                value: str = ""
                query = '//strong[@property="v:average"]/text()'
                data = response.xpath(query).extract()
                if data:
                    value = data[0].strip()
                subject[name] = value

            if dms.Item.VOTES_NAME:
                name: str = dms.Item.VOTES_NAME
                value: str = ""
                query = '//span[@property="v:votes"]/text()'
                data = response.xpath(query).extract()
                if data:
                    value = data[0].strip()
                subject[name] = value

        except Exception as e:
            logging.error("Exception: %s", e)

        return subject

    def parse(self, response):
        self.check_code(response)

        movies_url = response.xpath('//a[@class="nbg"]/@href').extract()
        for movie_url in movies_url:
            logging.info("movie_url: %s", movie_url)
            yield Request(movie_url, callback=self.parse_subject)

        next_url = response.xpath('//span[@class="next"]/a/@href').extract()
        if next_url:
            logging.info("tag: %s", next_url[0])
            yield Request(next_url[0])
