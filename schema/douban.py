# coding=utf-8

from typing import Dict, List, Set, Tuple, Union
import scrapy

from core.schema import Schema
from series.data import SeriesContext
from pandas import Series


class MovieSubjectViewItem(scrapy.Item):
    date = scrapy.Field()

    region = scrapy.Field()
    lang = scrapy.Field()
    genre = scrapy.Field()

    score = scrapy.Field()
    rating_number = scrapy.Field()


class MovieSubjectSchema(Schema):
    def __init__(self, file_name: str, table_name: str) -> None:
        super().__init__()

        self.__item: scrapy.Item = None
        if table_name.find("subject") != -1:
            import spider.douban.movie_subject as dms
            self.__item = dms.Item()

        self.__ranges: List[str] = ['date']
        self.__tags: List[str] = ['region', 'lang', 'genre']
        self.__fields: List[str] = ['score', 'rating_number']

    def parse(self, data: Series, item: scrapy.Item) -> bool:
        item['region'] = set()
        if 'region' in self.__item.fields:
            regions: Set[str] = item['region']
            _regions: List[str] = data['region'].split("/")
            for _region in _regions:
                regions.add(_region.strip())

        item['lang'] = set()
        if 'lang' in self.__item.fields:
            langs: Set[str] = item['lang']
            _langs: List[str] = data['lang'].split("/")
            for _lang in _langs:
                langs.add(_lang.strip())

        item['genre'] = set()
        if 'genre' in self.__item.fields:
            genres: Set[str] = item['genre']
            _genres: List[str] = data['genre'].split("/")
            for _genre in _genres:
                genres.add(_genre.strip())

        item['date'] = 0
        if 'release' in self.__item.fields:
            release: str = data['release']
            date = release.split("-")[0].split("(")[0]
            if date:
                item['date'] = int(date)

        item['score'] = float(0.0)
        if 'score' in self.__item.fields:
            score = data['score']
            if score:
                item['score'] = float(score)

        return True

    def range_filter(self, item: scrapy.Item, range_name: str, range_value: Tuple[int, int]) -> bool:
        if range_name:
            _from, _to = range_value
            value = item[range_name]
            if not _from <= value < _to:
                return False
        return True

    def tag_filter(self, item: scrapy.Item, tags: List[Tuple[str, str]]) -> bool:
        for tag in tags:
            name = tag[0]
            value = tag[1]
            found = False

            if name == 'region':
                region = item['region']
                for r in region:
                    if r.find(value) != -1:
                        found = True

                if not found:
                    return False

            elif name == 'lang':
                lang = item['lang']
                for l in lang:
                    if l.find(value) != -1:
                        found = True

                if not found:
                    return False

        return True

    """
    def sum_field(self, item: scrapy.Item, name: str, field: int,
                  result: Dict[int, Tuple[int, int]]):
        value = item[name]
        _sum, _count = result[field]
        _sum += value
        _count += 1
        result[field] = _sum, _count

    def count_field(self, item: scrapy.Item, name: str, field: int,
                    data: Dict[int, Tuple[int, int]]):
        _sum, _count = data[field]
        _sum += 1
        _count += 1
        data[field] = _sum, _count
    """

    def loop_term(self, item: scrapy.Item,
                  index: int, key: str, terms: List[str], name: str, field: str,
                  data: Dict[str, Dict[int, Union[int, List[int]]]]) -> None:

        if index != len(terms):
            term = terms[index]

            values = item[term]
            for value in values:
                _key = ""
                if not value:
                    value = "-"
                if key:
                    _key += "/"
                _key += value
                self.loop_term(item, index + 1, key + _key, terms, name, field, data)
        else:
            self.__term_field(item, name, key, field, data)

    def term_field(self, item: scrapy.Item,
                   name: str, terms: List[str], field: str,
                   data: Dict[str, Dict[int, Union[int, List[int]]]]) -> None:

        self.loop_term(item, 0, "", terms, name, field, data)

    def __term_field(self, item: scrapy.Item,
                     name: str, key: str, field: str,
                     data: Dict[str, Dict[int, Union[int, List[int]]]]) -> None:

        _data: Dict[int, Union[int, List[int]]] = data.get(key)

        if _data is None:
            _data = dict()
            data[key] = _data

        term = item[name]
        if field:
            value = item[field]
            values: List[int] = _data.get(term)
            if values is None:
                values = list()
                _data[term] = values
            values.append(value)

        else:
            value = _data.get(term, 0)
            _data[term] = int(value + 1)

    def sum_field(self, data: Dict[str, Dict[int, Union[int, List[int]]]]) -> Dict[str, Dict[int, int]]:
        result: Dict[str, Dict[int, int]] = dict()
        for name, item in data.items():
            _result: Dict[int, int] = dict()
            for k, v in item.items():
                _result[k] = sum(v)
            result[name] = _result
        return result

    def count_field(self, data: Dict[str, Dict[int, Union[int, List[int]]]]) -> Dict[str, Dict[int, int]]:
        return data

    def avg_field(self, data: Dict[str, Dict[int, Union[int, List[int]]]]) -> Dict[str, Dict[int, int]]:
        result: Dict[str, Dict[int, int]] = dict()
        for name, item in data.items():
            _result: Dict[int, int] = dict()
            for k, v in item.items():
                _result[k] = sum(v) / len(v)
            result[name] = _result
        return result

    def _process_series(self, data: Series, context: SeriesContext) -> None:
        item = MovieSubjectViewItem()

        if not self.parse(data, item):
            return

        if not self.range_filter(item, context.range_name, context.range):
            return

        if not self.tag_filter(item, context.tags):
            return

        self.term_field(item, context.range_name, context.terms, context.field_name, context.data)

    def process_series(self, context: SeriesContext) -> None:
        if context.field_aggregation == "count":
            context.field_name = ""

        self.source.data().apply(self._process_series, axis=1, context=context)

        if context.field_aggregation == "sum":
            context.data = self.sum_field(context.data)
        elif context.field_aggregation == "count":
            context.data = self.count_field(context.data)
        elif context.field_aggregation == "avg":
            context.data = self.avg_field(context.data)
