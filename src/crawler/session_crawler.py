"""Base class for crawler"""
import abc
import logging
import pandas as pd

from datetime import timedelta
from time import sleep
from requests import Session


logger_format = '%(asctime)-15s:%(levelname)s:%(name)s:%(message)s'
logger = logging.getLogger(__name__)


class BaseSessionCrawler():
    def __init__(self):
        self._session = None

    @property
    def session(self):
        if self._session is None:
            self._session = Session()
        return self._session


class SessionCrawler(BaseSessionCrawler):
    MAX_QUERY_PAGE = 10  # set the max query page to avoid being banned
    PAGE_QUERY_INTERVAL = 1  # sleep 1 sec after page query to avoid being banned

    def __init__(self, global_search=False):
        super().__init__()
        self.global_search = global_search

    @staticmethod
    @abc.abstractmethod
    def get_url(keyword, page=1):
        raise NotImplementedError

    @staticmethod
    def _extract_page_data(self, page_html, keyword):
        raise NotImplementedError

    def gen_page_data(self, keyword, page=1):
        url = self.get_url(keyword, page)
        logger.info('Crawling url: %s', url)
        res = self.session.get(url)
        yield from self._extract_page_data(res.content, keyword)

    def gen_data(self, keyword):
        """
            TODO: add keyword support for '台積電&聯發科'
                1. replace & to %20 in the URL
                2. check all keys, which split from keyword.split('&'), in title
        """
        for page in range(1, self.MAX_QUERY_PAGE + 1):
            yield from self.gen_page_data(keyword, page)
            sleep(self.PAGE_QUERY_INTERVAL)
        logger.warning('Exceed MAX_QUERY_PAGE %d', self.MAX_QUERY_PAGE)

    def search(self, keyword, start, end):
        for d in self.gen_data(keyword):
            # NOTE: stop search if start - 1 day is reaching
            if d['time'] < start - timedelta(days=1):
                logger.info('Stop search, since d["time"](%s) < start(%s) - 1 day', d['time'], start)
                break

            # skip time > end
            if d['time'] > end:
                logger.debug('Skip data %s, since d["time"](%s) > end(%s)', d, d['time'], end)
                continue

            # skip time < start
            if d['time'] < start:
                logger.debug('Skip data %s, since d["time"](%s) < start(%s)', d, d['time'], start)
                continue

            # yield d
            logger.info('Get data: %s', d)
            yield d

    def run(self, keywords, start, end):
        df_all = pd.DataFrame()
        for keyword in keywords:
            data = self.search(keyword, start, end)
            df = pd.DataFrame(data)
            df['keyword'] = [keyword for _ in range(len(df))]
            df_all = df_all.append(df, ignore_index=False, sort=False)
        return df_all
