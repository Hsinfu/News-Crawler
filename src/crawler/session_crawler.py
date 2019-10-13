"""Base class for crawler"""
import abc
import logging
import pandas as pd

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

    @staticmethod
    @abc.abstractmethod
    def get_url(keyword, page=1):
        raise NotImplementedError

    @staticmethod
    @abc.abstractmethod
    def _extract_page_data(page_html):
        raise NotImplementedError

    def gen_page_data(self, keyword, page=1):
        url = self.get_url(keyword, page)
        logger.info('Crawling url: %s', url)
        res = self.session.get(url)
        page_html = res.content
        yield from self._extract_page_data(page_html)

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

    def search(self, keyword, start, end, global_search=False):
        for d in self.gen_data(keyword):
            if not global_search and keyword not in d['title']:
                logger.debug('Skip data %s, since keyword(%s) not in title(%s)', d, keyword, d['title'])
                continue
            if d['time'] > end:
                logger.debug('Skip data %s, since d["time"](%s) > end(%s)', d, d['time'], end)
                continue
            if d['time'] < start:
                logger.info('Stop search, since d["time"](%s) < start(%s)', d['time'], start)
                break
            logger.info('Get data: %s', d)
            yield d

    def run(self, keywords, start, end, global_search=False):
        df_all = pd.DataFrame()
        for keyword in keywords:
            data = self.search(keyword, start, end, global_search)
            df = pd.DataFrame(data)
            df['keyword'] = [keyword for _ in range(len(df))]
            df_all = df_all.append(df, ignore_index=False, sort=False)
        return df_all
