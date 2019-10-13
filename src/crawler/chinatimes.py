"""ChinaTimesCrawler scratch by keywords and time rage to get related news link."""
import argparse
import logging
import pandas as pd

from bs4 import BeautifulSoup
from datetime import datetime
from dateutil.parser import parse as parse_dt
from time import sleep
from requests import Session

logger_format = '%(asctime)-15s:%(levelname)s:%(name)s:%(message)s'
logger = logging.getLogger(__name__)


class ChinaTimesCrawler():
    BASE_URL = 'https://www.chinatimes.com/search/'
    MAX_QUERY_PAGE = 10  # set the max query page to avoid being banned
    PAGE_QUERY_INTERVAL = 1  # sleep 1 sec after page query to avoid being banned

    def __init__(self):
        self._session = None

    @property
    def session(self):
        if self._session is None:
            self._session = Session()
        return self._session

    @staticmethod
    def get_url(keyword, page=1):
        return '{}{}?page={}&chdtv'.format(ChinaTimesCrawler.BASE_URL, keyword, page)

    @staticmethod
    def _extract_page_data(page_html):
        soup = BeautifulSoup(page_html, 'html.parser')
        html_elements = soup.select('section.search-result div.article-list ul li')
        for html_element in html_elements:
            yield {
                'title': ''.join(html_element.select_one('h3.title a').contents),
                'time': parse_dt(html_element.select_one('time').attrs['datetime']),
                'link': html_element.select_one('h3.title a').attrs['href'],
            }

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


def _parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--keywords',
        required=True,
        nargs='+',
        help='Keywords',
    )
    parser.add_argument(
        '--start',
        required=True,
        type=parse_dt,
        help='Start time',
    )
    parser.add_argument(
        '--end',
        type=parse_dt,
        help='End time. Default: now',
    )
    parser.add_argument(
        '--global-search',
        action='store_true',
        help='If global search, then search both in the title and the description',
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='If debug, then show logger.debug',
    )
    args = parser.parse_args()
    if args.end is None:
        args.end = datetime.now()
    return args


def _main():
    args = _parse_arguments()
    if args.debug:
        logging.basicConfig(level=logging.DEBUG, format=logger_format)
    else:
        logging.basicConfig(level=logging.INFO, format=logger_format)

    chinatimes_crawler = ChinaTimesCrawler()
    df_all = chinatimes_crawler.run(
        keywords=args.keywords,
        start=args.start,
        end=args.end,
        global_search=args.global_search)
    print(df_all)


if __name__ == '__main__':
    _main()
