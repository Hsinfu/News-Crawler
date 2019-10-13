"""MoneyUDNCrawler scratch by keywords and time rage to get related news link."""
import argparse
import logging
import os
import sys

from bs4 import BeautifulSoup
from datetime import datetime
from dateutil.parser import parse as parse_dt
from time import sleep

THIS_FILE_DIR = os.path.dirname(__file__)
sys.path.append(os.path.join(THIS_FILE_DIR, '..'))
from crawler.session_crawler import SessionCrawler

logger_format = '%(asctime)-15s:%(levelname)s:%(name)s:%(message)s'
logger = logging.getLogger(__name__)


class MoneyUDNCrawler(SessionCrawler):
    BASE_URL = 'https://money.udn.com/search/result/1001/'

    @staticmethod
    def get_url(keyword, page=1):
        return '{}{}/{}'.format(MoneyUDNCrawler.BASE_URL, keyword, page)

    def _extract_page_data(self, page_html, keyword):
        def get_title(ele):
            def gen_title():
                for c in ele.select_one('h3').contents:
                    # NOTE: handle the <u>{keyword}</u>
                    yield c if isinstance(c, str) else ''.join(c.contents)
            return ''.join(gen_title())

        def get_link(ele):
            return ele.select_one('a').attrs['href']

        def get_time(link):
            res = self.session.get(link)
            sleep(self.PAGE_QUERY_INTERVAL)
            soup = BeautifulSoup(res.content, 'html.parser')
            time_ele = soup.select_one('div#story div#shareBar div.shareBar__info--author span')
            return parse_dt(time_ele.contents[0])

        soup = BeautifulSoup(page_html, 'html.parser')
        html_elements = soup.select('div#search_content dl dt')
        for html_ele in html_elements:
            title = get_title(html_ele)
            # if not self.global_search, skip data while keyword not in title
            if not self.global_search and keyword not in title:
                logger.debug('Skip data, since keyword(%s) not in title(%s)', keyword, title)
                continue
            link = get_link(html_ele)
            time = get_time(link)
            yield {'title': title, 'link': link, 'time': time}


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

    moneyudn_crawler = MoneyUDNCrawler(global_search=args.global_search)
    df_all = moneyudn_crawler.run(
        keywords=args.keywords,
        start=args.start,
        end=args.end)
    print(df_all)


if __name__ == '__main__':
    _main()
