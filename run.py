import argparse
import logging

from datetime import datetime
from dateutil.parser import parse as parse_dt

from src.crawler import MoneyUDNCrawler, ChinaTimesCrawler

logger_format = '%(asctime)-15s:%(levelname)s:%(name)s:%(message)s'


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
    df_moneyudn = moneyudn_crawler.run(
        keywords=args.keywords,
        start=args.start,
        end=args.end)

    chinatimes_crawler = ChinaTimesCrawler(global_search=args.global_search)
    df_chinatimes = chinatimes_crawler.run(
        keywords=args.keywords,
        start=args.start,
        end=args.end)

    df_all = df_moneyudn.append(df_chinatimes, ignore_index=True, sort=False)
    print(df_all)


if __name__ == '__main__':
    _main()
