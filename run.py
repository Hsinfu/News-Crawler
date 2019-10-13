import argparse
import logging
import os
import pandas as pd

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
        '--website',
        nargs='+',
        default=['moneyudn', 'chinatimes'],
        help='Available: ["moneyudn", "chinatimes"]',
    )
    parser.add_argument(
        '--output-dir',
        required=True,
        help='The dir to save the excel results file',
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

    df_all = pd.DataFrame()
    if 'moneyudn' in args.website:
        moneyudn_crawler = MoneyUDNCrawler(global_search=args.global_search)
        df_moneyudn = moneyudn_crawler.run(
            keywords=args.keywords,
            start=args.start,
            end=args.end)
        df_all = df_all.append(df_moneyudn, ignore_index=True, sort=False)

    if 'chinatimes' in args.website:
        chinatimes_crawler = ChinaTimesCrawler(global_search=args.global_search)
        df_chinatimes = chinatimes_crawler.run(
            keywords=args.keywords,
            start=args.start,
            end=args.end)
        df_all = df_all.append(df_chinatimes, ignore_index=True, sort=False)

    print(df_all)

    # set the output filename
    output_fname = os.path.join(
        args.output_dir,
        '{}_{}.xlsx'.format(
            args.start.strftime('%Y-%m-%dT%H:%M:%S'),
            args.end.strftime('%Y-%m-%dT%H:%M:%S')))
    sheet_name = 'Sheet1'

    # Create a Pandas Excel writer using XlsxWriter as the engine.
    # Also set the default datetime and date formats.
    writer = pd.ExcelWriter(
        output_fname,
        engine='xlsxwriter',
        datetime_format='yyyy-mm-d hh:mm')

    # Convert the dataframe to an XlsxWriter Excel object.
    df_all.to_excel(
        writer,
        columns=['keyword', 'time', 'title', 'link'],
        sheet_name=sheet_name,
        index=None,
        header=True)

    # Set the column widths, to make the dates clearer.
    writer.sheets[sheet_name].set_column('B:B', 15)
    writer.sheets[sheet_name].set_column('C:D', 60)

    # Close the Pandas Excel writer and output the Excel file.
    writer.save()

if __name__ == '__main__':
    _main()
