import datetime as dt
import os
import gzip as compression
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import logging
from kai_common import timeutil, util, boto_wrapper, fs
from kai_common.fs import fs
from typing import List, Generator, Any, Tuple, Iterable, Callable
from itertools import groupby
from operator import itemgetter
from parse_helper import _paginate_by_day
from _pos_parser import StrVector
import _pos_parser

logger = logging.getLogger('model')


def process_raw_data(source_data_path: str = 's3://kesko-aws-data-import/pdata/KEDW_AWS_POS_DTL_201',
                     parsed_receipt_save_to_path: str = 's3://kai-receipt-data-test',
                     metadata_save_to_path: str = 's3://kai-data-test/metadata/metadata.json.lz4',
                     date_origin: dt.datetime = dt.datetime.today(),
                     date_offset: dt.timedelta = dt.timedelta(days = -100),
                     overwrite: bool = False):
    all_files_in_path = boto_wrapper.s3_list(source_data_path)
    date_destination = date_origin + date_offset

    start_date = min(date_origin, date_destination)
    end_date = max(date_origin, date_destination)
    logger.warning(f'Starting pos data parsing for {date_offset.days} days, '
                   f'starting {timeutil.to_iso_date_string(date_origin)}')

    filenames = sorted([key[0] for key in all_files_in_path
                        if start_date.date() <= timeutil.find_datelike_substr(key[0]) <= end_date.date()])
    logger.warning(f'Total files: {len(filenames)}, Starting filename: {filenames[0]}')
    paginated_file_names = [StrVector(day) for day in _paginate_by_day(filenames)]
    aggregated_pos_save_paths = [f'/tmp/{timeutil.find_datelike_substr(day[0])}' for day in paginated_file_names]
    # save_path: str = f'{parsed_receipt_save_to_path}/{timeutil.find_datelike_substr(day[0])}.tsv.lz4'
    _pos_parser.load(paginated_file_names, StrVector(aggregated_pos_save_paths), StrVector(), False)


process_raw_data(date_offset=dt.timedelta(days=-1))
