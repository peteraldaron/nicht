import datetime as dt
import json
import os
import gzip as compression
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
from functools import partial
import logging
from kai_common import timeutil, util, boto_wrapper
from kai_common.fs import fs, s3
from typing import List, Generator, Any, Tuple, Iterable, Callable
from itertools import groupby
from operator import itemgetter
from parse_helper import _paginate_by_day
from _pos_parser import StrVector
import _pos_parser

logger = logging.getLogger('model')
PRODUCT_METADATA_KEYS = [
        'food_nonfood_ind',
        'product_name',
        'producthierarchylevel1',
        'producthierarchylevel1name',
        'producthierarchylevel2',
        'producthierarchylevel2name',
        'producthierarchylevel3',
        'producthierarchylevel3name',
        'producthierarchylevel4',
        'producthierarchylevel4name',
        'producthierarchylevel5',
        'producthierarchylevel5name']


def process_raw_data(source_data_path: str = 's3://kesko-aws-data-import/pdata/KEDW_AWS_POS_DTL_201',
                     parsed_receipt_save_to_path: str = 's3://kai-receipt-data-test',
                     metadata_save_to_path: str = 's3://kai-data-test/metadata',
                     date_origin: dt.datetime = dt.datetime.today(),
                     date_offset: dt.timedelta = dt.timedelta(days = -100),
                     overwrite: bool = False):
    all_files_in_path = boto_wrapper.s3_list(source_data_path)
    date_destination = date_origin + date_offset
    fsio = s3.s3()

    start_date = min(date_origin, date_destination)
    end_date = max(date_origin, date_destination)
    logger.warning(f'Starting pos data parsing for {date_offset.days} days, '
                   f'starting {timeutil.to_iso_date_string(date_origin)}')

    filenames = sorted([key[0] for key in all_files_in_path
                        if start_date.date() <= timeutil.find_datelike_substr(key[0]) <= end_date.date()])
    logger.warning(f'Total files: {len(filenames)}, Starting filename: {filenames[0]}')
    paginated_file_names = [StrVector(day) for day in _paginate_by_day(filenames)]
    aggregated_pos_save_paths = [f'/tmp/{timeutil.find_datelike_substr(day[0])}.tsv' for day in paginated_file_names]
    # save_path: str = f'{parsed_receipt_save_to_path}/{timeutil.find_datelike_substr(day[0])}.tsv.lz4'
    prod, profit, margin, sbu, bh, bp = _pos_parser.load(paginated_file_names, StrVector(aggregated_pos_save_paths),
                                                         StrVector(), parsed_receipt_save_to_path, False)
    #return _pos_parser.load(paginated_file_names, StrVector(aggregated_pos_save_paths), StrVector(), False)
    profit_dict = defaultdict(dict)
    margin_dict = defaultdict(dict)
    product_metadata_dict = defaultdict(dict)

    for entry in profit:
        profit_dict[str(entry[0])][entry[1]] = entry[2]

    for entry in margin:
        margin_dict[str(entry[0])][entry[1]] = entry[2]

    for entry in prod.items():
        for key in PRODUCT_METADATA_KEYS:
            product_metadata_dict[str(entry[0])][key.upper()] = getattr(entry[1], key)

    fsio.put_object(f'{metadata_save_to_path}/product_metadata.json', data=json.dumps(product_metadata_dict).encode())
    fsio.put_object(f'{metadata_save_to_path}/sbu_map.json', data=json.dumps(sbu).encode())
    fsio.put_object(f'{metadata_save_to_path}/profit.json', data=json.dumps(profit_dict).encode())
    fsio.put_object(f'{metadata_save_to_path}/margin.json', data=json.dumps(margin_dict).encode())
    fsio.put_object(f'{metadata_save_to_path}/household_blacklist.json', data=json.dumps(list(bh)).encode())
    fsio.put_object(f'{metadata_save_to_path}/person_blacklist.json', data=json.dumps(list(bp)).encode())

process_raw_data(parsed_receipt_save_to_path='s3://kai-receipt-data-staging/testfolder',
                 metadata_save_to_path='s3://kai-data-staging/metadata',
                 date_offset=dt.timedelta(days=-1))
