import datetime as dt
import os
import gzip as compression
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import logging
from kai_common import timeutil, util, boto_wrapper
from kai_common.fs import fs
from typing import List, Generator, Any, Tuple, Iterable, Callable
from itertools import groupby
from operator import itemgetter
import _pos_parser

logger = logging.getLogger('model')


def _paginate_by_day(sorted_filenames: List[str]) -> List[List[str]]:
    dates = groupby(enumerate([timeutil.find_datelike_substr(key) for key in sorted_filenames]),
                    key=itemgetter(1))
    return [[sorted_filenames[seq[0]] for seq in group] for day, group in dates]


def to_file(dir_name: str, seq_data: Tuple[int, bytes]) -> str:
    seq, raw_data = seq_data
    dest_file = f'{dir_name}/{seq}.tsv'
    with open(dest_file, 'wb') as fp:
        fp.write(raw_data)
    return f'{dir_name}/{seq}.tsv'


def download_and_decompress(paths: List[str]) -> List[str]:
    dir_name = fs.get_tempdir().name
    logger.warning(f'Using temp dir {dir_name}')
    if not os.path.exists(dir_name):
        os.makedirs(dir_name, exist_ok=True)
    decompressed_data_generator = map(compression.decompress, boto_wrapper.s3_batch_download(paths))
    with ThreadPoolExecutor(max_workers=16) as ex:
        return list(ex.map(partial(to_file, dir_name), enumerate(decompressed_data_generator)))


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
    for day in _paginate_by_day(filenames):
        # save_path: str = f'{parsed_receipt_save_to_path}/{timeutil.find_datelike_substr(day[0])}.tsv.lz4'
        daily_files = download_and_decompress(day)
        _pos_parser.load([daily_files], ["/tmp/out1"], [])


process_raw_data(date_offset=dt.timedelta(days=-1))
