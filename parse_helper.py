import os
import gzip as compression
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from kai_common import timeutil, util, boto_wrapper
from kai_common.fs import fs, s3
from typing import List, Generator, Any, Tuple, Iterable, Callable
from itertools import groupby
from operator import itemgetter


def _paginate_by_day(sorted_filenames: List[str]) -> List[List[str]]:
    dates = groupby(enumerate([timeutil.find_datelike_substr(key) for key in sorted_filenames]),
                    key=itemgetter(1))
    return [[sorted_filenames[seq[0]] for seq in group] for day, group in dates]


def to_file(dir_name: str, seq_data: Tuple[int, bytes]) -> str:
    seq, raw_data = seq_data
    print(dir_name, seq)
    dest_file = f'{dir_name}/{seq}.tsv'
    with open(dest_file, 'wb') as fp:
        fp.write(raw_data)
    return f'{dir_name}/{seq}.tsv'


def download_and_decompress(paths: List[str], override_dir: str = None) -> List[str]:
    dir_name = override_dir or fs.get_tempdir().name
    print(f'Using temp dir {dir_name}')
    if not os.path.exists(dir_name):
        os.makedirs(dir_name, exist_ok=True)

    print("downloading")
    decompressed_data_generator = map(compression.decompress, boto_wrapper.s3_batch_download(paths))
    print("done downloading")
    with ThreadPoolExecutor(max_workers=16) as ex:
        return list(ex.map(partial(to_file, dir_name), enumerate(decompressed_data_generator)))
