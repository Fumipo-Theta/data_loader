from .i_data_loader import IDataLoader
from .i_lazy_reader import ILazyReader
from .csv_reader import CsvReader
from .excel_reader import ExcelReader
from .pickle_reader import PickleReader
from ..get_path import PathList
from func_helper import identity
from typing import Type
import pandas as pd
import re
from multiprocessing.dummy import Pool

ext = {
    "csv": r'\.[cC](sv|SV)$',
    "excel": r"^(?!.*\~\$).*\.xlsx?$"
}


def parallel_read(meta, transformers):
    def read(path):
        reader = TableLoader.IReader(path)
        reader.read(**meta)
        return reader.assemble(*transformers)
    return read


def concat_dfs(dfs):
    _dfs = []
    for df in dfs:
        if type(df) is pd.DataFrame:
            _dfs.append(df)
        elif isinstance(df, dict):
            _dfs += list(df.values())
    return pd.concat(_dfs, sort=True)


class TableLoader(IDataLoader):
    def __init__(self, source="", meta={}, preprocessors=[]):
        self.source = source
        self.read_option = meta
        self.preprocessors = preprocessors

    def query(self, filter_func=identity, concat=True):
        return TableLoader.read(
            self.source,
            self.read_option,
            [
                *self.preprocessors,
                filter_func
            ],
            concat
        )

    @staticmethod
    def read(source, meta={}, transformers=[], concat=True):
        paths = PathList.to_strings(source)

        # with Pool() as p:
        dfs = list(map(parallel_read(meta, transformers), paths))

        if concat:
            return concat_dfs(dfs) if len(dfs) > 0 else []
        else:
            dfs

    @staticmethod
    def IReader(path: str)->ILazyReader:
        if (re.search(r"\.csv$", path, re.IGNORECASE) != None):
            return CsvReader(path)

        elif (re.search(r"\.xlsx?$", path, re.IGNORECASE) != None):
            return ExcelReader(path)

        elif (re.search(r"\.pkl$", path, re.IGNORECASE)) != None:
            return PickleReader(path)

        else:
            raise SystemError(f"Invalid file type: {path}")
