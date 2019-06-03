from .i_data_loader import IDataLoader
from .csv_reader import CsvReader
from .excel_reader import ExcelReader
from ..get_path import PathList
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
        reader.setPath(path)
        reader.read(**meta)
        reader.assemble(*transformers)
        return reader.df
    return read


class TableLoader(IDataLoader):
    def __init__(self):
        pass

    def read(self, path_like, meta={}, transformers=[], concat=True):
        paths = TableLoader.toPathList(path_like)

        # with Pool() as p:
        dfs = list(map(parallel_read(meta, transformers), paths))

        if concat:
            return pd.concat(dfs, sort=True) if len(dfs) > 0 else []
        else:
            dfs

    @staticmethod
    def IReader(path):
        if (re.search(r"\.csv$", path, re.IGNORECASE) != None):
            return CsvReader()

        elif (re.search(r"\.xlsx?$", path, re.IGNORECASE) != None):
            return ExcelReader()

        else:
            raise SystemError(f"Invalid file type: {path}")

    @staticmethod
    def toPathList(pathLike):
        if type(pathLike) is PathList:
            return pathLike.files()
        elif type(pathLike) in [list, tuple]:
            return pathLike
        elif type(pathLike) is str:
            return [pathLike]
        else:
            print(pathLike)
            raise TypeError("Invalid data source type.")
