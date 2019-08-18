from .i_data_loader import IDataLoader
from func_helper import pip, identity
import pandas as pd
from ..get_path import PathList


class DataFrameLoader(IDataLoader):
    def __init__(self, path_like="", meta={}, preprocessors=[]):
        self.paths = PathList.to_strings(path_like)
        self.read_option = meta
        self.preprocessors = preprocessors

    def query(self, filter_func=identity):
        return self.read(
            self.paths,
            self.read_option,
            [
                *self.preprocessors,
                filter_func
            ]
        )

    def read(self, data, meta={}, transformers=[identity]):
        return pip(*transformers)(
            data)
