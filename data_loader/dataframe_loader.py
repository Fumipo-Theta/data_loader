from .i_data_loader import IDataLoader
from func_helper import pip, identity
import pandas as pd


class DataFrameLoader(IDataLoader):
    def __init__(self, source, meta={}, preprocessors=[]):
        self.source = source
        self.read_option = meta
        self.preprocessors = preprocessors

    def query(self, filter_func=identity):
        return DataFrameLoader.read(
            self.source,
            self.read_option,
            [
                *self.preprocessors,
                filter_func
            ]
        )

    @staticmethod
    def read(source, meta={}, transformers=[identity]):
        return pip(*transformers)(
            source)
