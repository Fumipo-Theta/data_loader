from .i_data_loader import IDataLoader
from func_helper import pip, identity
import pandas as pd


class DictLoader(IDataLoader):
    """
    dictLoader = DictLoader()
    df = dictLoader.read(dict_data, transformers=[f,g])
    """

    def __init__(self, source, meta={}, preprocessors=[]):
        self.source = source
        self.read_option = meta
        self.preprocessors = preprocessors

    def query(self, filter_func=identity):
        return DictLoader.read(
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
            pd.DataFrame(source))
