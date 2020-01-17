from .i_data_loader import IDataLoader
import pandas as pd


class TestLoader(IDataLoader):
    def __init__(self):
        pass

    def query(self):
        return TestLoader.read()

    @staticmethod
    def read(source=None, meta={}, transformers=[]):
        return pd.DataFrame({
            "x": [0, 0.5, 1],
            "y": [0, 0.5, 1]
        })
