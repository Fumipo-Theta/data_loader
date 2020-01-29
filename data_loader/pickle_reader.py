import pandas as pd
from func_helper import pip, identity
from .i_lazy_reader import ILazyReader


class PickleReader(ILazyReader):
    def __init__(self, path: str=None, verbose: bool=False, **kwargs):
        self.is_verbose = verbose
        self.path = path

    def read(self, **kwargs):
        self.reader = pd.read_pickle(self.path)

    def assemble(self, *preprocesses) -> pd.DataFrame:
        preprocessor = pip(
            *preprocesses) if len(preprocesses) > 0 else identity

        return preprocessor(self.reader)
