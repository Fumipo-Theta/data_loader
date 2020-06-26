import pandas as pd
from .i_lazy_reader import ILazyReader


class NothingReader(ILazyReader):
    def read(self):
        pass

    def assemble(self, *preprocesses) -> pd.DataFrame:
        return pd.DataFrame()
