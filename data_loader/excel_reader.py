import pandas as pd
import re
from typing import Callable, List
from func_helper import pip, identity
from .i_lazy_reader import ILazyReader

DataFrame_transformer = Callable[[pd.DataFrame], pd.DataFrame]

matchExcel = r"^(?!.*\~\$).*\.xlsx?$"


class ExcelReader(ILazyReader):
    def __init__(self, path: str, verbose: bool=False):
        self.is_verbose = verbose
        self.path = path

    def read(self, header: int=0, **read_excel_kwargs):

        arg = {
            "header": header,
            **read_excel_kwargs
        }
        if (re.search(r"\.xlsx?$", self.path, re.IGNORECASE) != None):
            self.reader = ExcelReader.readExcel(
                self.path, self.is_verbose, **arg)
        else:
            raise SystemError("Invalid file type.")
        return self

    @staticmethod
    def readExcel(path, verbose, **kwargs):
        if verbose:
            print(f"kwargs for pandas.read_excel: {kwargs}")

        return pd.read_excel(path, **kwargs)

    def assemble(self, *preprocesses: DataFrame_transformer)->pd.DataFrame:

        preprocessor = pip(
            *preprocesses) if len(preprocesses) > 0 else identity

        return preprocessor(self.reader)
