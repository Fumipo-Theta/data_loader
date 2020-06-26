from abc import ABCMeta, abstractmethod
import pandas as pd


class ILazyReader(metaclass=ABCMeta):
    def __init__(self, path: str):
        pass

    @abstractmethod
    def read(self):
        pass

    @abstractmethod
    def assemble(self, *preprocesses) -> pd.DataFrame:
        pass
