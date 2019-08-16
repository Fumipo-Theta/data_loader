from abc import ABCMeta, abstractmethod


class ILazyReader(metaclass=ABCMeta):
    def __ini__(self):
        pass

    @abstractmethod
    def read(self):
        pass
