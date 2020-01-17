import abc


class IDataLoader(metaclass=abc.ABCMeta):
    def __init__(self, source, meta={}, preprocessors=[]):
        pass

    @abc.abstractmethod
    def query(self, filter_func, **kwargs):
        pass

    @staticmethod
    @abc.abstractmethod
    def read(source, meta={}, transformers=[], **kwargs):
        pass
