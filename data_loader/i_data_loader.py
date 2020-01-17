import abc


class IDataLoader(metaclass=abc.ABCMeta):
    def __init__(self, source, meta={}, preprocessors=[]):
        pass

    @abc.abstractmethod
    def query(self, filter_func, **kwargs):
        pass

    @abc.abstractmethod
    @staticmethod
    def read(source, meta={}, transformers=[], **kwargs):
        pass
