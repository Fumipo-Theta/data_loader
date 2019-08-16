import abc


class IDataLoader(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def query(self, pathlike, meta={}, preprocessors=[], **kwargs):
        pass

    @abc.abstractmethod
    def read(self, pathlike, meta={}, transformers=[], **kwargs):
        pass
