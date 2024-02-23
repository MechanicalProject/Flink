import abc
from abc import abstractmethod


class StreamResolver(metaclass=abc.ABCMeta):
    @abstractmethod
    def resolve(self):
        return NotImplementedError