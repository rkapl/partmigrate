import abc

class Target(abc.ABC):
    @abc.abstractmethod
    def read(self, offset: int, buffer):
        pass

    @abc.abstractmethod
    def write(self, offset: int, buffer):
        pass

    @abc.abstractmethod
    def resize(self, new_size: int):
        pass

    @abc.abstractmethod
    def size(self) -> int:
        pass

    def id(self) -> str:
        pass

    def supports_source(self):
        return False

    def supports_destination(self):
        return False

    @abc.abstractmethod
    def close():
        pass
