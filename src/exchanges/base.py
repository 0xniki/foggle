from abc import abstractmethod, ABC

class Exchange(ABC):
    def __init__(self):
        super().__init__()

    @abstractmethod
    def connect(self) -> None:
        pass

    @abstractmethod
    def disconnect(self) -> None:
        pass  