from abc import *

class Database(ABC):

    def __init__(self, db_type, config) -> None:
        self.db_type = db_type
        self.config = config

    @abstractmethod
    def get_connection(self):
        pass

class SQLite(Database):

    def get_connection(self):
        pass
