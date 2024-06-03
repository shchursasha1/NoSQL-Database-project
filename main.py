from src.core import Core
from src.query_language import QueryExecutor

class NoSQLDatabase:
    def __init__(self, db_dir):
        self.core = Core(db_dir)

    def insert(self, table_name, obj):
        self.core.insert(table_name, obj)

    def update(self, table_name, updates, condition):
        self.core.update(table_name, condition, updates)

    def select(self, table_name, condition):
        return self.core.select(table_name, condition)

    def delete(self, table_name, condition):
        self.core.delete(table_name, condition)

    def flush(self, table_name):
        self.core.flush(table_name)

    def create_index(self, table_name, fields):
        self.core.create_index(table_name, fields)

    
if __name__ == "__main__":
    db = NoSQLDatabase('D:/OOP/NoSQL Database project/db')
    executor = QueryExecutor(db)
    
    # Testing 'insert' method
    executor.execute("insert users {'name': 'John Doe', 'age': 30, 'email': 'null'}")

    # Testing 'select' method
    executor.execute('select users where "name" == "John Doe"')


