from src.main_core import Core
from src.query_language import QueryExecutor
from src.DBWebServer import DBWebServer

class NoSQLDatabase:
    """
    A NoSQL Database implementation using JSON as the data format.

    This class provides methods to store, retrieve, and manipulate data in a NoSQL database.
    It uses JSON as the underlying data format for storing documents.

    Attributes:
        db_dir (str): The directory where the database files will be stored.

    Methods:
        insert(object): Inserts a new object into the database.
        select(query): Retrieves documents from the database based on a query.
        update(query, update): Updates documents in the database based on a query and an update.
        delete(query): Deletes documents from the database based on a query.
    """
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

    def start_server(self):
        server = DBWebServer(self.core.db_path)
        server.app.run(host=server.host, port=server.port)


if __name__ == "__main__":
    db = NoSQLDatabase('D:/OOP/NoSQL Database project/db')
    executor = QueryExecutor(db)
    
    # Testing 'insert' method - approved
    #executor.execute("insert users {'name': 'John Doe', 'age': 30, 'email': 'null'}")

    # Testing 'select' method - approved
    print(executor.execute('select users where "name" == "Oleksandr Kolko"'))
    #print(executor.execute('select users1 where "degree" == "true"'))
    #print(executor.execute('select users where "email" == "johndoe23@example.com"'))
    #print(executor.execute('select users1 where "id" == 644'))

    # Testing 'delete' method - approved
    #executor.execute('delete users1 where "id" == 33')
    #executor.execute('delete users1 where "id" == 457')
    #executor.execute('delete users1 where "id" == 634')
    #executor.execute('delete users1 where "id" == 679')
    #executor.execute('delete users1 where "id" == 756')

    # Testing 'update' method - approved
    #executor.execute('update users set {"name": "John 384836845684856"} where "id" == 24')

    # Testing 'start_server' - approved
    db.start_server()
