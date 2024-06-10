from src.core import Core
from src.query_language import QueryExecutor
from src.DBWebServer import DBWebServer

class NoSQLDatabase:
    """
    This class is the main class of the project. It is responsible for creating an instance of the Core class
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

    def create_index(self, table_name, fields):
        self.core.create_index(table_name, fields)

    def start_server(self):
        server = DBWebServer(self.core.db_path)
        server.app.run(host=server.host, port=server.port)    
    
if __name__ == "__main__":
    db = NoSQLDatabase('D:/OOP/NoSQL Database project/db')
    executor = QueryExecutor(db)
    
    # Testing 'insert' method - approved
    #executor.execute("insert users {'name': 'John Doe', 'age': 30, 'email': 'null'}")

    # Testing 'select' method - approved
    #print(executor.execute('select users where "email" == null'))
    #print(executor.execute('select users1 where "degree" == "true"'))
    #print(executor.execute('select users1 where "id" == 644'))

    # Testing 'delete' method - approved
    #executor.execute('delete users1 where "id" == 1243')
    #executor.execute('delete users1 where "id" == 1244')
    #executor.execute('delete users1 where "id" == 1245')
    #executor.execute('delete users1 where "id" == 1246')
    #executor.execute('delete users1 where "id" == 1247')

    # Testing 'update' method - approved
    #executor.execute('update users set {"name": "John Doe"} where "id" == 13')

    # Testing 'start_server' - approved
    #db.start_server()