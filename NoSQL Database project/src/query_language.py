import json

class QueryParser:
    """
    A class to parse the query string and return a tuple of the command and the arguments
    """
    def __init__(self, string: str) -> None:
        self.string = string
        
    @staticmethod
    def parse(query):
        tokens = query.split()
        command = tokens[0].lower()

        if command == 'insert':
            table_name = tokens[1]
            tokens = query.split()
            json_string = ' '.join(tokens[2:])  # only take the JSON part of the string
            json_string = json_string.replace("'", "\"")
            object = json.loads(json_string)
            return ('insert', table_name, object)
        elif command == 'select':
            table_name = tokens[1]
            condition = ' '.join(tokens[3:])
            return ('select', table_name, condition)
        elif command == 'update':
            table_name = tokens[1]
            updates = json.loads(' '.join(tokens[3:tokens.index('where')]))
            condition = ' '.join(tokens[tokens.index('where') + 1:])
            return ('update', table_name, updates, condition)
        elif command == 'delete':
            table_name = tokens[1]
            condition = ' '.join(tokens[3:])
            return ('delete', table_name, condition)
        elif command == 'create' and tokens[1] == 'index':
            table_name = tokens[2]
            index_name = tokens[3]
            fields = tokens[5:]
            return ('create_index', table_name, index_name, fields)
        elif command == 'flush':
            table_name = tokens[1]
            return ('flush', table_name)
        else:
            raise ValueError(f"Unknown command: {command}")
        

class QueryExecutor:
    """
    A class to execute the query on the database
    """
    def __init__(self, db):
        self.db = db
    
    def execute(self, query):
        parsed_query = QueryParser.parse(query)
        command = parsed_query[0]

        if command == 'insert':
            _, table_name, object = parsed_query
            self.db.insert(table_name, object)
        elif command == 'select':
            _, table_name, condition = parsed_query
            return self.db.select(table_name, condition)
        elif command == 'update':
            _, table_name, updates, condition = parsed_query
            self.db.update(table_name, updates, condition)
        elif command == 'delete':
            _, table_name, condition = parsed_query
            self.db.delete(table_name, condition)
        elif command == 'create_index':
            _, table_name, index_name, fields = parsed_query
            # Implement index creation logic
        elif command == 'flush':
            self.db.flush(parsed_query[1])
        else:
            raise ValueError(f"Unknown command: {command}")