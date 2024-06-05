import json
import os
#from src.constants import KEY_ERROR, ITEM_ERROR, ARGS_ERROR, MAX_FILE_SIZE

MAX_FILE_SIZE = 1000000  # Maximum file size in bytes (1 MB)

class Index:
    def __init__(self, table_name, field):
        self.table_name = table_name
        self.field = field
        self.index = {}

    def add(self, key, offset):
        if key not in self.index:
            self.index[key] = []
        self.index[key].append(offset)

    def remove(self, key, offset):
        if key in self.index:
            self.index[key].remove(offset)
            if not self.index[key]:
                del self.index[key]

    def get(self, key):
        return self.index.get(key, [])

class Core:
    def __init__(self, db_path) -> None:
        self.db_path = db_path
        self.data_files = {}
        self.indexes = {}
        self.delete_markers = {}

        if not os.path.exists(db_path):
            os.makedirs(db_path)

    def _get_data_files(self):  # потрібен буде пошук по всіх файлах?
        return os.listdir(self.db_path)
    
    @staticmethod
    def _parse_condition(condition):
        key, value = condition.split('==')
        key = key.strip().strip('"')
        value = value.strip().strip('"')
        return key, value

    def insert(self, table_name, obj):
        file_path = self._get_table_path(table_name)

        if table_name not in self.data_files:
            self.data_files[table_name] = file_path

        if os.path.exists(file_path):
            with open(file_path, 'r') as file:
                file_content = file.read()
                if file_content == '':
                    data = {f"{table_name}": [obj]}
                    obj['id'] = 1  # Set the id of the first user to 1              
                else:
                    file.seek(0)  # Move the cursor back to the start of the file
                    data = json.load(file)
                    if not data[f'{table_name}']:
                        obj['id'] = 1  # Set the id of the first user to 1
                    else:
                        max_id = max(user['id'] for user in data[f'{table_name}'])  # Find the current maximum id
                        obj['id'] = max_id + 1  # Set the id of the new user to the current maximum id + 1
                    data[f'{table_name}'].append(obj)

        self._write_table(table_name, data)

    def update(self, table_name, condition, updates):
        file_path = self._get_table_path(table_name)

        if os.path.exists(file_path):
            with open(file_path, 'r'):
                pass

    def select(self, table_name, condition):
        file_path = self._get_table_path(table_name)
        data = self._read_table(table_name)

        if os.path.exists(file_path):
            objects = [obj for obj in data[table_name]]
            parsed_condition = self._parse_condition(condition)
            key, value = parsed_condition
            if value == "null":
                value = None  # If value is the string "None", set it as None
            elif value == "true":
                value = True
            else:
                try:
                    value = int(value)  # Try to convert value to an integer (searching for id)
                except ValueError:
                    pass
            results = [check for check in objects if key in check and check[key] == value]
            return results
        else:
            raise ValueError(f"Table {table_name} does not exist")

    # TODO: реалізувати маркери видалення (щоб об'єкт позначався видаленним)
    def delete(self, table_name, condition):
        data_to_delete = self.select(table_name, condition)
        if len(data_to_delete) > 0:
            all_data = self._read_table(table_name)
            objects = [obj for obj in all_data[table_name]]
            remaining_data = [obj for obj in objects if obj not in data_to_delete]
            self._rewrite_table(table_name, remaining_data)
        else:
            raise ValueError(f"Data to delete not found in table {table_name}")
        
    def _is_deleted(self, object):
        pass

    def flush(self, table_name):
        pass

    def create_index(self, table_name, fields):
        if table_name not in self.indexes:
            self.indexes[table_name] = {}
        for field in fields:
            self.indexes[table_name][field] = self._create_index_for_field(table_name, field)

    def _create_index_for_field(self, table_name, field):
        file_path = self._get_table_path(table_name)
        index = {}
        with open(file_path, 'r') as f:
            offset = 0
            for line in f:
                obj = json.loads(line)
                if field in obj:
                    if obj[field] not in index:
                        index[obj[field]] = []
                    index[obj[field]].append(offset)
                offset += len(line)
        return index
    
    def _get_table_path(self, table_name):
        base_path = os.path.join(self.db_path, f"{table_name}.json")
        if os.path.exists(base_path) and os.path.getsize(base_path) > MAX_FILE_SIZE: # If the file is too large (over 1 MB) create a new file
            i = 1
            while True:
                new_path = os.path.join(self.db_path, f"{table_name}_{i}.json")
                if not os.path.exists(new_path) or os.path.getsize(new_path) <= MAX_FILE_SIZE:
                    return new_path
                i += 1
        return base_path

    def _read_table(self, table_name):
        file_path = self._get_table_path(table_name)
        with open(file_path, 'r') as file:
            return json.load(file)
        
    def _write_table(self, table_name, data):
        file_path = self._get_table_path(table_name)

        with open(file_path, 'w') as file:
            json.dump(data, file, indent=4)

    def _rewrite_table(self, table_name, new_data):
        file_path = self._get_table_path(table_name)

        if os.path.exists(file_path):
            with open(file_path, 'r') as file:
                data = json.load(file)
                data[table_name] = new_data  # Update the data with the new data

            with open(file_path, 'w') as file:
                json.dump(data, file, indent=4)
        else:
            raise ValueError(f"Table {table_name} does not exist")

if __name__ == "__main__":
    db = Core("D:/OOP/NoSQL Database project/db")
    
    # Testing 'insert' method - approved
    #db.insert("users", {"name": "Oleksandr Kolko", "age": 23, "email": "johndoe23@example.com"})
    #db.insert("users", {"name": "Oleksandr Shchur", "age": 23, "email": "johndoe23@example.com"})
    #db.insert("users", {"name": "Oleksandr Shchur", "age": 23, "email": "johndoe23@example.com", "degree": True})
     
    # Testing 'select' method - approved
    #print(db.select("users", '"name" == "Oleksandr Shchur"'))

    # Testing 'delete' method - approved
    #db.delete("users", '"id" == 27')

    # Testing 'update' method
    #db.update("users", '"name" == "Richard Hughes"', {"name": "John Doe"})


