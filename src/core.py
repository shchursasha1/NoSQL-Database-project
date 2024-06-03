import json
import os
#from src.error_constants import KEY_ERROR, ITEM_ERROR, ARGS_ERROR

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

        if not os.path.exists(db_path):
            os.makedirs(db_path)

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
                    max_id = max(user['id'] for user in data[f'{table_name}'])  # Find the current maximum id
                    obj['id'] = max_id + 1  # Set the id of the new user to the current maximum id + 1
                    data[f'{table_name}'].append(obj)

        with open(file_path, 'w') as file:
            json.dump(data, file, indent=4)

    def update(self, table_name, condition, updates):
        data = self.select(table_name, condition)
        for obj in data:
            for key, value in updates.items():
                obj[key] = value
        self._rewrite_table(table_name, data)

    def select(self, table_name, condition):
        file_path = os.path.join(self.db_path, f"{table_name}.json")

        if os.path.exists(file_path):
            with open(file_path, 'r') as file:
                data = json.load(file)
                objects = [obj for obj in data[table_name]]
                condition = condition.split()
                #print(condition)
                results = [check for check in objects if check["name"] == condition[2]]
                #print(results)
        else:
            raise ValueError(f"Table {table_name} does not exist")

    def delete(self, table_name, condition):
        data = self.select(table_name, condition)
        self._rewrite_table(table_name, [obj for obj in self._read_table(table_name) if obj not in data])

    def flush(self, table_name):
        file_path = self._get_table_path(table_name)
        with open(file_path, 'r+') as f:
            f.truncate()

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
        return os.path.join(self.db_path, f"{table_name}.json")

    def _read_table(self, table_name):
        file_path = self._get_table_path(table_name)
        with open(file_path, 'r') as file:
            print(json.load(file))

    def _rewrite_table(self, table_name, data):
        file_path = self._get_table_path(table_name)

        pass
            

if __name__ == "__main__":
    db = Core("D:/OOP/NoSQL Database project/db")
    
    # Testing 'insert' method
    #db.insert("users", {"name": "Oleksandr Kolko", "age": 23, "email": "johndoe23@example.com"})
    #db.insert("users", {"name": "Oleksandr Shchur", "age": 23, "email": "johndoe23@example.com "})
    
    # Testing 'select' method
