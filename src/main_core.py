import json
import os
from src.constants import MAX_FILE_SIZE, FLUSH_THRESHOLD
from tests.test_time import _timer

class Core:
    """
    This class is responsible for the core functionality of the database. It is responsible for inserting, updating, selecting and deleting data from the database
    args:
    db_path (str): The path to the directory where the database will be stored
    """
    def __init__(self, db_path) -> None:
        self.db_path = db_path
        self.data_files = {}
        self.delete_markers = {}
        self.delete_counter = 0

        if not os.path.exists(db_path):
            os.makedirs(db_path)

    def _get_data_files(self):
        return os.listdir(self.db_path)
    
    @staticmethod
    def _parse_condition(condition):
        key, value = condition.split('==')
        key = key.strip().strip('"')
        value = value.strip().strip('"')
        return key, value

    @_timer
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
                    if table_name not in data:
                        data[table_name] = []
                    if not data[f'{table_name}']:
                        obj['id'] = 1  # Set the id of the first user to 1
                    else:
                        max_id = max(user.get('id', 0) for user in data[f'{table_name}'])  # Find the current maximum id
                        obj['id'] = max_id + 1  # Set the id of the new user to the current maximum id + 1
                    data[f'{table_name}'].append(obj)

        self._write_table(table_name, data)

    @_timer
    def update(self, table_name, condition, updates):
        file_path = self._get_table_path(table_name)
        data = self._read_table(table_name)

        if os.path.exists(file_path):
            search_results = self.select(table_name, condition)

            if not search_results:
                raise ValueError(f"No matching records found in table {table_name} for condition {condition}")
                    
            for obj in data[table_name]:
                if obj in search_results:
                    for key, value in updates.items():
                        obj[key] = value

            self._write_table(table_name, data)
        else:
            raise ValueError(f"Table {table_name} does not exist")
    
    @_timer
    def select(self, table_name, condition):
        file_path = self._get_table_path(table_name)
        data = self._read_table(table_name)

        if os.path.exists(file_path):
            objects = [obj for obj in data[table_name] if not self._is_obj_deleted(obj)]
            parsed_condition = self._parse_condition(condition)
            key, value = parsed_condition

            if value == "null":
                value = None  # Convert 'null' to None
            elif value == "true":
                value = True
            else:
                try:
                    value = int(value)  # Try to convert value to an integer (searching for id)
                except ValueError:
                    pass

            results = [check for check in objects if key in check and check[key] == value]

            if not results:
                raise ValueError(f"No matching records found in table {table_name} for condition {condition}")
                
            return results
        else:
            raise ValueError(f"Table {table_name} does not exist")

    @_timer
    def delete(self, table_name, condition):
        data_to_delete = self.select(table_name, condition)

        if data_to_delete:
            if table_name not in self.delete_markers:
                self.delete_markers[table_name] = set()

            for obj in data_to_delete:
                id = obj.get('id')

                if id in self.delete_markers[table_name]:
                    continue

                self.delete_markers[table_name].add(id)
                self.delete_counter += 1

                if self.delete_counter >= FLUSH_THRESHOLD:
                    self.flush(table_name)
        else:
            raise ValueError(f"Data to delete not found in table {table_name}")
        
    def _is_obj_deleted(self, obj):
        return obj.get('id') in self.delete_markers.get('users', set())
     
    def flush(self, table_name):
        for table_name in self.delete_markers.keys():
            all_data = self._read_table(table_name)
            remaining_data = list(filter(lambda obj: obj.get('id') not in self.delete_markers[table_name], all_data[table_name]))
            self._rewrite_table(table_name, remaining_data)
            self.delete_markers[table_name].clear()
        self.delete_counter = 0
    
    def _get_table_path(self, table_name):
        base_path = os.path.join(self.db_path, f"{table_name}.json")
        if os.path.exists(base_path) and os.path.getsize(base_path) > MAX_FILE_SIZE: # If the file is too large (over MAX_FILE_SIZE), create a new file
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
    #db.insert("users1", {"name": "Oleksandr Kolko", "age": 23, "email": "johndoe23@example.com"})
    #db.insert("users", {"name": "Oleksandr Shchur", "age": 23, "email": "johndoe23@example.com"})

    #for i in range(1_000):
        #db.insert("users1", {"name": "Oleksandr Shchur", "age": 23, "email": "johndoe23@example.com", "degree": True})
     
    # Testing 'select' method - approved
    #print(db.select("users1", '"id" == 777'))

    # Testing 'delete' method - approved
    #db.delete("users", '"id" == 14')
    #db.delete("users1", '"id" == 9')
    #db.delete("users1", '"id" == 14')

    # Testing 'update' method - approved
    #db.update("users", '"id" == 14', {"name": "John Doe"})