import json
import os
from threading import Lock
from src.constants import MAX_FILE_SIZE, FLUSH_THRESHOLD
from src.BTree import Node, BTS
from tests.test_time import _timer


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
        self.garden = {}
        self.delete_markers = {}
        self.delete_counter = 0
        self.lock = Lock()

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
    def insert(self, table_name, obj):  # Приблизно за 1/4 секунди інсертиться
        with self.lock:
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
                            max_id = max(
                                user.get('id', 0) for user in data[f'{table_name}'])  # Find the current maximum id
                            obj['id'] = max_id + 1  # Set the id of the new user to the current maximum id + 1
                        data[f'{table_name}'].append(obj)

            self._write_table(table_name, data)

    def get_bytes(self, table_name):
        file_path = self._get_table_path(table_name)
        pars = []
        correct_pars = []
        bytes_count = 0
        condition = 0
        with open(file_path, 'rb') as file:
            for line in file:
                pos = 0
                while pos < len(line):
                    if chr(line[pos]) == '{':
                        pars.append(['{', bytes_count + pos])
                    elif chr(line[pos]) == '}':
                        pars.append(['}', bytes_count + pos])
                    pos += 1
                bytes_count += len(line)
        for el in pars[1:-1]:
            if el[0] == '{' and condition == 0:
                start = el[1]
                condition += 1
            elif el[0] == '}' and condition == 1:
                end = el[1]
                cords = (start, end)
                correct_pars.append(cords)
                condition -= 1
            elif el[0] == '{':
                condition += 1
            elif el[0] == '}':
                condition -= 1
        return correct_pars

    def create_BTS(self, parametr, table_name):
        file_path = self._get_table_path(table_name)
        tree = BTS()
        bytes = self.get_bytes(table_name)
        with open(file_path, 'r') as file:
            for byte in bytes:
                start = byte[0]
                end = byte[1]
                file.seek(start)
                data = file.read(end - start)
                for line in data.split('\n'):
                    if parametr in line:
                        if line[-1] == ',':
                            line = line[:-1]
                        value = line.split(':')[1]
                        try:
                            value = int(value)
                        except ValueError:
                            value = value.strip().strip('"')
                        tree.append(Node(value, [byte]))
                    else:
                        continue
        return tree

    @_timer
    def update(self, table_name, condition, updates):
        with self.lock:
            file_path = self._get_table_path(table_name)
            data = self._read_table(table_name)
            parsed_condition = self._parse_condition(condition)
            key, value = parsed_condition

            if os.path.exists(file_path):
                search_results = self.select(table_name, condition)

                if not search_results:
                    raise ValueError(f"No matching records found in table {table_name} for condition {condition}")

                for obj in data[table_name]:
                    if obj in search_results:
                        for key, value in updates.items():
                            obj[key] = value
                self.garden[key] = self.create_BTS(key, table_name)
                self._write_table(table_name, data)
            else:
                raise ValueError(f"Table {table_name} does not exist")

    @_timer
    def select(self, table_name, condition):
        file_path = self._get_table_path(table_name)

        if os.path.exists(file_path):
            results = []
            parsed_condition = self._parse_condition(condition)
            key, value = parsed_condition
            if key not in self.garden:
                self.garden[key] = self.create_BTS(key, table_name)

            try:
                value = int(value)  # Try to convert value to an integer (searching for id)
            except ValueError:
                pass

            coordinats = self.garden[key].search(value)
            with open(file_path, 'r') as file:
                try:
                    for borders in coordinats:
                        start = borders[0]
                        end = borders[1]
                        file.seek(start)
                        data = file.read(end - start)
                        data = data.strip()
                        if data[-1] == ',':
                            data = data[:-1]
                        results.append(json.loads(data))
                except TypeError:
                    raise ValueError(f"No matching records found in table {table_name} for condition {condition}")


            if not results:
                raise ValueError(f"No matching records found in table {table_name} for condition {condition}")

            return results
        else:
            raise ValueError(f"Table {table_name} does not exist")

    @_timer
    def delete(self, table_name, condition):
        with self.lock:
            all_data = self._read_table(table_name)
            parsed_condition = self._parse_condition(condition)
            key, value = parsed_condition
            data_to_delete = self.select(table_name, condition)

            if data_to_delete:
                if table_name not in self.delete_markers:
                    self.delete_markers[table_name] = set()

                for obj in data_to_delete:
                    id = obj.get('id')

                    if id in self.delete_markers[table_name]:
                        print(f"Object with id {id} is already marked for deletion")

                    self.delete_markers[table_name].add(id)
                    self.delete_counter += 1

                    if self.delete_counter >= FLUSH_THRESHOLD:
                        remaining_data = list(filter(lambda obj: obj.get('id') not in self.delete_markers[table_name], all_data[table_name]))
                        self._rewrite_table(table_name, remaining_data)
                        self.delete_counter = 0
                        self.delete_markers[table_name].clear()
                        self.garden[key] = self.create_BTS(key, table_name)
            else:
                raise ValueError(f"Data to delete not found in table {table_name}")

    def _is_obj_deleted(self, obj):
        return obj.get('id') in self.delete_markers.get('users', set())

    def flush(self, table_name):  # ???
        self._rewrite_table(table_name)

    def _get_table_path(self, table_name):
        base_path = os.path.join(self.db_path, f"{table_name}.json")
        if os.path.exists(base_path) and os.path.getsize(base_path) > MAX_FILE_SIZE:  # If the file is too large (over 1 MB), create a new file
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
    # db.insert("users1", {"name": "Oleksandr Kolko", "age": 23, "email": "johndoe23@example.com"})
    # db.insert("users", {"name": "Oleksandr Shchur", "age": 23, "email": "johndoe23@example.com"})

    # for i in range(1_000):
    # db.insert("users1", {"name": "Oleksandr Shchur", "age": 23, "email": "johndoe23@example.com", "degree": True})

    # Testing 'select' method - approved
    #print(db.select("users1", '"id" == 777'))

    # Testing 'delete' method - approved
    #print(db.garden)
    #db.garden['id'].show()
    #db.delete("users", '"id" == 105')
    #db.delete("users", '"id" == 106')
    #db.delete("users", '"id" == 107')
    #db.delete("users", '"id" == 108')
    #db.delete("users", '"id" == 109')


    # Testing 'update' method - approved
    #print(db.select("users", '"id" == 12'))
    #db.update("users", '"id" == 12', {"name": "John Doe"})
    #print(db.select("users", '"id" == 12'))